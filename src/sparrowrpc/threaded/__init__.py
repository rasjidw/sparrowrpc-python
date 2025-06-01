# -----------------------------------------------------------------------
# WARNING: This file is auto-generated and should not be edited manually!
#
# It is generated using 'generate_from_template_code.py' using the
# _template_ directory as the source.
# -----------------------------------------------------------------------

from __future__ import annotations

from abc import ABC, abstractmethod
from collections import defaultdict, namedtuple
import inspect
import logging
import sys

from ..bases import MsgChannelBase

from ..registers import FuncInfo

from threading import Thread, Lock, Event, current_thread
from queue import Queue, Empty as QueueEmpty
from typing import Iterable

from traceback import format_exc, print_exc
from typing import Any, TYPE_CHECKING


from binarychain import BinaryChain, ChainReader


from ..bases import ProtocolEngineBase

from ..messages import (FinalType, IncomingAcknowledge, IncomingException, IncomingNotification, IncomingRequest, IncomingResponse, IterableCallbackInfo,
                        MessageSentEvent, MtpeExceptionCategory, MtpeExceptionInfo, OutgoingAcknowledge, OutgoingException,
                        OutgoingNotification, OutgoingRequest, OutgoingResponse, RequestBase, RequestCallbackInfo, RequestType, ResponseType)

from ..exceptions import CallerException, CalleeException


log = logging.getLogger(__name__)


def get_thread_or_task_name():
    return current_thread().name


def is_awaitable(may_be_awaitable):
    if sys.implementation.name == 'micropython':
        # FIXME: review inspect once next release of micropython done
        return type(may_be_awaitable).__name__ == 'generator'
    else:
        return inspect.isawaitable(may_be_awaitable)


def unwrap_result(raw_result):
    # FIXME: We may not need this once we have the @nonblocking dectorator
    unwraped_result = raw_result
    while True:
        if is_awaitable(unwraped_result):
            unwraped_result = unwraped_result
        else:
            break
    return unwraped_result


class ThreadedTransportBase(ABC):
    def __init__(self, max_msg_size, max_bc_length, incoming_msg_queue_size, outgoing_msg_queue_size, read_buf_size=8192):
        self.max_msg_size = max_msg_size
        self.max_bc_length = max_bc_length
        self.incoming_queue = Queue(maxsize=incoming_msg_queue_size)  # incoming queue of (BinaryChain, complete, remote_closed)
        self.outgoing_queue = Queue(maxsize=outgoing_msg_queue_size)  # outgoing queue of (BinaryChain, outgoing_queue)
        self.read_buf_size = read_buf_size
        self.remote_closed = False
        self.chain_reader = ChainReader(max_part_size=self.max_msg_size, max_chain_size=self.max_msg_size, max_chain_length=self.max_bc_length)
        self.started = False
        self.reader_thread = Thread(target=self._reader, daemon=True)
        self.writer_thread = Thread(target=self._writer, daemon=True)

    @abstractmethod
    def _read_data(self, size):
        raise NotImplementedError()

    @abstractmethod
    def _write_data(self, data):
        raise NotImplementedError()
    
    def start(self):
        if not self.started:
            self.reader_thread.start()
            self.writer_thread.start()
            self.started = True

    def _reader(self):
        while True:
            try:
                data = self._read_data(self.read_buf_size)
                if data:
                    for incoming_chain in self.chain_reader.get_binary_chains(data):
                        queue_data = (incoming_chain, self.chain_reader.complete(), self.remote_closed)
                        self.incoming_queue.put(queue_data)
                else:
                    break
            except Exception as e:
                log.error(f'Reader aborting with exception {e!s}')
                break
        self.remote_closed = True
        try:
            # put sentinel on incoming queue
            queue_data = (None, self.chain_reader.complete(), self.remote_closed)
            self.incoming_queue.put(queue_data)
        except Exception as e:
            log.error(f'Error putting sentinel on incoming queue - {e!s}')

    def _writer(self):
        while True:
            time_to_stop = self._writer_send_one()
            if time_to_stop:
                break

    def _writer_send_one(self):
        try:
            queue_data = self.outgoing_queue.get()
            if queue_data is None:
                return True
            
            chain, notifier_queue = queue_data
            assert isinstance(chain, BinaryChain)
            assert isinstance(notifier_queue, Queue)
            e = None  # return None if not exception
            try:
                data = chain.serialise()
                self._write_data(data)
            except Exception as exc:
                e = exc
            notifier_queue.put(e)
            return False
        except Exception as e:
            log.error(f'Writer aborting with exception {e!s}')
        return True

    # FIXME: Do we even need this now - perhaps just read directly from the queue?
    def get_next_binary_chain(self):
        binary_chain, complete, remote_closed = self.incoming_queue.get()
        return (binary_chain, complete, remote_closed)

    def send_binary_chain(self, binary_chain):
        log.debug(f'Adding binary chain to outgoing queue: {id(binary_chain)}: {repr(binary_chain)}')
        notifier_queue = Queue()
        queue_item = (binary_chain, notifier_queue)
        self.outgoing_queue.put(queue_item)
        e = notifier_queue.get()
        log.debug(f'Binary Chain {id(binary_chain)} sent.')
        if e:
            raise e
        
    def shutdown(self):
        # signal outgoing queue to stop
        self.outgoing_queue.put(None)
        queue_data = (None, None, None)
        self.incoming_queue.put(queue_data)  # end incoming queue
        self.close()
        self.reader_thread.join()
        self.writer_thread.join()
        
    @abstractmethod
    def close(self):
        raise NotImplementedError()


class ThreadedDispatcherBase(ABC):
    @abstractmethod
    def dispatch_incoming(self, msg_channel: ThreadedMsgChannel, request: RequestBase, func_info: FuncInfo):
        raise NotImplementedError()
    @abstractmethod
    def shutdown(self, timeout=0):
        raise NotImplementedError()


# FIXME: Turn this into a class
def threaded_call_func(msg_channel: ThreadedMsgChannel, incoming_msg: IncomingRequest|IncomingNotification, func_info: FuncInfo):
    log.debug(f'In threaded_call_func with {incoming_msg} and {func_info}')
    params = dict()
    injectors = list()
    if func_info.injectable_params:
        for param_name, injector_cls in func_info.injectable_params.items():
            assert issubclass(injector_cls, ThreadedInjectorBase)
            injector = injector_cls(msg_channel, incoming_msg, func_info)
            injectors.append(injector)
            injector.pre_call_setup()
            params[param_name] = injector.get_param()

    if func_info.func:
        func = func_info.func
    else:
        log.debug('Calling Iterable __next__')
        func = func_info.iterable_callback.__next__

    if incoming_msg.raw_binary:
        result = func(incoming_msg.data, **params)
    else:
        # normal non-binary request
        if incoming_msg.params:
            for param_name, value in incoming_msg.params.items():
                if param_name in params:
                    raise ValueError('duplicate param name')  # FIXME: make a caller error
                params[param_name] = value
        if isinstance(incoming_msg, IncomingRequest) and incoming_msg.callback_params:
            for (param_name, cb_data) in incoming_msg.callback_params.items():
                if param_name in params:
                    raise ValueError('duplicate param name')  # FIXME: make a caller error
                assert isinstance(cb_data, dict)
                proxy_type, cb_param_data = list(cb_data.items())[0]
                callback_request_id = cb_param_data['callback_request_id']
                cb_info = cb_param_data['cb_info']
                if proxy_type == '#cb':
                    cb_proxy = ThreadedCallbackProxy(param_name, callback_request_id, cb_info)
                elif proxy_type == '#icb':
                    cb_proxy = ThreadedIterableCallbackProxy(param_name, callback_request_id, cb_info)
                assert isinstance(cb_proxy, CallbackProxyBase)
                cb_proxy.set_channel(msg_channel)
                params[param_name] = cb_proxy
        try:
            result = func(**params)
        except StopIteration:
            # pass this up, but don't log as this is normal for stopping multipart requests
            raise 
        except Exception as e:
            log.warning('-' * 20)
            log.warning(format_exc())
            log.warning('-' * 20)
            raise

    # FIXME: Currently the post_call_cleanup is missed if there is an exception
    for injector in injectors:
        assert isinstance(injector, ThreadedInjectorBase)
        injector.post_call_cleanup()
    return result

def threaded_run_request_wait_to_complete(msg_channel: ThreadedMsgChannel, request: IncomingRequest,
                                                  func_info: FuncInfo, completed_callback=None):
    try:
        if func_info.multipart_response:
            for part_result in threaded_call_func(msg_channel, request, func_info):
                outgoing_msg = OutgoingResponse(request_id=request.id, result=part_result, response_type=ResponseType.MULTIPART)
                msg_channel._send_message(outgoing_msg)
            final_response = OutgoingResponse(request_id=request.id, response_type=ResponseType.MULTIPART, final=FinalType.TERMINATOR)
            msg_channel._send_message(final_response)
        else:
            result = threaded_call_func(msg_channel, request, func_info)
            if request.request_type == RequestType.QUIET:
                # never send the result to quiet requests.
                result = None
            outgoing_msg = OutgoingResponse(request_id=request.id, result=result)
            msg_channel._send_message(outgoing_msg)
    except Exception as e:
        if func_info.iterable_callback and isinstance(e, StopIteration):
            outgoing_msg = OutgoingResponse(request_id=request.id, result=None, final=FinalType.TERMINATOR)
        else:
            log.debug(format_exc())
            if isinstance(e, CallerException):
                exc_info = MtpeExceptionInfo(category=MtpeExceptionCategory.CALLER, type=type(e).__name__, msg=str(e))
            else:
                exc_info = MtpeExceptionInfo(category=MtpeExceptionCategory.CALLEE, type=type(e).__name__, msg=str(e))
            outgoing_msg = OutgoingException(request_id=request.id, exc_info=exc_info) 
        msg_channel._send_message(outgoing_msg)

    if completed_callback:
        task = None
        try:
            completed_callback(task, request)
        except Exception as e:
            log.error(f'Error calling completed_callback: {e!s}')


def threaded_run_request_not_waiting(msg_channel: ThreadedMsgChannel, request: IncomingRequest|IncomingNotification, 
                                             func_info: FuncInfo, completed_callback=None):
    try:
        # we send a response (with no data) to slient requests (before calling the function), but nothing for notifications.
        if isinstance(request, IncomingRequest) and request.request_type == RequestType.SILENT:
            outgoing_msg = OutgoingResponse(request_id=request.id)
            msg_channel._send_message(outgoing_msg)

        threaded_call_func(msg_channel, request, func_info)
    except Exception as e:
        # FIXME - make notification and slient errors available to the client software
        log.warning(f'Notification or Silent Request {request} raised error {str(e)}')

    if completed_callback:
        task = None
        try:
            completed_callback(task, request)
        except Exception as e:
            log.error(f'Error calling completed_callback: {e!s}')


def threaded_dispatch_request_or_notification(msg_channel, incoming_msg, func_info, completed_callback=None):
    if isinstance(incoming_msg, IncomingRequest):
        if incoming_msg.request_type == RequestType.SILENT:
            threaded_run_request_not_waiting(msg_channel, incoming_msg, func_info, completed_callback)
        else:
            threaded_run_request_wait_to_complete(msg_channel, incoming_msg, func_info, completed_callback)
    elif isinstance(incoming_msg, IncomingNotification):
        threaded_run_request_not_waiting(msg_channel, incoming_msg, func_info, completed_callback)
    else:
        log.warning(f'Got unhandled message {incoming_msg}')


class ThreadedDispatcher(ThreadedDispatcherBase):
    def __init__(self, num_threads, queue_size=10):
        if num_threads < 1:
            raise ValueError('num_threads must be at least 1')
        self.incoming_queue = Queue(queue_size)
        self.time_to_stop = False

        self.threads = [Thread(target=self._dispatch_worker) for _ in range(num_threads)]
        for t in self.threads:
            t.start()

    def _dispatch_worker(self):
        log.debug(f'Starting dispatch worker in thread {get_thread_or_task_name()}.')
        while not self.time_to_stop:
            try:
                msg_channel, incoming_msg, func_info = self.incoming_queue.get(timeout=1)
            except QueueEmpty:
                continue
            threaded_dispatch_request_or_notification(msg_channel, incoming_msg, func_info)
        log.debug(f'Dispatch worker in thread {get_thread_or_task_name()} finished.')

    def dispatch_incoming(self, msg_channel: ThreadedMsgChannel, request: RequestBase, func_info: FuncInfo):
        queue_item = (msg_channel, request, func_info)
        self.incoming_queue.put(queue_item)

    def shutdown(self, timeout=10):  # FIXME: What timeout do we really want?
        log.debug('Shutting down dispatcher')
        self.time_to_stop = True
        for t in self.threads:
            assert isinstance(t, Thread)
            t.join(timeout=timeout)   # FIXME: What to do with workers still running after timeout
        log.debug('Dispatcher shut down')


class ThreadedMsgChannel(MsgChannelBase):
    def __init__(self, transport: ThreadedTransportBase, initiator: bool, engine: ProtocolEngineBase, dispatcher: ThreadedDispatcherBase, channel_tag='', func_registers=None, channel_register=None):
        super().__init__(initiator, engine, channel_tag, func_registers, channel_register)
        self.transport = transport
        self.dispatcher = dispatcher
        self.request = ThreadedRequestProxyMaker(self)
        self._message_id_lock = Lock()
        self._msg_reader_thread = None
        
    def get_proxy(self):
        return ThreadedChannelProxy(self)
    
    def start_channel(self):
        self.transport.start()
        self._msg_reader_thread = Thread(target=self._incoming_msg_pump)
        self._msg_reader_thread.start()
        self._channel_register.register(self)
        log.debug(f'Channel {self} registered')

    def wait_for_remote_close(self):
        log.debug(f'Waiting for incoming message pump to finish on {get_thread_or_task_name()}')
        self._msg_reader_thread.join()

    def _incoming_msg_pump(self):
        log.debug(f'message pump started on thread {get_thread_or_task_name()}')
        while True:
            (bin_chain, complete, remote_closed) = self.transport.incoming_queue.get()
            if bin_chain is None:
                if not complete:
                    log.warning('Got end of chains but not complete')
                break
            
            message, dispatch, incoming_callback = self._parse_and_allocate_bin_chain(bin_chain)
            log.debug(f'** Incoming message: {message}, Dispatch: {dispatch}, Callback: {incoming_callback}')
            if dispatch:
                self._dispatch(message)
            if incoming_callback:
                incoming_callback(message)
        log.debug(f'message pump stopped on thread {get_thread_or_task_name()}')

    def _dispatch(self, message: IncomingRequest|IncomingNotification):
        func_info, ack_err_msg = self._get_func_info_and_ack_err_msg(message)
        if ack_err_msg:
            self._send_message(OutgoingAcknowledge(request_id=message.id))
        if func_info:
            self.dispatcher.dispatch_incoming(self, message, func_info)

    def _create_message_id(self):
        with self._message_id_lock:
            message_id = self._message_id
            self._message_id += 1
            return message_id

    def _send_message(self, message, message_event_callback = None, add_id_callback: callable = None):
        add_id, register_event_callback = self._get_add_id_and_reg_cb(message)
        if add_id:
            message_id = self._create_message_id()
            if add_id_callback:
                add_id_callback(message_id)
        else:
            message_id = None
        if register_event_callback:
            self._reg_callback(message_id, message_event_callback)

        bc = self.engine.outgoing_message_to_binary_chain(message, message_id)
        self.transport.send_binary_chain(bc)
        if message_event_callback:
            message_event_callback(MessageSentEvent(request_id=message_id))
        return message_id
    
    def queue_message(self, message, message_event_callback: callable, add_id_callback: callable = None):
        return self._send_message(message, message_event_callback, add_id_callback)

    def send_shutdown_pending(self):
        # FIXME
        pass

    def shutdown_channel(self):
        # FIXME: send ?
        self._channel_register.unregister(self)
        self.transport.shutdown()
        self._msg_reader_thread.join()
        log.debug(f'Channel {self} unregistered and msg_reader thread cleaned up')


# FIXME: Test timeouts!
class ThreadedChannelProxy:
    def __init__(self, channel: ThreadedMsgChannel):
        self.channel = channel
        self._callbacks = defaultdict(dict)   # self._callbacks[request_id][param_name] = cb_info

    def send_request_multipart_result_as_iterator(self, message: OutgoingRequest, timeout=None, msg_sent_callback=None, ack_callback=None):
        response_pump = ThreadedResponsePump(self, msg_sent_callback, ack_callback, expected_response_type=ResponseType.MULTIPART)
        response_pump.send_message(message, timeout)
        return response_pump

    def send_request(self, message: OutgoingRequest, timeout=None, msg_sent_callback=None, ack_callback=None):
        response_pump = ThreadedResponsePump(self, msg_sent_callback, ack_callback)
        response_pump.send_message(message, timeout)
        result = response_pump.__next__()
        # FIXME: Check complete etc?
        return result
                    
    def send_request_for_iter(self, message: OutgoingRequest, timeout=None, msg_sent_callback=None, ack_callback=None):
        response_pump = ThreadedResponsePump(self, msg_sent_callback, ack_callback, callback_iterable=True)
        response_pump.send_message(message, timeout)
        result = response_pump.__next__()
        # FIXME: Check complete etc?
        return result
    
    def send_notification(self, message: OutgoingNotification, timeout=None):
        self._send_message_wait_for_sent_event(message, timeout)

    def _send_message_wait_for_sent_event(self, message, timeout=None):
        wait_event = Event()
        def msg_sent_cb(event):
            wait_event.set()
        self.channel.queue_message(message, msg_sent_cb)
        if not wait_event.wait(timeout=timeout):
            log.error('Timeout error on notificaiton send')  # FIXME: Do we raise an error?


    def send_request_raw_async(self, message: OutgoingRequest, event_callback):
        reg_id_for_callbacks = None
        if isinstance(message, OutgoingRequest) and message.callback_params:
            # avoid a possible race by registring the req_id for any callbacks prior to sending the message 
            def reg_id_for_callbacks(req_id):
                for param_name, cb_info in message.callback_params.items():
                    self._callbacks[req_id][param_name] = cb_info

        req_id = self.channel.queue_message(message, event_callback, add_id_callback = reg_id_for_callbacks)
        return req_id


# FIXME: Do we just always return the result and event.final??? Makes the API more consistent
class ThreadedResponsePump():
    def __init__(self, channel_proxy: ThreadedChannelProxy, msg_sent_callback=None, ack_callback=None, expected_response_type=ResponseType.NORMAL, 
                 callback_iterable=False):
        self.channel_proxy = channel_proxy
        
        self.msg_sent_callback = msg_sent_callback
        self.ack_callback = ack_callback
        self.expected_response_type = expected_response_type
        self.callback_iterable = callback_iterable

        self.return_queue = Queue()
        self.count = 0
        self.timeout = None
        self.sent_notification = False
        self.complete = False

    def _cb_reader(self, event):
        self.return_queue.put(event)

    def send_message(self, message: OutgoingRequest, timeout=None):
        self.timeout = timeout
        if isinstance(message, OutgoingNotification):
            self.sent_notification = True
        self.channel_proxy.send_request_raw_async(message, self._cb_reader)

    def call_msg_sent_callback(self, event):
        if self.msg_sent_callback:
            self.msg_sent_callback(event)
        else:
            log.debug(f'Got msg sent event')

    def call_ack_callback(self, event):
        if self.ack_callback:
            self.ack_callback(event)
        else:
            log.info(f'Incoming ack received, but no callback passed in')

    def process_response(self, event):
        # check we have an expected NORMAL or MULTIPART response
        if event.response_type != self.expected_response_type:
            raise ValueError(f'Excpected response type {self.expected_response_type} but got {event.response_type}')
        if event.response_type == ResponseType.NORMAL:
            if self.callback_iterable:
                return event.result, event.final
            else:
                return event.result
            return
        elif event.response_type == ResponseType.MULTIPART:
            if event.final == FinalType.FINAL:
                self.complete = True
                return event.result
            elif event.final == FinalType.TERMINATOR:
                self.complete = True
                raise StopIteration()
            else:
                return event.result
            
    def process_incoming_req_or_notification(self, event):
        try:
            cb_info = self.channel_proxy._callbacks[event.callback_request_id][event.target]
            if isinstance(cb_info, RequestCallbackInfo):
                func_info = FuncInfo(target_name=event.target, func=cb_info.func)
            elif isinstance(cb_info, IterableCallbackInfo):
                func_info = FuncInfo(target_name=event.target, iterable_callback=cb_info.iter)
            else:
                log.error(f'Invalid cb_info type of {type(cb_info)}')
            log.debug(f'Calling dispatch with {func_info}')
            threaded_dispatch_request_or_notification(self.channel_proxy.channel, event, func_info)
        except KeyError:
            log.error(f'No callback found for incoming callback request {event}')

    def process_incoming_exception(self, event):
        exc_info = event.exc_info
        assert isinstance(exc_info, MtpeExceptionInfo)
        e = None
        if exc_info.category == MtpeExceptionCategory.CALLER:
            for cls in CallerException.get_subclasses():
                if exc_info.type == cls.__name__:
                    raise cls(exc_info.msg)
            raise CallerException(f'Type: {exc_info.type}, Msg: {exc_info.msg}')
        elif exc_info.category == MtpeExceptionCategory.CALLEE:
            raise CalleeException(f'Type: {exc_info.type}, Msg: {exc_info.msg}')

    def get_next_event_with_timeout(self):
        while True:
            try:
                return self.return_queue.get(timeout=1)
            except QueueEmpty:
                self.count += 1
                if self.timeout and self.count > self.timeout:
                    raise RuntimeError('TIMEOUT')  # FIXME. We should at least clean up incoming registers etc. Do we notify the callee?

    def __iter__(self):
        return self
        
    def __next__(self):
        while True:
            event = self.get_next_event_with_timeout()
            log.debug(f'Got event in __next__: {event}')
            if isinstance(event, MessageSentEvent):
                self.call_msg_sent_callback(event)
                if self.sent_notification:
                    self.complete = True
                    raise StopIteration
                continue

            if isinstance(event, IncomingAcknowledge):
                self.call_ack_callback(event)
                continue

            if isinstance(event, IncomingResponse):
                return self.process_response(event)

            if isinstance(event, IncomingRequest) or isinstance(event, IncomingNotification):
                self.process_incoming_req_or_notification(event)
                continue

            if isinstance(event, IncomingException):
                self.process_incoming_exception(event)

            log.error(f'Unhandled event {event}')


class ThreadedRequestProxy:
    def __init__(self, msg_channel: ThreadedMsgChannel, target: str, namespace: str=None, node: str=None, request_type: RequestType=RequestType.NORMAL, timeout: int=None, msg_sent_callback=None, ack_callback=None, multipart_response=False):
        self._msg_channel = msg_channel
        self._target = target
        self._namespace = namespace
        self._node = node
        self._request_type = request_type
        self._timeout = timeout
        self._msg_sent_callback = msg_sent_callback
        self._ack_callback = ack_callback
        self._multipart_response = multipart_response

    def __call__(self, **kwargs):
        params = dict()
        callback_params = dict()
        for param, value in kwargs.items():
            # not just checking isinstance(value, Iterable) because we don't want lists etc
            if hasattr(value, '__iter__') and hasattr(value, '__next__'):
                callback_params[param] = IterableCallbackInfo(iter=value)
            elif callable(value):
                callback_params[param] = RequestCallbackInfo(func=value)
            else:
                params[param] = value
        request = OutgoingRequest(target=self._target, namespace=self._namespace, node=self._node, params=params, callback_params=callback_params, request_type=self._request_type, acknowledge=bool(self._ack_callback))
        channel_proxy = self._msg_channel.get_proxy()
        if self._multipart_response:
            return channel_proxy.send_request_multipart_result_as_iterator(request, timeout=self._timeout, msg_sent_callback=self._msg_sent_callback, ack_callback=self._ack_callback)
        else:
            return channel_proxy.send_request(request, timeout=self._timeout, msg_sent_callback=self._msg_sent_callback, ack_callback=self._ack_callback)
    

class ThreadedRequestProxyMaker:
    def __init__(self, msg_channel: ThreadedMsgChannel, namespace: str=None, node: str=None, request_type: RequestType=RequestType.NORMAL, timeout: int=None, msg_sent_callback=None, ack_callback=None, multipart_response=False):
        self._msg_channel = msg_channel
        self._namespace = namespace
        self._node = node
        self._request_type = request_type
        self._timeout = timeout
        self._msg_sent_callback = msg_sent_callback
        self._ack_callback = ack_callback
        self._multipart_response = multipart_response

    def __getattr__(self, target):
        return ThreadedRequestProxy(self._msg_channel, target, self._namespace, self._node, self._request_type, self._timeout, self._msg_sent_callback, self._ack_callback, self._multipart_response)
    
    def __call__(self, namespace: str=None, node: str=None, request_type: RequestType = RequestType.NORMAL, timeout: int=None, msg_sent_callback=None, ack_callback=None, multipart_response=False):
        return ThreadedRequestProxyMaker(self._msg_channel, namespace, node, request_type, timeout, msg_sent_callback, ack_callback, multipart_response)



class CallbackProxyBase:
    def __init__(self, cb_param_name, callback_request_id, cb_info):
        self._cb_param_name = cb_param_name
        self._callback_request_id = callback_request_id
        self.cb_info = cb_info
        self._channel = None

    def set_channel(self, channel):
        # FIXME: create base MsgChannel in core so we can assert this is a MsgChannel here.
        self._channel = channel


class ThreadedCallbackProxy(CallbackProxyBase):
    def __init__(self, cb_param_name, callback_request_id, cb_info):
        super().__init__(cb_param_name, callback_request_id, cb_info)
        self.request_type = None  # can be changed before sending / FIXME: how we do set a default?

    def set_to_notification(self):
        self.request_type = None

    def set_to_request(self, request_type: RequestType):
        self.request_type = request_type

    def __call__(self, **kwargs):
        if self.request_type:
            outgoing_msg = OutgoingRequest(target=self._cb_param_name, callback_request_id=self._callback_request_id, params=kwargs, request_type=self.request_type)
        else:
            outgoing_msg = OutgoingNotification(target=self._cb_param_name, callback_request_id=self._callback_request_id, params=kwargs)
        log.debug(f'Callback Proxy call: {repr(outgoing_msg)}')
        proxy = self._channel.get_proxy()
        if self.request_type:
            result = proxy.send_request(outgoing_msg)
            return result
        else:
            proxy.send_notification(outgoing_msg)


class ThreadedIterableCallbackProxy(CallbackProxyBase):
    def __init__(self, cb_param_name, callback_request_id, cb_info):
        super().__init__(cb_param_name, callback_request_id, cb_info)
        self._final = False

    def __iter__(self):
        return self

    def __next__(self):
        if self._final:
            raise StopIteration()
        
        outgoing_msg = OutgoingRequest(target=self._cb_param_name, callback_request_id=self._callback_request_id)
        log.debug(f'CallbackIterableProxy call: {repr(outgoing_msg)}')
        proxy = self._channel.get_proxy()
        result, final = proxy.send_request_for_iter(outgoing_msg)
        if final == FinalType.TERMINATOR:
            raise StopIteration()
        elif final == FinalType.FINAL:
            self._final = True
        return result



class ThreadedInjectorBase(ABC):    
    # FIXME: create base MsgChannel in core so we can assert this is a MsgChannel here.
    def __init__(self, msg_channel, incoming_request: RequestBase, func_info: FuncInfo):
        self.msg_channel = msg_channel
        self.incoming_request = incoming_request
        self.func_info = func_info

    @abstractmethod
    def pre_call_setup(self):
        raise NotImplementedError
    
    @abstractmethod
    def get_param(self):
        raise NotImplementedError
    
    @abstractmethod
    def post_call_cleanup(self):
        raise NotImplementedError


class ThreadedMsgChannelInjector(ThreadedInjectorBase):
    def pre_call_setup(self):
        pass

    # FIXME: create base MsgChannel in core so we can assert this is a MsgChannel here.
    def get_param(self):
        return self.msg_channel
        
    def post_call_cleanup(self):
        pass
