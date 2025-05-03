from __future__ import annotations

from abc import ABC, abstractmethod
from collections import defaultdict, namedtuple
import inspect  #= async
import json
import logging
import os
import socket
import sys
if 'threaded' in __name__: #= remove
    from threading import Thread, Lock, Event, current_thread  #= threaded <
    from queue import Queue, Empty as QueueEmpty  #= threaded <
    from typing import Iterable #= threaded <
else: #= remove
    import asyncio #= async <
    from asyncio import Lock, Event #= async <
    try: #= async <
        from asyncio import Queue, QueueEmpty #= async <
    except AttributeError:  #= async <
        from uasync.queues import Queue, QueueEmpty  #= async <

    #from concurrent.futures import ThreadPoolExecutor #= async <
    from typing import AsyncIterable as Iterable  #= async <
from traceback import format_exc
from typing import Any, TYPE_CHECKING

from binarychain import BinaryChain, ChainReader


from ..core import (RequestBase, ResponseType, FinalType, MessageSentEvent,
                       OutgoingRequest, OutgoingResponse, OutgoingNotification, OutgoingException, OutgoingAcknowledge,
                       IncomingRequest, IncomingResponse, IncomingNotification, IncomingException, IncomingAcknowledge,
                       FuncInfo, RequestCallbackInfo, IterableCallbackInfo,
                         RequestType, MtpeExceptionCategory, MtpeExceptionInfo, global_channel_register,
                         MsgChannelBase)
from ..core import ProtocolEngineBase

from ..exceptions import CallerException, CalleeException
from ..core import FunctionRegister, default_func_register


log = logging.getLogger(__name__)


def get_thread_or_task_name():
    return current_thread().name  #= threaded
    return asyncio.current_task().get_name()  #= async


class _Template_TransportBase(ABC):
    def __init__(self, max_msg_size, max_bc_length, incoming_msg_queue_size, outgoing_msg_queue_size, read_buf_size=8192):
        self.max_msg_size = max_msg_size
        self.max_bc_length = max_bc_length
        self.incoming_queue = Queue(maxsize=incoming_msg_queue_size)
        self.outgoing_queue = Queue(maxsize=outgoing_msg_queue_size)
        self.read_buf_size = read_buf_size
        self.remote_closed = False
        self.chain_reader = ChainReader(max_part_size=self.max_msg_size, max_chain_size=self.max_msg_size, max_chain_length=self.max_bc_length)
        self.started = False
        #= threaded start
        self.reader_thread = Thread(target=self._reader, daemon=True)
        self.writer_thread = Thread(target=self._writer, daemon=True)
        #= threaded end
        #= async start
        self.reader_task = asyncio.create_task(self._reader())
        self.writer_task = asyncio.create_task(self._writer())
        #= async end

    @abstractmethod
    async def _read_data(self, size):
        raise NotImplementedError()

    @abstractmethod
    async def _write_data(self, data):
        raise NotImplementedError()
    
    async def start(self):
        if not self.started:
            #= threaded start
            self.reader_thread.start()
            self.writer_thread.start()
            #= threaded end
            self.started = True

    async def _reader(self):
        while True:
            try:
                data = await self._read_data(self.read_buf_size)
                if data:
                    for incoming_chain in self.chain_reader.get_binary_chains(data):
                        await self.incoming_queue.put(incoming_chain)
                else:
                    break
            except Exception as e:
                log.warning(f'Reader aborting with exception {e!s}')
                break
        self.remote_closed = True
        await self.incoming_queue.put(None)

    async def _writer(self):
        while True:
            time_to_stop = await self._writer_send_one()
            if time_to_stop:
                break

    async def _writer_send_one(self):
        queue_data = await self.outgoing_queue.get()
        if queue_data is None:
            return True
        
        chain, notifier_queue = queue_data
        assert isinstance(chain, BinaryChain)
        assert isinstance(notifier_queue, Queue)
        e = None  # return None if not exception
        try:
            data = chain.serialise()
            await self._write_data(data)
        except Exception as exc:
            e = exc
        await notifier_queue.put(e)
        return False

    async def get_binary_chains(self):
        log.debug('Starting in get_binary_chains') #= async
        while True:
            binary_chain = await self.incoming_queue.get()
            if binary_chain is None:  # closing down
                return
            log.debug('Got chain from queue')  #= async
            yield (binary_chain, self.chain_reader.complete(), self.remote_closed)

    async def send_binary_chain(self, binary_chain):
        log.debug(f'Adding binary chain to outgoing queue: {id(binary_chain)}: {repr(binary_chain)}')
        notifier_queue = Queue()
        queue_item = (binary_chain, notifier_queue)
        await self.outgoing_queue.put(queue_item)
        e = await notifier_queue.get()
        log.debug(f'Binary Chain {id(binary_chain)} sent.')
        if e:
            raise e
        
    async def shutdown(self):
        await self.incoming_queue.put(None)  # end incoming queue
        await self.close()
        
    @abstractmethod
    async def close(self):
        raise NotImplementedError()


class _Template_DispatcherBase(ABC):
    @abstractmethod
    async def dispatch_incoming(self, msg_channel: _Template_MsgChannel, request: RequestBase, func_info: FuncInfo):
        raise NotImplementedError()
    @abstractmethod
    async def shutdown(self, timeout=0):
        raise NotImplementedError()


async def _template__call_func(msg_channel: _Template_MsgChannel, incoming_msg: IncomingRequest|IncomingNotification, func_info: FuncInfo):
    params = dict()
    injectors = list()
    if func_info.injectable_params:
        for param_name, injector_cls in func_info.injectable_params.items():
            assert issubclass(injector_cls, _Template_InjectorBase)
            injector = injector_cls(msg_channel, incoming_msg, func_info)
            injectors.append(injector)
            await injector.pre_call_setup()
            params[param_name] = await injector.get_param()

    if incoming_msg.raw_binary:
        result = func_info.func(incoming_msg.data, **params)
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
                    cb_proxy = _Template_CallbackProxy(param_name, callback_request_id, cb_info)
                elif proxy_type == '#icb':
                    cb_proxy = _Template_IterableCallbackProxy(param_name, callback_request_id, cb_info)
                assert isinstance(cb_proxy, CallbackProxyBase)
                cb_proxy.set_channel(msg_channel)
                params[param_name] = cb_proxy
        #= threaded start
        result = func_info.func(**params)
        #= threaded end
        #= async start
        if inspect.iscoroutinefunction(func_info.func):
            result = await func_info.func(**params)
        else:
            result = func_info.func(**params)  # FIXME - should put in thread pool if not flagged as non-blocking.
        #= async end
    for injector in injectors:
        assert isinstance(injector, _Template_InjectorBase)
        await injector.post_call_cleanup()
    return result

async def _template__run_request_wait_to_complete(msg_channel: _Template_MsgChannel, request: IncomingRequest, func_info: FuncInfo):
    try:
        if func_info.multipart_reponse:
            async for part_result in await _template__call_func(msg_channel, request, func_info):
                outgoing_msg = OutgoingResponse(request_id=request.id, result=part_result, response_type=ResponseType.MULTIPART)
                await msg_channel._send_message(outgoing_msg)
            final_response = OutgoingResponse(request_id=request.id, response_type=ResponseType.MULTIPART, final=FinalType.TERMINATOR)
            await msg_channel._send_message(final_response)
        else:
            result = await _template__call_func(msg_channel, request, func_info)
            if request.request_type == RequestType.QUIET:
                # never send the result to quiet requests.
                result = None
            outgoing_msg = OutgoingResponse(request_id=request.id, result=result)
            await msg_channel._send_message(outgoing_msg)
    except Exception as e:
        if func_info.is_iterable_callback and isinstance(e, StopAsyncIteration):
            outgoing_msg = OutgoingResponse(request_id=request.id, result=None, final=FinalType.TERMINATOR)
        else:
            log.debug(format_exc())
            if isinstance(e, CallerException):
                exc_info = MtpeExceptionInfo(category=MtpeExceptionCategory.CALLER, type=type(e).__name__, msg=str(e))
            else:
                exc_info = MtpeExceptionInfo(category=MtpeExceptionCategory.CALLEE, type=type(e).__name__, msg=str(e))
            outgoing_msg = OutgoingException(request_id=request.id, exc_info=exc_info) 
        await msg_channel._send_message(outgoing_msg)

async def _template__run_request_not_waiting(msg_channel: _Template_MsgChannel, request: IncomingRequest|IncomingNotification, func_info: FuncInfo):
    try:
        # we send a response (with no data) to slient requests (before calling the function), but nothing for notifications.
        if isinstance(request, IncomingRequest) and request.request_type == RequestType.SILENT:
            outgoing_msg = OutgoingResponse(request_id=request.id)
            await msg_channel._send_message(outgoing_msg)

        await _template__call_func(msg_channel, request, func_info)
    except Exception as e:
        # FIXME - make notification and slient errors available to the client software
        log.warning(f'Notification or Silent Request {request} raised error {str(e)}')


async def _template__dispatch_request_or_notification(msg_channel, incoming_msg, func_info):
    if isinstance(incoming_msg, IncomingRequest):
        if incoming_msg.request_type == RequestType.SILENT:
            await _template__run_request_not_waiting(msg_channel, incoming_msg, func_info)
        else:
            await _template__run_request_wait_to_complete(msg_channel, incoming_msg, func_info)
    elif isinstance(incoming_msg, IncomingNotification):
        await _template__run_request_not_waiting(msg_channel, incoming_msg, func_info)
    else:
        log.warning(f'Got unhandled message {incoming_msg}')


class _Template_Dispatcher(_Template_DispatcherBase):
    def __init__(self, num_threads, queue_size=10):
        if num_threads < 1:
            raise ValueError('num_threads must be at least 1')
        self.incoming_queue = Queue(queue_size)
        self.time_to_stop = False

        #= async start
        self.tasks = set()
        #self.executor = ThreadPoolExecutor(max_workers=num_threads)  # FIXME: Not using this yet. For non-async functions exported
        self.task_fetcher = asyncio.create_task(self._async_task_fetcher())
        # asyncio.ensure_future(self.task_fetcher)
        #= async end
        #= threaded start
        self.threads = [Thread(target=self._worker) for _ in range(num_threads)]
        for t in self.threads:
            t.start()
        #= threaded end

    #= threaded start
    def _worker(self):
        log.debug(f'Starting dispatch worker in thread {get_thread_or_task_name()}.')
        while not self.time_to_stop:
            try:
                msg_channel, incoming_msg, func_info = self.incoming_queue.get(timeout=1)
            except QueueEmpty:
                continue
            _template__dispatch_request_or_notification(msg_channel, incoming_msg, func_info)
        log.debug(f'Dispatch worker in thread {get_thread_or_task_name()} finished.')
    #= threaded end
    #= async start
    async def _async_task_fetcher(self):
        while not self.time_to_stop:
            try:
                msg_channel, incoming_msg, func_info = await asyncio.wait_for(self.incoming_queue.get(), timeout=1)
            except asyncio.TimeoutError:
                continue
            task = asyncio.create_task(_template__dispatch_request_or_notification(msg_channel, incoming_msg, func_info))
            self.tasks.add(task)
            task.add_done_callback(self.tasks.discard)
            # asyncio.ensure_future(task)
    #= async end

    async def dispatch_incoming(self, msg_channel: _Template_MsgChannel, request: RequestBase, func_info: FuncInfo):
        queue_item = (msg_channel, request, func_info)
        await self.incoming_queue.put(queue_item)

    async def shutdown(self, timeout=0):
        log.debug('Shutting down dispatcher')
        self.time_to_stop = True
        #= threaded start
        for t in self.threads:
            assert isinstance(t, Thread)
            t.join(timeout=timeout)
        #= threaded end
        #= async start
        for task in self.tasks:
            await task
        #= async end
        log.debug('Dispatcher shut down')


class _Template_MsgChannel(MsgChannelBase):
    def __init__(self, transport: _Template_TransportBase, initiator: bool, engine: ProtocolEngineBase, dispatcher: _Template_DispatcherBase, channel_tag='', func_registers=None, channel_register=None):
        MsgChannelBase.__init__(self, initiator, engine, channel_tag, func_registers, channel_register)
        self.transport = transport
        self.dispatcher = dispatcher
        self.request = _Template_RequestProxyMaker(self)
        self._message_id_lock = Lock()
        self._msg_reader_thread = None #= threaded
        self._msg_reader_task = None #= async
        
    async def get_proxy(self):
        return _Template_ChannelProxy(self)
    
    async def start_channel(self):
        await self.transport.start()
        #= threaded start
        self._msg_reader_thread = Thread(target=self._incoming_msg_pump)
        self._msg_reader_thread.start()
        #= threaded end
        #= async start
        self._msg_reader_task = asyncio.create_task(self._incoming_msg_pump())
        # asyncio.ensure_future(self._msg_reader_task)
        #= async end
        self._channel_register.register(self)
        log.debug(f'Channel {self} registered')

    async def wait_for_remote_close(self):
        log.debug(f'Waiting for incoming message pump to finish on {get_thread_or_task_name()}')
        self._msg_reader_thread.join() #= threaded
        await self._msg_reader_task #= async

    async def _incoming_msg_pump(self):
        log.debug(f'message pump started on thread {get_thread_or_task_name()}')
        async for (bin_chain, complete, remote_closed) in self.transport.get_binary_chains():
            message, dispatch, incoming_callback = self._parse_and_allocate_bin_chain(bin_chain)
            if dispatch:
                await self._dispatch(message)
            if incoming_callback:
                await incoming_callback(message)
            if remote_closed:
                break
        log.debug(f'message pump stopped on thread {get_thread_or_task_name()}')

    async def _dispatch(self, message: IncomingRequest|IncomingNotification):
        func_info, ack_err_msg = self._get_func_info_and_ack_err_msg(message)
        if ack_err_msg:
            await self._send_message(OutgoingAcknowledge(message.id))
        if func_info:
            await self.dispatcher.dispatch_incoming(self, message, func_info)

    async def _create_message_id(self):
        async with self._message_id_lock:
            message_id = self._message_id
            self._message_id += 1
            return message_id

    async def _send_message(self, message, message_event_callback = None):
        add_id, register_event_callback = self._get_add_id_and_reg_cb(message)
        message_id = await self._create_message_id() if add_id else None
        if register_event_callback:
            self._reg_callback(message_id, message_event_callback)

        bc = self.engine.outgoing_message_to_binary_chain(message, message_id)
        await self.transport.send_binary_chain(bc)
        if message_event_callback:
            await message_event_callback(MessageSentEvent(message_id))
        return message_id
    
    async def queue_message(self, message, message_event_callback: callable):
        return await self._send_message(message, message_event_callback)

    async def send_shutdown_pending(self):
        # FIXME
        pass

    async def shutdown_channel(self):
        # FIXME: send ?
        self._channel_register.unregister(self)
        await self.transport.shutdown()
        self._msg_reader_thread.join() #= threaded
        await self._msg_reader_task #= async
        log.debug(f'Channel {self} unregistered and msg_reader thread cleaned up')


# FIXME: Test timeouts!
class _Template_ChannelProxy:
    def __init__(self, channel: _Template_MsgChannel):
        self.channel = channel

        self._callbacks = defaultdict(dict)   # self._callback[request_id][param_name] = cb_info

    async def _send_request_result_as_generator(self, message: OutgoingRequest, timeout=None, msg_sent_callback=None, ack_callback=None, expected_response_type=ResponseType.NORMAL, callback_iterable=False):
        return_queue = Queue()
        async def cb_reader(event):
            await return_queue.put(event)
        await self.send_request_raw_async(message, cb_reader)
        count = 0
        while True:
            #= threaded start
            try:
                event = return_queue.get(timeout=1)
            except QueueEmpty:
                count += 1
                if timeout and count > timeout:
                    raise RuntimeError('TIMEOUT')  # FIXME. We should at least clean up incoming registers etc. Do we notify the callee?
                continue
            #= threaded end
            #= async start
            try:
                event = await asyncio.wait_for(return_queue.get(), timeout=1)
            except asyncio.TimeoutError:
                count += 1
                if timeout and count > timeout:
                    raise RuntimeError('TIMEOUT')  # FIXME. We should at least clean up incoming registers etc. Do we notify the callee?
                continue
            #= async end
            if isinstance(event, MessageSentEvent):
                if isinstance(message, OutgoingNotification):
                    return
                if msg_sent_callback:
                    await msg_sent_callback(event)
                else:
                    log.debug(f'Got msg sent event')
                continue
            if isinstance(event, IncomingAcknowledge):
                if ack_callback:
                    await ack_callback(event)   # FIXME: Do we return the event, or just call the callback with None?
                else:
                    log.info(f'Incoming ack received, but no callback passed in')
                continue
            if isinstance(event, IncomingResponse):
                # check we have an expected NORMAL or MULTIPART response
                if event.response_type != expected_response_type:
                    raise ValueError(f'Excpected response type {expected_response_type} but got {event.response_type}')
                if event.response_type == ResponseType.NORMAL:
                    if callback_iterable:
                        yield event.result, event.final
                    else:
                        yield event.result
                    return
                elif event.response_type == ResponseType.MULTIPART:
                    if event.final == FinalType.FINAL:
                        yield event.result
                        return
                    elif event.final == FinalType.TERMINATOR:
                        return
                    else:
                        yield event.result
                        continue
            if isinstance(event, IncomingRequest) or isinstance(event, IncomingNotification):
                try:
                    cb_info = self._callbacks[event.callback_request_id][event.target]
                    if isinstance(cb_info, RequestCallbackInfo):
                        func_info = FuncInfo(event.target, None, None, None, False, cb_info.func)
                    elif isinstance(cb_info, IterableCallbackInfo):
                        async def iter_func():
                            return anext(cb_info.iter)
                        func_info = FuncInfo(event.target, None, None, None, False, iter_func, is_iterable_callback=True)
                    await _template__dispatch_request_or_notification(self.channel, event, func_info)
                except KeyError:
                    log.error(f'No callback found for incoming callback request {event}')
                continue
            if isinstance(event, IncomingException):
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
            log.warning(f'Unhandled event {event}')

    async def send_request_multipart_result_as_generator(self, message: OutgoingRequest, timeout=None, msg_sent_callback=None, ack_callback=None):
        async for result in await self._send_request_result_as_generator(message, timeout, msg_sent_callback, ack_callback, expected_response_type=ResponseType.MULTIPART):
            yield result

    async def send_request(self, message: OutgoingRequest, timeout=None, msg_sent_callback=None, ack_callback=None):
        return await anext(self._send_request_result_as_generator(message, timeout, msg_sent_callback, ack_callback, expected_response_type=ResponseType.NORMAL))
                    
    async def send_request_for_iter(self, message: OutgoingRequest, timeout=None, msg_sent_callback=None, ack_callback=None):
        return await anext(self._send_request_result_as_generator(message, timeout, msg_sent_callback, ack_callback, expected_response_type=ResponseType.NORMAL, 
                                                                        callback_iterable=True))
    
    async def send_notification(self, message: OutgoingNotification, timeout=None):
        await self._send_message_wait_for_sent_event(message, timeout)

    async def _send_message_wait_for_sent_event(self, message, timeout=None):
        wait_event = Event()
        async def msg_sent_cb(event):
            wait_event.set()
        await self.channel.queue_message(message, msg_sent_cb)
        #= threaded start
        if not wait_event.wait(timeout=timeout):
            log.error('Timeout error on notificaiton send')  # FIXME: Do we raise an error?
        #= threaded end
        #= async start
        try:
            await asyncio.wait_for(wait_event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            log.error('Timeout error on notificaiton send')  # FIXME: Do we raise an error?
        #= async end


    async def send_request_raw_async(self, message: OutgoingRequest, event_callback):
        req_id = await self.channel.queue_message(message, event_callback)

        # FIXME: Possible race condition here, as (in theory) we could get the callback before the self._callbacks dict is updated.
        # Fix is probably to allocate the id in a separate call, rather than in queue message?
        if isinstance(message, OutgoingRequest) and message.callback_params:
            for param_name, cb_info in message.callback_params.items():
                self._callbacks[req_id][param_name] = cb_info
        return req_id


class _Template_RequestProxy:
    def __init__(self, msg_channel: _Template_MsgChannel, target: str, namespace: str=None, node: str=None, request_type: RequestType=RequestType.NORMAL, timeout: int=None, msg_sent_callback=None, ack_callback=None, multipart_reponse=False):
        self._msg_channel = msg_channel
        self._target = target
        self._namespace = namespace
        self._node = node
        self._request_type = request_type
        self._timeout = timeout
        self._msg_sent_callback = msg_sent_callback
        self._ack_callback = ack_callback
        self._multipart_reponse = multipart_reponse

    async def __call__(self, **kwargs):
        params = kwargs
        params = dict()
        callback_params = dict()
        for param, value in kwargs.items():
            if callable(value):
                callback_params[param] = RequestCallbackInfo(value)
            # not just checking isinstance(value, Iterable) because we don't want lists etc
            elif hasattr(value, '__aiter__') and hasattr(value, '__anext__'):
                callback_params[param] = IterableCallbackInfo(value)
            else:
                params[param] = value
        request = OutgoingRequest(self._target, namespace=self._namespace, node=self._node, params=params, callback_params=callback_params, request_type=self._request_type, acknowledge=bool(self._ack_callback))
        channel_proxy = await self._msg_channel.get_proxy()
        if self._multipart_reponse:
            return await channel_proxy.send_request_multipart_result_as_generator(request, timeout=self._timeout, msg_sent_callback=self._msg_sent_callback, ack_callback=self._ack_callback)
        else:
            return await channel_proxy.send_request(request, timeout=self._timeout, msg_sent_callback=self._msg_sent_callback, ack_callback=self._ack_callback)
    

class _Template_RequestProxyMaker:
    def __init__(self, msg_channel: _Template_MsgChannel, namespace: str=None, node: str=None, request_type: RequestType=RequestType.NORMAL, timeout: int=None, msg_sent_callback=None, ack_callback=None, multipart_reponse=False):
        self._msg_channel = msg_channel
        self._namespace = namespace
        self._node = node
        self._request_type = request_type
        self._timeout = timeout
        self._msg_sent_callback = msg_sent_callback
        self._ack_callback = ack_callback
        self._multipart_reponse = multipart_reponse

    def __getattr__(self, target):
        return _Template_RequestProxy(self._msg_channel, target, self._namespace, self._node, self._request_type, self._timeout, self._msg_sent_callback, self._ack_callback, self._multipart_reponse)
    
    def __call__(self, namespace: str=None, node: str=None, request_type: RequestType = RequestType.NORMAL, timeout: int=None, msg_sent_callback=None, ack_callback=None, multipart_reponse=False):
        return _Template_RequestProxyMaker(self._msg_channel, namespace, node, request_type, timeout, msg_sent_callback, ack_callback, multipart_reponse)





class CallbackProxyBase:
    def __init__(self, cb_param_name, callback_request_id, cb_info):
        self._cb_param_name = cb_param_name
        self._callback_request_id = callback_request_id
        self.cb_info = cb_info
        self._channel = None

    def set_channel(self, channel):
        # FIXME: create base MsgChannel in core so we can assert this is a MsgChannel here.
        self._channel = channel


class _Template_CallbackProxy(CallbackProxyBase):
    def __init__(self, cb_param_name, callback_request_id, cb_info):
        CallbackProxyBase.__init__(self, cb_param_name, callback_request_id, cb_info)
        self.request_type = None  # can be changed before sending / FIXME: how we do set a default?

    def set_to_notification(self):
        self.request_type = None

    def set_to_request(self, request_type: RequestType):
        self.request_type = request_type

    async def __call__(self, **kwargs):
        if self.request_type:
            outgoing_msg = OutgoingRequest(target=self._cb_param_name, callback_request_id=self._callback_request_id, params=kwargs, request_type=self.request_type)
        else:
            outgoing_msg = OutgoingNotification(target=self._cb_param_name, callback_request_id=self._callback_request_id, params=kwargs)
        log.debug(f'Callback Proxy call: {repr(outgoing_msg)}')
        proxy = await self._channel.get_proxy()
        if self.request_type:
            result = await proxy.send_request(outgoing_msg)
            return result
        else:
            await proxy.send_notification(outgoing_msg)


class _Template_IterableCallbackProxy(Iterable, CallbackProxyBase):
    def __init__(self, cb_param_name, callback_request_id, cb_info):
        CallbackProxyBase.__init__(self, cb_param_name, callback_request_id, cb_info)
        self._final = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._final:
            raise StopAsyncIteration()
        
        outgoing_msg = OutgoingRequest(target=self._cb_param_name, callback_request_id=self._callback_request_id)
        log.debug(f'CallbackIterableProxy call: {repr(outgoing_msg)}')
        proxy = await self._channel.get_proxy()
        result, final = await proxy.send_request_for_iter(outgoing_msg)
        if final == FinalType.TERMINATOR:
            raise StopAsyncIteration()
        elif final == FinalType.FINAL:
            self._final = True
        return result



class _Template_InjectorBase(ABC):    
    # FIXME: create base MsgChannel in core so we can assert this is a MsgChannel here.
    def __init__(self, msg_channel, incoming_request: RequestBase, func_info: FuncInfo):
        self.msg_channel = msg_channel
        self.incoming_request = incoming_request
        self.func_info = func_info

    @abstractmethod
    async def pre_call_setup(self):
        raise NotImplementedError
    
    @abstractmethod
    async def get_param(self):
        raise NotImplementedError
    
    @abstractmethod
    async def post_call_cleanup(self):
        raise NotImplementedError


class _Template_MsgChannelInjector(_Template_InjectorBase):
    async def pre_call_setup(self):
        pass

    # FIXME: create base MsgChannel in core so we can assert this is a MsgChannel here.
    async def get_param(self):
        return self.msg_channel
        
    async def post_call_cleanup(self):
        pass
