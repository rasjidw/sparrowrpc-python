# -----------------------------------------------------------------------
# WARNING: This file is auto-generated and should not be edited manually!
#
# It is generated using 'generate_from_template_code.py' using the
# _template_ directory as the source.
# -----------------------------------------------------------------------

from __future__ import annotations

from abc import ABC, abstractmethod
from collections import defaultdict
import inspect
import logging
import sys
from typing import Callable

from ..bases import MsgChannelBase, DEFAULT_SERIALISER_SIG, DEFAULT_ENCODER_TAG

from ..registers import FuncInfo
from ..lib import portable_format_exc, IN_MICROPYTHON, ON_WEBASSEMBLY

import asyncio
from asyncio import Lock, Event
try:
    from asyncio import Queue, QueueEmpty
except (AttributeError, ImportError):
    from uasync.queues import Queue, QueueEmpty # type: ignore

# FIXME: add back ThreadPoolExecutor for non-micropython
# also add @non_blocking decorator
#from concurrent.futures import ThreadPoolExecutor


from binarychain import BinaryChain, ChainReader


from ..engine import ProtocolEngine

from ..messages import (FinalType, Acknowledge, ExceptionResponse, Notification, Request, Response,
                        IterableCallbackInfo, MessageSentEvent, ExceptionCategory, ExceptionInfo, CallBase,
                        RequestCallbackInfo, RequestType, ResponseType, MessageBase)

from ..exceptions import CallerException, CalleeException


log = logging.getLogger(__name__)


# noinspection PyUnreachableCode
def get_thread_or_task_name():
    name_getter = getattr(asyncio.current_task(), 'get_name', None)
    if name_getter:
        return name_getter()
    else:
        return 'dummy'


def is_awaitable(may_be_awaitable):
    if sys.implementation.name == 'micropython':
        # FIXME: review inspect once next release of micropython done
        return type(may_be_awaitable).__name__ == 'generator'
    else:
        return inspect.isawaitable(may_be_awaitable)


async def unwrap_result(raw_result):
    # FIXME: We may not need this once we have the @nonblocking decorator
    unwrapped_result = raw_result
    while True:
        if is_awaitable(unwrapped_result):
            unwrapped_result = await unwrapped_result
        else:
            break
    return unwrapped_result


class AsyncTransportBase(ABC):
    def __init__(self, max_msg_size, max_bc_length, incoming_msg_queue_size, outgoing_msg_queue_size, read_buf_size=8192):
        self.max_msg_size = max_msg_size
        self.max_bc_length = max_bc_length
        self.incoming_queue = Queue(maxsize=incoming_msg_queue_size)  # incoming queue of (BinaryChain, complete, remote_closed)
        self.outgoing_queue = Queue(maxsize=outgoing_msg_queue_size)  # outgoing queue of (BinaryChain, notifier_queue)
        self.read_buf_size = read_buf_size
        self.remote_closed = False
        self.chain_reader = ChainReader(max_part_size=self.max_msg_size, max_chain_size=self.max_msg_size, max_chain_length=self.max_bc_length)
        self.started = False
        self.reader_task = asyncio.create_task(self._reader())
        self.writer_task = asyncio.create_task(self._writer())

    @abstractmethod
    async def _read_data(self, size):
        raise NotImplementedError()

    @abstractmethod
    async def _write_data(self, data):
        raise NotImplementedError()
    
    async def start(self):
        if not self.started:
            self.started = True

    async def _reader(self):
        while True:
            try:
                data = await self._read_data(self.read_buf_size)
                if isinstance(data, (bytearray, bytes)):
                    for incoming_chain in self.chain_reader.get_binary_chains(data):
                        queue_data = (incoming_chain, self.chain_reader.complete(), self.remote_closed)
                        log.debug(f'Put Incoming queue_data: {queue_data}')
                        await self.incoming_queue.put(queue_data)
                else:
                    break
            except Exception as e:
                log.error(f'Reader aborting with exception {e!s}')
                break
        self.remote_closed = True
        try:
            # put sentinel on incoming queue
            queue_data = (None, self.chain_reader.complete(), self.remote_closed)
            log.debug(f'Put Incoming queue_data: (Sentinel) {queue_data}')
            await self.incoming_queue.put(queue_data)
        except Exception as e:
            log.error(f'Error putting sentinel on incoming queue - {e!s}')

    async def _writer(self):
        try:
            while True:
                time_to_stop = await self._writer_send_one()
                if time_to_stop:
                    break
        except Exception as e:
            log.error(f'Unexpected error in _writer - {e!s}')

    async def _writer_send_one(self):
        try:
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
                log.error(f'Exception on sending chain: {chain}')
                log.error(portable_format_exc(exc))
                e = exc
            await notifier_queue.put(e)
            return False
        except Exception as e:
            log.error(f'Writer aborting with exception: {e!s}')
        return True

    # FIXME: Do we even need this now - perhaps just read directly from the queue?
    async def get_next_binary_chain(self):
        log.debug('Starting in get_binary_chains')
        binary_chain, complete, remote_closed = await self.incoming_queue.get()
        return (binary_chain, complete, remote_closed)

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
        # signal outgoing queue to stop
        await self.outgoing_queue.put(None)
        queue_data = (None, None, None)
        await self.incoming_queue.put(queue_data)  # end incoming queue
        await self.close()
        try:
            await self.reader_task
        except Exception as e:
            log.error(f'reader_task error:\n{portable_format_exc(e)}')
        try:
            await self.writer_task
        except Exception as e:
            log.error(f'writer_task error:\n{portable_format_exc(e)}')
        
    @abstractmethod
    async def close(self):
        raise NotImplementedError()


class AsyncDispatcherBase(ABC):
    @abstractmethod
    async def dispatch_incoming(self, msg_channel: AsyncMsgChannel, request: CallBase, func_info: FuncInfo):
        raise NotImplementedError()
    @abstractmethod
    async def shutdown(self, timeout=0):
        raise NotImplementedError()


# FIXME: Turn this into a class
async def async_call_func(msg_channel: AsyncMsgChannel, incoming_msg: Request|Notification, func_info: FuncInfo):
    log.debug(f'In async_call_func with {incoming_msg} and {func_info}')
    params = dict()
    injectors = list()
    if func_info.injectable_params:
        for param_name, injector_cls in func_info.injectable_params.items():
            assert issubclass(injector_cls, AsyncInjectorBase)
            injector = injector_cls(msg_channel, incoming_msg, func_info)
            injectors.append(injector)
            await injector.pre_call_setup()
            params[param_name] = await injector.get_param()

    if func_info.func:
        func = func_info.func
    else:
        log.debug('Calling Iterable __anext__')
        # noinspection PyUnresolvedReferences
        func = func_info.iterable_callback.__anext__

    # normal non-binary request
    if incoming_msg.params:
        for param_name, value in incoming_msg.params.items():
            if param_name in params:
                raise ValueError('duplicate param name')  # FIXME: make a caller error
            params[param_name] = value
    if isinstance(incoming_msg, Request) and incoming_msg.callback_params:
        for (param_name, cb_data) in incoming_msg.callback_params.items():
            if param_name in params:
                raise ValueError('duplicate param name')  # FIXME: make a caller error
            assert isinstance(cb_data, dict)
            proxy_type, cb_param_data = list(cb_data.items())[0]
            callback_request_id = cb_param_data['callback_request_id']
            cb_info = cb_param_data['cb_info']
            if proxy_type == '#cb':
                cb_proxy = AsyncCallbackProxy(param_name, callback_request_id, cb_info)
            elif proxy_type == '#icb':
                cb_proxy = AsyncIterableCallbackProxy(param_name, callback_request_id, cb_info)
            else:
                raise ValueError(f'Invalid callback proxy type {proxy_type}')
            assert isinstance(cb_proxy, CallbackProxyBase)
            cb_proxy.set_channel(msg_channel)
            params[param_name] = cb_proxy
    try:
        raw_result = func(**params)  # FIXME - should put in thread pool if not flagged as non-blocking.
        result = await unwrap_result(raw_result)
    except StopAsyncIteration:
        # pass this up, but don't log as this is normal for stopping multipart requests
        raise
    except Exception as e:
        log.warning('-' * 20)
        log.warning(portable_format_exc(e))
        log.warning('-' * 20)
        raise

    # FIXME: Currently the post_call_cleanup is missed if there is an exception
    for injector in injectors:
        assert isinstance(injector, AsyncInjectorBase)
        await injector.post_call_cleanup()
    return result

async def async_run_request_wait_to_complete(msg_channel: AsyncMsgChannel, request: Request,
                                                  func_info: FuncInfo, completed_callback=None):
    try:
        if func_info.multipart_response:
            async for part_result in await async_call_func(msg_channel, request, func_info):
                outgoing_msg = Response(request_id=request.message_id, result=part_result,
                                        response_type=ResponseType.MULTIPART,
                                        envelope_serialisation_code=request.envelope_serialisation_code,
                                        payload_serialisation_code=request.payload_serialisation_code,
                                        protocol_version=request.protocol_version)
                await msg_channel._send_message(outgoing_msg)
            final_response = Response(request_id=request.message_id, response_type=ResponseType.MULTIPART,
                                      final=FinalType.TERMINATOR,
                                      envelope_serialisation_code=request.envelope_serialisation_code,
                                      payload_serialisation_code=request.payload_serialisation_code,
                                      protocol_version=request.protocol_version)
            await msg_channel._send_message(final_response)
        else:
            result = await async_call_func(msg_channel, request, func_info)
            if request.request_type == RequestType.QUIET:
                # never send the result to quiet requests.
                result = None
            outgoing_msg = Response(request_id=request.message_id, result=result,
                                    envelope_serialisation_code=request.envelope_serialisation_code,
                                    payload_serialisation_code=request.payload_serialisation_code,
                                    protocol_version=request.protocol_version)
            await msg_channel._send_message(outgoing_msg)
    except Exception as e:
        if func_info.iterable_callback and isinstance(e, StopAsyncIteration):
            outgoing_msg = Response(request_id=request.message_id, result=None, final=FinalType.TERMINATOR,
                                    envelope_serialisation_code=request.envelope_serialisation_code,
                                    payload_serialisation_code=request.payload_serialisation_code,
                                    protocol_version=request.protocol_version)
        else:
            log.debug(portable_format_exc(e))
            if isinstance(e, CallerException):
                exc_info = ExceptionInfo(category=ExceptionCategory.CALLER, exc_type=type(e).__name__, msg=str(e))
            else:
                exc_info = ExceptionInfo(category=ExceptionCategory.CALLEE, exc_type=type(e).__name__, msg=str(e))
            outgoing_msg = ExceptionResponse(request_id=request.message_id, exc_info=exc_info,
                                             envelope_serialisation_code=request.envelope_serialisation_code,
                                             protocol_version=request.protocol_version
                                             )
        await msg_channel._send_message(outgoing_msg)

    if completed_callback:
        task = asyncio.current_task()
        try:
            await completed_callback(task, request)
        except Exception as e:
            log.error(f'Error calling completed_callback: {e!s}')


async def async_run_request_not_waiting(msg_channel: AsyncMsgChannel, request: Request|Notification,
                                             func_info: FuncInfo, completed_callback=None):
    try:
        # we send a response (with no data) to silent requests (before calling the function), but nothing for notifications.
        if isinstance(request, Request) and request.request_type == RequestType.SILENT:
            outgoing_msg = Response(request_id=request.message_id)
            await msg_channel._send_message(outgoing_msg)

        await async_call_func(msg_channel, request, func_info)
    except Exception as e:
        # FIXME - make notification and silent errors available to the client software
        log.warning(f'Notification or Silent Request {request} raised error {str(e)}')

    if completed_callback:
        task = asyncio.current_task()
        try:
            await completed_callback(task, request)
        except Exception as e:
            log.error(f'Error calling completed_callback: {e!s}')


async def async_dispatch_request_or_notification(msg_channel, incoming_msg, func_info, completed_callback=None):
    try:
        if isinstance(incoming_msg, Request):
            if incoming_msg.request_type == RequestType.SILENT:
                await async_run_request_not_waiting(msg_channel, incoming_msg, func_info, completed_callback)
            else:
                await async_run_request_wait_to_complete(msg_channel, incoming_msg, func_info, completed_callback)
        elif isinstance(incoming_msg, Notification):
            await async_run_request_not_waiting(msg_channel, incoming_msg, func_info, completed_callback)
        else:
            log.warning(f'Got unhandled message {incoming_msg}')
    except Exception as e:
        # FIXME: What do we do here!
        log.error(f'>>>> Dispatch request error: {str(e)} <<<<')


class AsyncDispatcher(AsyncDispatcherBase):
    def __init__(self, num_threads, queue_size=10):
        if num_threads < 1:
            raise ValueError('num_threads must be at least 1')
        self.incoming_queue = Queue(queue_size)
        self.time_to_stop = False

        self.tasks = set()
        self.completed_tasks_queue = Queue()  # using this since MicroPython does not have add_done_callback on tasks
        #self.executor = ThreadPoolExecutor(max_workers=num_threads)  # FIXME: Not using this yet. For non-async functions exported
        self.task_fetcher_task = asyncio.create_task(self._async_task_fetcher())
        self.completed_task_awaiter_task = asyncio.create_task(self._cleanup_completed_tasks())

    async def _async_task_fetcher(self):
        while not self.time_to_stop:
            try:
                msg_channel, incoming_msg, func_info = await asyncio.wait_for(self.incoming_queue.get(), timeout=1)
            except Exception as e:
                if not isinstance(e, asyncio.TimeoutError):
                    log.error(f'*** Got error {str(e)} in _async_task_fetcher - continuing')
                continue
            try:
                task = asyncio.create_task(async_dispatch_request_or_notification(msg_channel, incoming_msg, func_info,
                                                                                        completed_callback=self._task_done_callback))
                self.tasks.add(task)
            except Exception:
                log.error(f'Error creating task for {incoming_msg}!')

    async def _cleanup_completed_tasks(self):
        while not self.time_to_stop:
            try:
                task = await asyncio.wait_for(self.completed_tasks_queue.get(), timeout=1)
            except Exception as e:
                if not isinstance(e, asyncio.TimeoutError):
                    log.error(f'Got exception {e!s} waiting for completed tasks')
                continue

            try:
                # await the completed task to get any exceptions
                await task
                log.debug(f'Task {task!s} awaited')
            except Exception as e:
                log.warning(f'Exception happened in task {task!s}')
                log.warning(portable_format_exc(e))

            try:
                self.tasks.remove(task)
                log.debug(f'Task {task!s} clean up complete')
            except KeyError:
                log.error(f'ERROR: task {task!s} not found!')

    async def _task_done_callback(self, task, incoming_message):
        log.debug(f'**** Task {task!s} for {incoming_message} is done. ****')
        try:
            await self.completed_tasks_queue.put(task)
        except Exception:
            log.error(f'ERROR: Unable to put task {task} onto completed_tasks_queue.')

    async def dispatch_incoming(self, msg_channel: AsyncMsgChannel, request: Request|Notification,
                                func_info: FuncInfo):
        queue_item = (msg_channel, request, func_info)
        await self.incoming_queue.put(queue_item)

    async def shutdown(self, timeout=10):  # FIXME: What timeout do we really want?
        log.debug('Shutting down dispatcher')
        self.time_to_stop = True
        await self.task_fetcher_task
        await self.completed_task_awaiter_task
        for task in self.tasks:
            try:
                task.cancel()
                await task
            except Exception as e:
                log.error(f'Exception in task {task} - {str(e)}')
        log.debug('Dispatcher shut down')


class AsyncMsgChannel(MsgChannelBase):
    def __init__(self, transport: AsyncTransportBase, initiator: bool, engine: ProtocolEngine,
                 dispatcher: AsyncDispatcherBase, default_serialiser_sig: str=DEFAULT_SERIALISER_SIG,
                 default_encoder_tag: str=DEFAULT_ENCODER_TAG,
                 channel_tag='', func_registers=None, channel_register=None):
        super().__init__(initiator, engine, default_serialiser_sig, default_encoder_tag,
                         channel_tag, func_registers, channel_register)
        self.transport = transport
        self.dispatcher = dispatcher
        self.request = AsyncRequestProxyMaker(self)
        self._message_id_lock = Lock()
        self._msg_reader_task = None
        
    def get_proxy(self):
        return AsyncChannelProxy(self)
    
    async def start_channel(self):
        await self.transport.start()
        self._msg_reader_task = asyncio.create_task(self._incoming_msg_pump())
        # asyncio.ensure_future(self._msg_reader_task)
        self._channel_register.register(self)
        log.debug(f'Channel {self} registered')

    async def wait_for_remote_close(self):
        log.debug(f'Waiting for incoming message pump to finish on {get_thread_or_task_name()}')
        await self._msg_reader_task

    async def _incoming_msg_pump(self):
        try:
            log.debug(f'message pump started on thread {get_thread_or_task_name()}')
            last_complete = True
            while True:
                (bin_chain, complete, remote_closed) = await self.transport.incoming_queue.get()
                if bin_chain is None:
                    # NOTE: complete is None is okay, as that is the shutdown marker
                    if not last_complete:
                        log.warning('Got end of chains but not complete')
                    break

                last_complete = complete
                message, dispatch, incoming_callback = self._parse_and_allocate_bin_chain(bin_chain)
                log.debug(f'** Incoming message: {message}, Dispatch: {dispatch}, Callback: {incoming_callback}')
                if dispatch:
                    await self._dispatch(message)
                if incoming_callback:
                    await incoming_callback(message)
        except Exception as e:
            log.error(f'_incoming_msg_pump stopped with exception {e!s}')
        log.debug(f'message pump stopped on thread {get_thread_or_task_name()}')

    async def _dispatch(self, message: Request|Notification):
        func_info, ack_err_msg = self._get_func_info_and_ack_err_msg(message)
        if ack_err_msg:
            await self._send_message(Acknowledge(request_id=message.message_id))
        if func_info:
            await self.dispatcher.dispatch_incoming(self, message, func_info)

    async def _create_message_id(self):
        async with self._message_id_lock:
            message_id = self._message_id
            self._message_id += 1
            return message_id

    async def _send_message(self, message: MessageBase, message_event_callback = None,
                            add_id_callback: Callable = None):
        if hasattr(message, 'message_id') and message.message_id is not None:
                raise ValueError('message_id must not be pre-set')

        if message.envelope_serialisation_code is None:
            message.envelope_serialisation_code = self.default_serialiser_sig

        if message.protocol_version is None:
            message.protocol_version = self.default_encoder_tag

        add_id, register_event_callback = self._get_add_id_and_reg_cb(message)
        if add_id:
            message_id = await self._create_message_id()
            if add_id_callback:
                add_id_callback(message_id)
        else:
            message_id = None
        if register_event_callback:
            self._reg_callback(message_id, message_event_callback)

        if message_id:
            message.message_id = message_id

        bc = self.engine.message_to_binary_chain(message)
        await self.transport.send_binary_chain(bc)
        if message_event_callback:
            await message_event_callback(MessageSentEvent(request_id=message_id))
        return message_id
    
    async def queue_message(self, message, message_event_callback: Callable, add_id_callback: Callable = None):
        return await self._send_message(message, message_event_callback, add_id_callback)

    async def send_shutdown_pending(self):
        # FIXME
        pass

    async def shutdown_channel(self):
        # FIXME: send ?
        self._channel_register.unregister(self)
        await self.transport.shutdown()
        await self._msg_reader_task
        log.debug(f'Channel {self} unregistered and msg_reader thread cleaned up')


# FIXME: Test timeouts!
class AsyncChannelProxy:
    def __init__(self, channel: AsyncMsgChannel):
        self.channel = channel
        self._callbacks = defaultdict(dict)   # self._callbacks[request_id][param_name] = cb_info

    async def send_request_multipart_result_as_iterator(self, message: Request, timeout=None, msg_sent_callback=None, ack_callback=None):
        response_pump = AsyncResponsePump(self, msg_sent_callback, ack_callback, expected_response_type=ResponseType.MULTIPART)
        await response_pump.send_message(message, timeout)
        return response_pump

    async def send_request(self, message: Request, timeout=None, msg_sent_callback=None, ack_callback=None):
        try:
            response_pump = AsyncResponsePump(self, msg_sent_callback, ack_callback)
            await response_pump.send_message(message, timeout)
            result = await response_pump.__anext__()
            # FIXME: Check complete etc?
            return result
        except Exception as e:
            log.error(f'Exception in send_request. {e!s}')
            raise e

    async def send_request_for_iter(self, message: Request, timeout=None, msg_sent_callback=None, ack_callback=None):
        response_pump = AsyncResponsePump(self, msg_sent_callback, ack_callback, callback_iterable=True)
        await response_pump.send_message(message, timeout)
        result = await response_pump.__anext__()
        # FIXME: Check complete etc?
        return result
    
    async def send_notification(self, message: Notification, timeout=None):
        await self._send_message_wait_for_sent_event(message, timeout)

    async def _send_message_wait_for_sent_event(self, message, timeout=None):
        wait_event = Event()
        async def msg_sent_cb(event):
            wait_event.set()
        await self.channel.queue_message(message, msg_sent_cb)
        try:
            await asyncio.wait_for(wait_event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            log.error('Timeout error on notification send')  # FIXME: Do we raise an error?


    async def send_request_raw_async(self, message: Request, event_callback):
        reg_id_for_callbacks = None
        if isinstance(message, Request) and message.callback_params:
            # avoid a possible race by registering the req_id for any callbacks prior to sending the message
            def reg_id_for_callbacks(req_id):
                for param_name, cb_info in message.callback_params.items():
                    self._callbacks[req_id][param_name] = cb_info

        req_id = await self.channel.queue_message(message, event_callback, add_id_callback = reg_id_for_callbacks)
        return req_id
    
    async def send_request_wait_via_callbacks(self, message: Request, on_msg_sent=None, on_ack=None, on_result=None):
        async def event_callback(event):
            if isinstance(event, MessageSentEvent):
                if on_msg_sent:
                    await on_msg_sent(event)
            elif isinstance(event, Acknowledge):
                if on_ack:
                    await on_ack(event)
            elif isinstance(event, Response):
                if on_result:
                    await on_result(event)
            else:
                log.error(f'Unhandled event type {type(event)}!')  # FIXME: should double check we have everything covered 
        return await self.send_request_raw_async(message, event_callback)


# FIXME: Do we just always return the result and event.final??? Makes the API more consistent
class AsyncResponsePump:
    def __init__(self, channel_proxy: AsyncChannelProxy, msg_sent_callback=None, ack_callback=None,
                 expected_response_type: ResponseType=ResponseType.NORMAL,
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

    async def _cb_reader(self, event):
        await self.return_queue.put(event)
        log.debug(f'Put event {event!s} on return queue')

    async def send_message(self, message: Request, timeout=None):
        self.timeout = timeout
        if isinstance(message, Notification):
            self.sent_notification = True
        await self.channel_proxy.send_request_raw_async(message, self._cb_reader)

    async def call_msg_sent_callback(self, event):
        if self.msg_sent_callback:
            # would prefer to use asyncio.isawaitable to detect, but
            # this is micropython compatible.
            may_be_awaitable = self.msg_sent_callback(event)
            try:
                await may_be_awaitable
            except TypeError:
                pass
        else:
            log.debug(f'Got msg sent event')

    async def call_ack_callback(self, event):
        if self.ack_callback:
            may_be_awaitable = self.ack_callback(event)
            try:
                await may_be_awaitable
            except TypeError:
                pass
        else:
            log.info(f'Incoming ack received, but no callback passed in')

    async def process_response(self, event):
        # check we have an expected NORMAL or MULTIPART response
        if event.response_type != self.expected_response_type:
            raise ValueError(f'Expected response type {self.expected_response_type} but got {event.response_type}')
        if event.response_type == ResponseType.NORMAL:
            if self.callback_iterable:
                return event.result, event.final
            else:
                return event.result
        elif event.response_type == ResponseType.MULTIPART:
            if event.final == FinalType.FINAL:
                self.complete = True
                return event.result
            elif event.final == FinalType.TERMINATOR:
                self.complete = True
                raise StopAsyncIteration()
            else:
                return event.result
        raise RuntimeError(f'Invalid response type for event {event!s}')
            
    async def process_incoming_req_or_notification(self, event):
        try:
            func_info = None
            cb_info = self.channel_proxy._callbacks[event.callback_request_id][event.target]
            if isinstance(cb_info, RequestCallbackInfo):
                func_info = FuncInfo(target_name=event.target, func=cb_info.func)
            elif isinstance(cb_info, IterableCallbackInfo):
                func_info = FuncInfo(target_name=event.target, iterable_callback=cb_info.iter)
            else:
                log.error(f'Invalid cb_info type of {type(cb_info)}')
            log.debug(f'Calling dispatch with {func_info}')
            await async_dispatch_request_or_notification(self.channel_proxy.channel, event, func_info)
        except KeyError:
            log.error(f'No callback found for incoming callback request {event}')

    async def process_incoming_exception(self, event):
        exc_info = event.exc_info
        assert isinstance(exc_info, ExceptionInfo)
        e = None
        if exc_info.category == ExceptionCategory.CALLER:
            for cls in CallerException.get_subclasses():
                if exc_info.exc_type == cls.__name__:
                    raise cls(exc_info.msg)
            raise CallerException(f'Type: {exc_info.exc_type}, Msg: {exc_info.msg}')
        elif exc_info.category == ExceptionCategory.CALLEE:
            raise CalleeException(f'Type: {exc_info.exc_type}, Msg: {exc_info.msg}')

    async def get_next_event_with_timeout(self):
        while True:
            try:
                log.debug('Waiting for return_queue...')
                if IN_MICROPYTHON and ON_WEBASSEMBLY:
                    # FIXME: work around a crash in micropython on webassembly (pyscript)
                    # means we don't have timeout support (which is not fully developed yet anyway....)
                    # Can be removed once https://github.com/pyscript/pyscript/issues/2415 is fixed in production
                    result = await self.return_queue.get()
                else:
                    result = await asyncio.wait_for(self.return_queue.get(), timeout=1)
                log.debug(f'Waiting for return_queue. Got result: {result}')
                return result
            except Exception as e:
                if not isinstance(e, asyncio.TimeoutError):
                    log.error(f'Get error {str(e)} waiting for timeout')
                self.count += 1
                if self.timeout and self.count > self.timeout:
                    raise RuntimeError('TIMEOUT')  # FIXME. We should at least clean up incoming registers etc. Do we notify the callee?

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            while True:
                log.debug(f'Waiting for event in __anext__')
                try:
                    event = await self.get_next_event_with_timeout()
                except Exception as e:
                    log.error(f'Got exception {str(e)} in __anext__. Continuing...')
                    continue

                log.debug(f'Got event in __anext__: {event}')
                if isinstance(event, MessageSentEvent):
                    await self.call_msg_sent_callback(event)
                    if self.sent_notification:
                        self.complete = True
                        raise StopAsyncIteration
                    continue

                if isinstance(event, Acknowledge):
                    await self.call_ack_callback(event)
                    continue

                if isinstance(event, Response):
                    return await self.process_response(event)

                if isinstance(event, Request) or isinstance(event, Notification):
                    await self.process_incoming_req_or_notification(event)
                    continue

                if isinstance(event, ExceptionResponse):
                    await self.process_incoming_exception(event)

                log.error(f'Unhandled event {event}')
        except Exception as e:
            if not isinstance(e, StopAsyncIteration):
                log.error(f'Got exception {str(e)} in __anext__')
            raise e

class AsyncRequestProxy:
    def __init__(self, msg_channel: AsyncMsgChannel, target: str, namespace: str=None, node: str=None,
                 request_type: RequestType=RequestType.NORMAL, timeout: int=None, msg_sent_callback=None,
                 ack_callback=None, multipart_response=False):
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
            if hasattr(value, '__aiter__') and hasattr(value, '__anext__'):
                callback_params[param] = IterableCallbackInfo(iter=value)
            elif callable(value):
                callback_params[param] = RequestCallbackInfo(func=value)
            else:
                params[param] = value
        request = Request(target=self._target, namespace=self._namespace, node=self._node, params=params,
                          callback_params=callback_params, request_type=self._request_type,
                          acknowledge=bool(self._ack_callback))
        channel_proxy = self._msg_channel.get_proxy()
        if self._multipart_response:
            return channel_proxy.send_request_multipart_result_as_iterator(request, timeout=self._timeout,
                                                                           msg_sent_callback=self._msg_sent_callback,
                                                                           ack_callback=self._ack_callback)
        else:
            return channel_proxy.send_request(request, timeout=self._timeout, msg_sent_callback=self._msg_sent_callback,
                                              ack_callback=self._ack_callback)
    

class AsyncRequestProxyMaker:
    def __init__(self, msg_channel: AsyncMsgChannel, namespace: str=None, node: str=None,
                 request_type: RequestType=RequestType.NORMAL, timeout: int=None, msg_sent_callback=None,
                 ack_callback=None, multipart_response=False):
        self._msg_channel = msg_channel
        self._namespace = namespace
        self._node = node
        self._request_type = request_type
        self._timeout = timeout
        self._msg_sent_callback = msg_sent_callback
        self._ack_callback = ack_callback
        self._multipart_response = multipart_response

    def __getattr__(self, target):
        return AsyncRequestProxy(self._msg_channel, target, self._namespace, self._node, self._request_type,
                                      self._timeout, self._msg_sent_callback, self._ack_callback,
                                      self._multipart_response)
    
    def __call__(self, namespace: str=None, node: str=None, request_type: RequestType = RequestType.NORMAL,
                 timeout: int=None, msg_sent_callback=None, ack_callback=None, multipart_response=False):
        return AsyncRequestProxyMaker(self._msg_channel, namespace, node, request_type, timeout, msg_sent_callback,
                                           ack_callback, multipart_response)



class CallbackProxyBase:
    def __init__(self, cb_param_name, callback_request_id, cb_info):
        self._cb_param_name = cb_param_name
        self._callback_request_id = callback_request_id
        self.cb_info = cb_info
        self._channel = None

    def set_channel(self, channel):
        # FIXME: create base MsgChannel in core so we can assert this is a MsgChannel here.
        self._channel = channel


class AsyncCallbackProxy(CallbackProxyBase):
    def __init__(self, cb_param_name, callback_request_id, cb_info):
        super().__init__(cb_param_name, callback_request_id, cb_info)
        self.request_type = None  # can be changed before sending / FIXME: how we do set a default?

    def set_to_notification(self):
        self.request_type = None

    def set_to_request(self, request_type: RequestType):
        self.request_type = request_type

    async def __call__(self, **kwargs):
        if self.request_type:
            outgoing_msg = Request(target=self._cb_param_name, callback_request_id=self._callback_request_id,
                                   params=kwargs, request_type=self.request_type)
        else:
            outgoing_msg = Notification(target=self._cb_param_name, callback_request_id=self._callback_request_id,
                                        params=kwargs)
        log.debug(f'Callback Proxy call: {repr(outgoing_msg)}')
        proxy = self._channel.get_proxy()
        if self.request_type:
            result = await proxy.send_request(outgoing_msg)
            return result
        else:
            await proxy.send_notification(outgoing_msg)
            return None


class AsyncIterableCallbackProxy(CallbackProxyBase):
    def __init__(self, cb_param_name, callback_request_id, cb_info):
        super().__init__(cb_param_name, callback_request_id, cb_info)
        self._final = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._final:
            raise StopAsyncIteration()
        
        outgoing_msg = Request(target=self._cb_param_name, callback_request_id=self._callback_request_id)
        log.debug(f'CallbackIterableProxy call: {repr(outgoing_msg)}')
        proxy = self._channel.get_proxy()
        result, final = await proxy.send_request_for_iter(outgoing_msg)
        if final == FinalType.TERMINATOR:
            raise StopAsyncIteration()
        elif final == FinalType.FINAL:
            self._final = True
        return result



class AsyncInjectorBase(ABC):    
    # FIXME: create base MsgChannel in core so we can assert this is a MsgChannel here.
    def __init__(self, msg_channel, incoming_request: CallBase, func_info: FuncInfo):
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


class AsyncMsgChannelInjector(AsyncInjectorBase):
    async def pre_call_setup(self):
        pass

    # FIXME: create base MsgChannel in core so we can assert this is a MsgChannel here.
    async def get_param(self):
        return self.msg_channel
        
    async def post_call_cleanup(self):
        pass
