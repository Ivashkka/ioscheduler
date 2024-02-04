import asyncio
import queue
import uuid as ud
from typing import Union

class _StdinCall:
    __slots__ = ['data', 'proc_pid', 'call_uuid', 'tag']
    def __init__(self, data, proc_pid : int, call_uuid, tag):
        self.data = data
        self.tag = tag
        self.proc_pid = proc_pid
        self.call_uuid = call_uuid

class _StdoutCall:
    __slots__ = ['data', 'proc_pid', 'call_uuid', 'tag']
    def __init__(self, data, proc_pid : int, call_uuid, tag):
        self.data = data
        self.tag = tag
        self.proc_pid = proc_pid
        self.call_uuid = call_uuid

class _StderrCall:
    __slots__ = ['data', 'proc_pid', 'call_uuid', 'tag']
    def __init__(self, data, proc_pid : int, call_uuid, tag):
        self.data = data
        self.tag = tag
        self.proc_pid = proc_pid
        self.call_uuid = call_uuid


class _SemaphoredList:
    __slots__ = ['_unsafe_list', '_viewing_count']

    def __init__(self, unsafe_list : list):
        self._unsafe_list = unsafe_list
        self._viewing_count = 0

    def _open_list(self):
        self._viewing_count += 1
        if self._viewing_count > 1 : raise Exception("_SemaphoredList counter above 1!")
        return self._unsafe_list

    def _close_list(self):
        self._viewing_count -= 1

    def _join(self):
        while self._viewing_count > 0:
            if self._viewing_count < 0 : raise Exception("_SemaphoredList counter below zero!")




class Transfer:
    _AllTransfers = _SemaphoredList([]) # static field
    _AliveCount = 0 # static field

    _alive = False
    _stdout_in_iteration = False # only for free
    _stdin_in_iteration = False # only for free
    _stdinout_in_iteration = False # only for blocking
    _in_init_state = False
    _holder = None
    _skip_next_iteration = False

    locals = None
    tag = None

    def __init__(self, holder, locals : object, tag : str):
        self._stdout = queue.Queue()
        self._stdin = queue.Queue()
        self._stdreq = queue.Queue()
        self._stderr = queue.Queue()
        self._holder = holder
        self.locals = locals
        self.tag = tag
        self._in_init_state = True
        self._alive = True
        Transfer._AliveCount += 1

    @staticmethod
    def create_transfer(holder, locals : object, tag : str):
        self_transfer = Transfer(holder, locals, tag)
        Transfer._AllTransfers._join()
        allTransfers = Transfer._AllTransfers._open_list()
        for tr in allTransfers:
            if tr.tag == self_transfer.tag:
                raise Exception("Can't create second transfer with the same tag!")
        allTransfers.append(self_transfer)
        Transfer._AllTransfers._close_list()

    async def _stdout_iteration_call(self):
        self._stdout_in_iteration = True

        if self._in_init_state == True:
            await self._holder.on_start(self)
            self._in_init_state = False
            self._stdout_in_iteration = False
            return

        if self._stdout.empty() == True or self._holder.is_stdout_available(self) != True:
            self._stdout_in_iteration = False
            return
        first_stdout_call = self._stdout.get()
        request_pid = None
        stderr_return, request_uuid = await self._holder.stdout(self, first_stdout_call.data, first_stdout_call.call_uuid)
        if request_uuid == None: request_pid = first_stdout_call.proc_pid
        self._stderr.put(_StderrCall(stderr_return, request_pid, request_uuid, self.tag))
        if self._holder.is_fatal(self, stderr_return) == True:
            await self._holder.on_fatal(self)

        self._stdout_in_iteration = False


    async def _stdin_iteration_call(self):
        self._stdin_in_iteration = True
        if self._in_init_state == True:
            self._stdout_in_iteration = False
            return
        if self._stdreq.empty() == True or self._holder.is_stdin_available(self) != True:
            self._stdin_in_iteration = False
            return
        first_stdreq_call = self._stdreq.get()
        request_pid = None
        stdin_return, stderr_return, request_uuid = await self._holder.stdin(self)
        if request_uuid == None: request_pid = first_stdreq_call.proc_pid
        self._stderr.put(_StderrCall(stderr_return, request_pid, request_uuid, self.tag))
        self._stdin.put(_StdinCall(stdin_return, request_pid, request_uuid, self.tag))
        if self._holder.is_fatal(self, stderr_return) == True:
            await self._holder.on_fatal(self)

        self._stdin_in_iteration = False

    async def _stdinout_iteration_call(self): # only for blocking
        if self._skip_next_iteration == True:
            self._skip_next_iteration = False
            return
        self._stdinout_in_iteration = True
        if self._stdout.empty() == False and self._holder.is_stdout_available(self) == True:
            all_stdout_data = queue.Queue()
            while self._stdout.empty() != True:
                all_stdout_data.put(self._stdout.get())
            while all_stdout_data.empty() != True:
                first_stdout_call = all_stdout_data.get()
                stderr_return = await self._holder.stdout(self, first_stdout_call.data)
                self._stderr.put(_StderrCall(stderr_return, first_stdout_call.proc_pid, None, self.tag))
                if self._holder.is_fatal(self, stderr_return) == True:
                    await self._holder.on_fatal(self)
                    break
        if self._stdreq.empty() == False and self._holder.is_stdin_available(self) == True:
            self._skip_next_iteration = True
            all_stdreq_data = queue.Queue()
            while self._stdreq.empty() != True:
                all_stdreq_data.put(self._stdreq.get())
            while all_stdreq_data.empty() != True:
                first_stdin_awaits = all_stdreq_data.get()
                stderr_return = await self._holder.stdout(self, first_stdin_awaits.data)
                if self._holder.is_fatal(self, stderr_return) == True:
                    self._stderr.put(_StderrCall(stderr_return, first_stdin_awaits.proc_pid, None, self.tag))
                    self._stdin.put(_StdinCall(None, first_stdin_awaits.proc_pid, None, self.tag))
                    await self._holder.on_fatal(self)
                    break
                stdin_return, stderr_return = await self._holder.stdin(self)
                self._stderr.put(_StderrCall(stderr_return, first_stdin_awaits.proc_pid, None, self.tag))
                self._stdin.put(_StdinCall(stdin_return, first_stdin_awaits.proc_pid, None, self.tag))
                if self._holder.is_fatal(self, stderr_return) == True:
                    await self._holder.on_fatal(self)
                    break
        self._stdinout_in_iteration = False

    async def stop(self):
        await self._holder.on_stop(self)
        self._alive = False
        Transfer._AliveCount -= 1

    def is_alive(self):
        return self._alive

    def is_stdout(self):
        return self._stdout_in_iteration

    def is_stdin(self):
        return self._stdin_in_iteration

    def is_stdinout(self): # only for blocking
        return self._stdinout_in_iteration



class BlockingTransferHolder:
    ### rewrite functions ###
    @staticmethod
    async def stdout(transfer : Transfer, data):
        return None

    @staticmethod
    async def stdin(transfer : Transfer):
        return None, None

    @staticmethod
    async def on_fatal(transfer : Transfer):
        return

    @staticmethod
    async def on_start(transfer : Transfer):
        return

    @staticmethod
    async def on_stop(transfer : Transfer):
        return

    @staticmethod
    def is_stdout_available(transfer : Transfer):
        return True

    @staticmethod
    def is_stdin_available(transfer : Transfer):
        return True

    @staticmethod
    def is_fatal(transfer : Transfer, return_code):
        return False
    ### rewrite functions ###


class FreeTransferHolder:
    ### rewrite functions ###
    @staticmethod
    async def stdout(transfer : Transfer, data, request_uuid : str):
        return None, request_uuid

    @staticmethod
    async def stdin(transfer : Transfer):
        request_uuid = None
        return None, None, request_uuid

    @staticmethod
    async def on_fatal(transfer : Transfer):
        return

    @staticmethod
    async def on_start(transfer : Transfer):
        return

    @staticmethod
    async def on_stop(transfer : Transfer):
        return

    @staticmethod
    def is_stdout_available(transfer : Transfer):
        return True

    @staticmethod
    def is_stdin_available(transfer : Transfer):
        return True

    @staticmethod
    def is_fatal(transfer : Transfer, return_code):
        return False
    ### rewrite functions ###


class Process:
    _AllProcesses = _SemaphoredList([]) # static field
    _AliveCount = 0 # static field
    _NextPid = 1
    _alive = False
    _in_iteration = False
    _in_init_state = False

    locals = None
    tag = None
    pid = None
    _interrupt = None
    return_value = None
    _holder = None

    def __init__(self, holder, locals : object, tag : str):
        self.pid = Process._NextPid
        Process._NextPid += 1
        if Process._NextPid >= 1000: Process._NextPid = 1
        self._holder = holder
        self.locals = locals
        self.tag = tag
        self._alive = True
        self._in_init_state = True
        Process._AliveCount += 1

    @staticmethod
    def create_process(holder, locals : object, tag : str):
        self_process = Process(holder, locals, tag)
        Process._AllProcesses._join()
        allProcesses = Process._AllProcesses._open_list()
        allProcesses.append(self_process)
        Process._AllProcesses._close_list()

    async def _body_iteration_call(self):
        self._in_iteration = True
        if self._in_init_state == True:
            await self._holder.on_start(self)
            self._in_init_state = False
            self._in_iteration = False
            return
        if self._holder.is_body_available(self) != True:
            self._in_iteration = False
            return
        return_value = await self._holder.body(self)
        self.return_value = return_value
        if self._holder.is_fatal(self, return_value):
            await self._holder.on_fatal(self)
        self._in_iteration = False

    async def output_interrupt(self, proc_output : object = None, track_uuid : str = ud.uuid4().hex[:20]):
        self._interrupt = _Interrupt(self.pid, self.tag, False, False, track_uuid, proc_output)
        while self._interrupt.freeze == True:
            await asyncio.sleep(0)
        code = self._interrupt.transfer_stderr
        self._interrupt = None
        return code.data

    async def input_interrupt(self, no_track : bool = False, track_uuid : str = ud.uuid4().hex[:20], blocking_output = None):
        self._interrupt = _Interrupt(self.pid, self.tag, no_track, True, track_uuid, blocking_output)
        while self._interrupt.freeze == True:
            await asyncio.sleep(0)
        data, code = self._interrupt.transfer_stdin, self._interrupt.transfer_stderr
        self._interrupt = None
        return data.data, code.data

    async def stop(self):
        await self._holder.on_stop(self)
        self._alive = False
        Process._AliveCount -= 1

    def is_alive(self):
        return self._alive

    def is_iteration(self):
        return self._in_iteration


class ProcessHolder:
### rewrite functions ###
    @staticmethod
    async def body(process : Process):
        return

    @staticmethod
    async def on_fatal(process : Process):
        return

    @staticmethod
    async def on_stop(process : Process):
        return

    @staticmethod
    async def on_start(process : Process):
        return

    @staticmethod
    def is_body_available(process : Process):
        return True

    @staticmethod
    def is_fatal(process : Process, return_value):
        return False
### rewrite functions ###


class _Interrupt:
    _AllInterrupts = _SemaphoredList([]) # static field
    def __init__(self, process_pid : int, process_tag, no_track : bool, expects_input : bool, track_uuid : str, proc_stdout = None):
        self.expects_input = expects_input
        self.proc_pid = process_pid
        self.proc_tag = process_tag
        self.no_track = no_track
        self.interrupt_uuid = track_uuid

        self.proc_stdout = _StdoutCall(proc_stdout, process_pid, self.interrupt_uuid, process_tag)
        self.transfer_stdin = None
        self.transfer_stderr = None

        _Interrupt._AllInterrupts._join()
        allInterrupts = _Interrupt._AllInterrupts._open_list()
        allInterrupts.append(self)
        _Interrupt._AllInterrupts._close_list()

        self.in_progress = False
        self.freeze = True


class _Core:
    init = False

    @staticmethod
    def _start():
        if _Core.init == True: raise Exception("Already running!")
        _Core.init = True
        eloop = asyncio.get_event_loop()
        eloop.run_until_complete(_Core._scheduler())

    @staticmethod
    async def _scheduler():
        while True:
            if Transfer._AliveCount <= 0 and Process._AliveCount <= 0: break
            _Interrupt._AllInterrupts._join()
            Transfer._AllTransfers._join()
            Process._AllProcesses._join()
            allInterrupts = _Interrupt._AllInterrupts._open_list()
            allTransfers = Transfer._AllTransfers._open_list()
            allProcesses = Process._AllProcesses._open_list()

            pop_tr_list = []
            pop_inter_list = []
            pop_proc_list = []

            start_list = []

            for i in range(0, len(allTransfers)):
                if allTransfers[i]._holder.__bases__[0] == FreeTransferHolder:
                    if allTransfers[i].is_alive() == True and allTransfers[i].is_stdout() == False:
                        start_list.append(allTransfers[i]._stdout_iteration_call)
                    if allTransfers[i].is_alive() == True and allTransfers[i].is_stdin() == False:
                        start_list.append(allTransfers[i]._stdin_iteration_call)
                else:
                    if allTransfers[i].is_alive() == True and allTransfers[i].is_stdinout() == False:
                        start_list.append(allTransfers[i]._stdinout_iteration_call)

                if allTransfers[i].is_alive() == False: pop_tr_list.append(i)

            for i in range(0, len(allProcesses)):
                if allProcesses[i].is_alive() == True and allProcesses[i].is_iteration() == False:
                    start_list.append(allProcesses[i]._body_iteration_call)
                if allProcesses[i].is_alive() == False: pop_proc_list.append(i)

            all_inputs = []
            all_errors = []

            for tr in allTransfers:
                while tr._stdin.empty() != True:
                    all_inputs.append(tr._stdin.get())
                while tr._stderr.empty() != True:
                    all_errors.append(tr._stderr.get())
                for inter in allInterrupts:
                    if inter.freeze == False: continue
                    if inter.proc_tag != tr.tag: continue
                    if inter.in_progress == True: continue
                    if inter.proc_stdout.data != None and inter.expects_input != True:
                        tr._stdout.put(inter.proc_stdout)
                    if inter.expects_input == True:
                        tr._stdreq.put(inter.proc_stdout)
                    inter.in_progress = True
            no_track_inters = []

            for i in range(0, len(allInterrupts)):
                if allInterrupts[i].freeze == False:
                    pop_inter_list.append(i)
                    continue
                if allInterrupts[i].no_track == True:
                    no_track_inters.append(allInterrupts[i])
                    continue
                for j in range(0, len(all_inputs)):
                    if all_inputs[j] == None: continue
                    if allInterrupts[i].proc_tag != all_inputs[j].tag: continue
                    if all_inputs[j].proc_pid == allInterrupts[i].proc_pid or all_inputs[j].call_uuid == allInterrupts[i].interrupt_uuid:
                        allInterrupts[i].transfer_stdin = all_inputs[j]
                        all_inputs[j] = None
                        break
                for j in range(0, len(all_errors)):
                    if all_errors[j] == None: continue
                    if allInterrupts[i].proc_tag != all_errors[j].tag: continue
                    if all_errors[j].proc_pid == allInterrupts[i].proc_pid or all_errors[j].call_uuid == allInterrupts[i].interrupt_uuid:
                        allInterrupts[i].transfer_stderr = all_errors[j]
                        all_errors[j] = None
                        break
                if allInterrupts[i].transfer_stderr != None:
                    if allInterrupts[i].expects_input == False:
                        allInterrupts[i].freeze = False
                    elif allInterrupts[i].transfer_stdin != None:
                        allInterrupts[i].freeze = False

            for nt in no_track_inters:
                for inp in all_inputs:
                    if inp != None and inp.tag == nt.proc_tag:
                        nt.transfer_stdin = inp
                        break
                for err in all_errors:
                    if err != None and err.tag == nt.proc_tag:
                        nt.transfer_stderr = err
                        break
                if nt.transfer_stdin != None and nt.transfer_stderr != None:
                    nt.freeze = False

            for i in range(0, len(pop_tr_list)):
                allTransfers.pop(pop_tr_list[i] - i)
            for i in range(0, len(pop_proc_list)):
                allProcesses.pop(pop_proc_list[i] - i)
            for i in range(0, len(pop_inter_list)):
                allInterrupts.pop(pop_inter_list[i] - i)

            _Interrupt._AllInterrupts._close_list()
            Transfer._AllTransfers._close_list()
            Process._AllProcesses._close_list()
            for task in start_list:
                asyncio.ensure_future(task())

            await asyncio.sleep(0)

        _Core.init = False

def start():
    _Core._start()

def create_process(holder : ProcessHolder, locals : object, tag : str):
    Process.create_process(holder, locals, tag)

def create_transfer(holder : Union[FreeTransferHolder, BlockingTransferHolder], locals : object, tag : str):
    Transfer.create_transfer(holder, locals, tag)
