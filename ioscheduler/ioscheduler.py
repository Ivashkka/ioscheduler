import enum
import asyncio
import queue
import signal
import uuid as ud
from typing import Union

class StdinClall:
    __slots__ = ['data', 'proc_uuid']
    def __init__(self, data, proc_uuid : str):
        self.data = data
        self.proc_uuid = proc_uuid

class StdoutClall:
    __slots__ = ['data', 'proc_uuid']
    def __init__(self, data, proc_uuid : str):
        self.data = data
        self.proc_uuid = proc_uuid

class StderrClall:
    __slots__ = ['data', 'proc_uuid']
    def __init__(self, data, proc_uuid : str):
        self.data = data
        self.proc_uuid = proc_uuid


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


class TransferHolder:
    ### rewrite functions ###
    @staticmethod
    async def stdout(transfer : object, stdout_call : StdoutClall):
        return StderrClall(None, None)

    @staticmethod
    async def stdin(transfer : object):
        return StdinClall(None, None), StderrClall(None, None)

    @staticmethod
    async def on_fatal(transfer : object):
        return

    @staticmethod
    async def on_stop(transfer : object):
        return

    @staticmethod
    def is_stdout_available(transfer : object):
        return True

    @staticmethod
    def is_stdin_available(transfer : object):
        return True

    @staticmethod
    def is_fatal(transfer : object, stderr_call : StderrClall):
        return False
    ### rewrite functions ###


class Transfer:
    _AllTransfers = _SemaphoredList([]) # static field
    _AliveCount = 0 # static field

    _stdout = queue.Queue()
    _stdin = queue.Queue()
    _stderr = queue.Queue()

    _alive = False
    _stdout_in_iteration = False
    _stdin_in_iteration = False
    _holder = None

    locals = None
    tag = None

    def __init__(self, holder : TransferHolder, locals : object, tag : str):
        self._holder = holder
        self.locals = locals
        self.tag = tag
        self._alive = True
        Transfer._AliveCount += 1

    @staticmethod
    def create_transfer(holder : TransferHolder, locals : object, tag : str):
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
        if self._holder.is_stdout_available(self) != True or self._stdout.empty() == True:
            self._stdout_in_iteration = False
            return

        stderr_return = await self._holder.stdout(self, self._stdout.get())
        self._stderr.put(stderr_return)

        if self._holder.is_fatal(self, stderr_return) == True:
            await self._holder.on_fatal(self)
        self._stdout_in_iteration = False

    async def _stdin_iteration_call(self):
        self._stdin_in_iteration = True
        if self._holder.is_stdin_available(self) != True:
            self._stdin_in_iteration = False
            return

        stdin_return, stderr_return = await self._holder.stdin(self)
        self._stdin.put(stdin_return)
        self._stderr.put(stderr_return)

        if self._holder.is_fatal(self, stderr_return) == True:
            await self._holder.on_fatal(self)
        self._stdin_in_iteration = False

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


class ProcessHolder:
### rewrite functions ###
    @staticmethod
    def stdin_filter(process : object, stdin_call : StdinClall):
        if process.proc_uuid == stdin_call.proc_uuid: return True
        return False

    @staticmethod
    def stderr_filter(process : object, stderr_call : StderrClall):
        if process.proc_uuid == stderr_call.proc_uuid: return True
        return False

    @staticmethod
    async def body(process : object):
        return

    @staticmethod
    async def on_fatal(process : object):
        return

    @staticmethod
    async def on_stop(process : object):
        return

    @staticmethod
    def is_body_available(process : object):
        return True

    @staticmethod
    def is_fatal(process : object, return_value):
        return False
### rewrite functions ###


class Process:
    _AllProcesses = _SemaphoredList([]) # static field
    _AliveCount = 0 # static field
    _alive = False
    _in_iteration = False

    locals = None
    tag = None
    interrupt = None
    return_value = None
    _holder = None

    def __init__(self, holder : ProcessHolder, locals : object, tag : str):
        self.proc_uuid = ud.uuid4().hex[:20]
        self.interrupt = _Interrupt(self)
        self._holder = holder
        self.locals = locals
        self.tag = tag
        self._alive = True
        Process._AliveCount += 1

    @staticmethod
    def create_process(holder : ProcessHolder, locals : object, tag : str):
        self_process = Process(holder, locals, tag)
        Process._AllProcesses._join()
        allProcesses = Process._AllProcesses._open_list()
        allProcesses.append(self_process)
        Process._AllProcesses._close_list()

    async def _body_iteration_call(self):
        self._in_iteration = True
        if self._holder.is_body_available(self) != True:
            self._in_iteration = False
            return
        return_value = await self._holder.body(self)
        self.return_value = return_value
        if self._holder.is_fatal(self, return_value):
            await self._holder.on_fatal(self)
        self._in_iteration = False

    async def generate_interrupt(self, std_expects : list, proc_output : object = None):
        await self.interrupt.generate(proc_output, std_expects)

    def _interrupt_stdin_filter(self, stdin_call : StdinClall):
        return self._holder.stdin_filter(self, stdin_call)

    def _interrupt_stderr_filter(self, stderr_call : StderrClall):
        return self._holder.stderr_filter(self, stderr_call)

    async def stop(self):
        await self._holder.on_stop(self)
        self._alive = False
        Process._AliveCount -= 1

    def is_alive(self):
        return self._alive

    def is_iteration(self):
        return self._in_iteration


class _Interrupt:
    def __init__(self, process : Process):
        self._std_expects = []
        self._process = process
        self._interrupt_in_progress = False
        self.proc_stdout = StdoutClall(None, process.proc_uuid)
        self.transfer_stdin = StdinClall(None, process.proc_uuid)
        self.transfer_stderr = StderrClall(None, process.proc_uuid)
        self.freeze = False

    async def generate(self, proc_output, std_expects: list):
        if self._interrupt_in_progress == True: raise Exception("Second interrupt while first in progress!")
        self.proc_stdout.data = proc_output
        self._std_expects = std_expects
        self.freeze = True
        while self.freeze: await asyncio.sleep(0.5)

    def try_to_satisfy(self, stdobj : Union[StderrClall, StdinClall]):
        if type(stdobj) not in self._std_expects: return
        if type(stdobj) == StdinClall:
            if self._process._interrupt_stdin_filter(stdobj) != True: return
            self.transfer_stdin = stdobj
        elif type(stdobj) == StderrClall:
            if self._process._interrupt_stderr_filter(stdobj) != True: return
            self.transfer_stderr = stdobj
        self._std_expects.remove(type(stdobj))
        if len(self._std_expects) == 0:
            self._interrupt_in_progress = False
            self.proc_stdout.data = None
            self.freeze = False


class _Core:
    init = False

    @staticmethod
    def _start():
        if _Core.init == True: raise Exception("Already running!")
        _Core.init = True
        asyncio.run(_Core._scheduler())

    @staticmethod
    async def _scheduler():
        while True:
            if Transfer._AliveCount <= 0 and Process._AliveCount <= 0: break
            Transfer._AllTransfers._join()
            Process._AllProcesses._join()
            allTransfers = Transfer._AllTransfers._open_list()
            allProcesses = Process._AllProcesses._open_list()

            pop_tr_list = []
            pop_proc_list = []

            for i in range(0, len(allTransfers)):
                if allTransfers[i].is_alive() == True and allTransfers[i].is_stdout() == False:
                    asyncio.create_task(allTransfers[i]._stdout_iteration_call())
                if allTransfers[i].is_alive() == True and allTransfers[i].is_stdin() == False:
                    asyncio.create_task(allTransfers[i]._stdin_iteration_call())
                if allTransfers[i].is_alive() == False: pop_tr_list.append(i)

            for i in range(0, len(allProcesses)):
                if allProcesses[i].is_alive() == True and allProcesses[i].is_iteration() == False:
                    asyncio.create_task(allProcesses[i]._body_iteration_call())
                if allProcesses[i].is_alive() == False: pop_proc_list.append(i)

            for tr in allTransfers:
                if len(allProcesses) == 0: break
                first_stdin = None
                first_stderr = None
                if not tr._stdin.empty(): first_stdin = tr._stdin.get()
                if not tr._stderr.empty(): first_stderr = tr._stderr.get()
                if first_stdin == None and first_stderr == None: continue
                for proc in allProcesses:
                    if proc.tag != tr.tag: continue
                    if proc.interrupt.freeze == True:
                        proc.interrupt.try_to_satisfy(first_stdin)
                        proc.interrupt.try_to_satisfy(first_stderr)

            for proc in allProcesses:
                if len(allTransfers) == 0: break
                if proc.interrupt.freeze != True: continue
                if proc.interrupt._interrupt_in_progress == True: continue
                if proc.interrupt.proc_stdout.data == None: continue
                for tr in allTransfers:
                    if tr.tag != proc.tag: continue
                    tr._stdout.put(proc.interrupt.proc_stdout)
                    proc.interrupt._interrupt_in_progress = True
                    break

            for i in pop_tr_list:
                allTransfers.pop(i)
            for i in pop_proc_list:
                allProcesses.pop(i)

            Transfer._AllTransfers._close_list()
            Process._AllProcesses._close_list()

            await asyncio.sleep(0.1)

        _Core.init = False

def start():
    _Core._start()

def create_process(holder : ProcessHolder, locals : object, tag : str):
    Process.create_process(holder, locals, tag)

def create_transfer(holder : TransferHolder, locals : object, tag : str):
    Transfer.create_transfer(holder, locals, tag)
