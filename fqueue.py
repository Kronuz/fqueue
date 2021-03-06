from __future__ import absolute_import, unicode_literals, print_function

import os
import time
import zlib

import fcntl
from contextlib import contextmanager
import posix_ipc
import sysv_ipc

import Queue
import marshal

import logging
logger = logging.getLogger(__name__)


# Try to use a semaphore that supports timeout natively
if sysv_ipc.SEMAPHORE_TIMEOUT_SUPPORTED or posix_ipc.SEMAPHORE_TIMEOUT_SUPPORTED:
    if sysv_ipc.SEMAPHORE_TIMEOUT_SUPPORTED:
        IPC_CREAT = sysv_ipc.IPC_CREAT
        BusyError = sysv_ipc.BusyError
        Semaphore = sysv_ipc.Semaphore
        sem_hash = hash
    else:
        IPC_CREAT = posix_ipc.O_CREAT
        BusyError = posix_ipc.BusyError
        Semaphore = posix_ipc.Semaphore
        sem_hash = lambda x: b'/%s%s' % (hash(x) & 0xffffffff, hash(x) >> 32 & 0xffffffff)

    def acquire(sem, timeout=None):
        return sem.acquire(timeout)
else:
    IPC_CREAT = sysv_ipc.IPC_CREAT
    BusyError = sysv_ipc.BusyError
    Semaphore = sysv_ipc.Semaphore
    sem_hash = hash

    def acquire(sem, timeout=None):
        start = time.time()
        block = sem.block
        if timeout is None:
            sleep = 0
            sem.block = True
        else:
            sleep = min(max(timeout / 5.0, 0.5), 2.0)
            sem.block = False
        try:
            while not timeout or time.time() - start < timeout:
                try:
                    return sem.acquire(timeout)
                except BusyError:
                    if not timeout or sem.block:
                        raise
                _sleep = min(max(0, timeout - time.time() + start), sleep)
                time.sleep(_sleep)
            raise BusyError
        finally:
            sem.block = block


@contextmanager
def flock(fd):
    fcntl.flock(fd, fcntl.LOCK_EX)
    try:
        yield fd
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)


class FileQueue(object):
    STOPPED = False

    shm_size = len(marshal.dumps((0, 0, 0)))
    bucket_size = 10 * 1024 * 1024  # 10MB
    sync_age = 500

    def __init__(self, name=None, log=None):
        self.name = name
        self.log = log or logger

        semname = self.name
        self.sem = Semaphore(sem_hash(b'%s.sem' % semname), IPC_CREAT, initial_value=1)
        self.lock = Semaphore(sem_hash(b'%s.lock' % semname), IPC_CREAT, initial_value=1)
        self.spos = sysv_ipc.SharedMemory(hash(b'%s.spos' % semname), sysv_ipc.IPC_CREAT, size=self.shm_size)

        fnamepos = '%s.pos' % self.name
        if not os.path.exists(fnamepos):
            empty = marshal.dumps((0, 0, 0))
            self.spos.write(empty)  # clean memory
            with open(fnamepos, 'wb') as f:  # touch file
                f.write(empty)

        self.fpos = open(fnamepos, 'r+b')

        self.fread = None
        self.frnum = None

        self.fwrite = None
        self.fwnum = None

        frnum, _ = self._update_pos()
        self._open_write(frnum)

    def _update_pos(self, fnum=None, offset=None):
        with flock(self.fpos) as fpos:
            fpos.seek(0)
            try:
                _fnum, _offset = marshal.load(fpos)
            except (EOFError, ValueError, TypeError):
                _fnum, _offset = 0, 0  # New, perhaps empty or corrupt pos file
            finally:
                if fnum is not None and offset is not None:
                    fpos.seek(0)
                    marshal.dump((fnum, offset), fpos)
                    fpos.flush()
                    os.fsync(fpos.fileno())
        return _fnum, _offset

    def _cleanup(self, fnum):
        """
        Deletes all files for the queue up to, and including, fnum.

        """
        while os.path.exists('%s.%s' % (self.name, fnum)):
            try:
                fname = '%s.%s' % (self.name, fnum)
                os.unlink(fname)
                # self.log.debug("Cleaned up file: %s", fname)
            except:
                pass
            fnum -= 1

    def _open_read(self, frnum):
        if self.frnum == frnum:
            return
        if self.fread:
            self.fread.close()
        fname = '%s.%s' % (self.name, frnum)
        if not os.path.exists(fname):
            open(fname, 'wb').close()  # touch file
        self.fread = open(fname, 'rb')
        self.frnum = frnum
        # self.log.debug("New read bucket: %s", self.frnum)

    def _open_write(self, fwnum):
        _fwnum = fwnum
        while os.path.exists('%s.%s' % (self.name, _fwnum)):
            fwnum = _fwnum
            _fwnum += 1
        if self.fwnum == fwnum:
            return
        if self.fwrite:
            self.fwrite.close()
        self.fwrite = open('%s.%s' % (self.name, fwnum), 'ab')
        self.fwnum = fwnum
        # self.log.debug("New write bucket: %s", self.fwnum)

    def __del__(self):
        self.fpos.close()
        if self.fwrite:
            self.fwrite.close()
        if self.fread:
            self.fread.close()

    def get(self, block=True, timeout=None):
        while True:
            try:
                # Try acquiring the semaphore (in case there's something to read)
                acquire(self.sem, block and timeout or 0)
            except BusyError:
                raise Queue.Empty
            try:
                acquire(self.lock, 5)
            except BusyError:
                if self.STOPPED:
                    raise Queue.Empty
                raise
            try:
                # Get nest file/offset (from shared memory):
                try:
                    frnum, offset, age = marshal.loads(self.spos.read())
                except (EOFError, ValueError, TypeError):
                    # New, perhaps empty or corrupt pos file
                    frnum, offset = self._update_pos()
                    age = 0
                # self.log.debug('%s @ %s' % (self.name, repr((frnum, offset, age))))

                # Open proper queue file for reading (if it isn't open yet):
                self._open_read(frnum)
                self.fread.seek(offset)

                # Read from the queue
                try:
                    crc32, value = marshal.load(self.fread)
                    if crc32 != zlib.crc32(value):
                        raise ValueError
                except EOFError:
                    pass  # The file could not be read, ignore
                except (ValueError, TypeError):
                    # New, perhaps empty or corrupt pos file (since the position offset has an invalid marshal)
                    frnum, offset = self._update_pos()
                    age = 0
                else:
                    offset = self.fread.tell()
                    if offset > self.bucket_size:
                        self._cleanup(frnum - 1)
                        self._open_read(frnum + 1)
                        age = self.sync_age  # Force updateing position
                        offset = 0
                    peek = self.fread.read(1)
                    if len(peek):
                        # If there's something more to read in the file,
                        # release (or re-release) the semaphore.
                        self.sem.release()
                    break
                finally:
                    # Update position
                    if age >= self.sync_age:
                        self._update_pos(self.frnum, offset)
                        age = 0
                    self.spos.write(marshal.dumps((self.frnum, offset, age + 1)))

            finally:
                self.lock.release()

        return value and marshal.loads(zlib.decompress(value))

    def put(self, value, block=True, timeout=None):
        value = zlib.compress(marshal.dumps(value))
        crc32 = zlib.crc32(value)
        with flock(self.fwrite) as fwrite:
            marshal.dump((crc32, value), fwrite)
            fwrite.flush()
            os.fsync(fwrite.fileno())
            offset = fwrite.tell()
        self.sem.release()
        if offset > self.bucket_size:
            # Switch to a new queue file:
            self._open_write(self.fwnum + 1)


# def main(argv):
#     queue = FileQueue('/tmp/testing')
#     queue.put('TEST')
#     queue.get()

# if __name__ == '__main__':
#     main(sys.argv)
