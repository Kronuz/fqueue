from __future__ import absolute_import, unicode_literals

import os
import sys

import fcntl
import errno
import signal
from contextlib import contextmanager
from posix_ipc import Semaphore, O_CREAT, BusyError

import Queue
import marshal

import logging
logger = logging.getLogger(__name__)


class TimeoutError(Exception):
    pass


@contextmanager
def _timeout(seconds):
    if seconds and seconds > 0:
        def timeout_handler(signum, frame):
            pass

        original_handler = signal.signal(signal.SIGALRM, timeout_handler)

        try:
            signal.alarm(seconds)
            yield
        finally:
            signal.alarm(0)
            signal.signal(signal.SIGALRM, original_handler)
    else:
        yield


def _acquire(fd, timeout):
    with _timeout(timeout):
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
        except IOError as e:
            if e.errno != errno.EINTR:
                raise e
            raise TimeoutError


def _release(fd):
    fcntl.flock(fd, fcntl.LOCK_UN)


@contextmanager
def lock(fd, timeout=None):
    _acquire(fd, timeout)
    try:
        yield fd
    finally:
        _release(fd)


class FileQueue(object):
    STOPPED = False

    def __init__(self, name=None, log=None):
        self.name = name
        self.logger = log or logger

        self.fnum = 0
        fname = '%s.%s' % (self.name, self.fnum)
        fnamepos = "%s.pos" % self.name
        if not os.path.exists(fnamepos):
            open(fnamepos, 'wb').close()  # touch file
        self.fpos = open(fnamepos, 'r+b')
        self.fwrite = open(fname, 'ab')
        self.fread = open(fname, 'rb')
        self.sem = Semaphore(b'%s-FileQueue.sem' % self.name.encode('ascii'), O_CREAT, initial_value=1)

    def __del__(self):
        self.fpos.close()
        self.fwrite.close()
        self.fread.close()

    def get(self, block=True, timeout=None):
        while True:
            try:
                self.sem.acquire(block and timeout or None)
            except BusyError:
                raise Queue.Empty
            try:
                with lock(self.fpos, 3) as fpos:
                    fpos.seek(0)
                    try:
                        num, offset = marshal.load(fpos)
                    except (EOFError, ValueError, TypeError):
                        num = offset = 0
                    self.fread.seek(offset)
                    try:
                        return marshal.load(self.fread)
                    except (EOFError, ValueError, TypeError):
                        pass
                    finally:
                        if len(self.fread.read(1)) != 1:
                            self.sem.release()
                        fpos.seek(0)
                        marshal.dump((self.fnum, self.fread.tell()), fpos)
                        fpos.flush()
            except TimeoutError:
                raise Queue.Empty

    def put(self, value, block=True, timeout=None):
        try:
            with lock(self.fwrite, 3) as fwrite:
                marshal.dump(value, fwrite)
                fwrite.flush()
            self.sem.release()
        except TimeoutError:
            raise Queue.Full


def main(argv):
    queue = FileQueue('/tmp/testing')
    queue.put('TEST')
    queue.get()

if __name__ == '__main__':
    main(sys.argv)
