from __future__ import absolute_import, unicode_literals, print_function

import os
import base64
import hashlib

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
    bucket_size = 1024 * 1024  # 1MB

    def __init__(self, name=None, log=None):
        self.name = name
        self.logger = log or logger

        semname = base64.urlsafe_b64encode(hashlib.md5(self.name.encode('ascii')).digest())
        self.sem = Semaphore(b'/' + semname, O_CREAT, initial_value=1)

        fnamepos = "%s.pos" % self.name
        if not os.path.exists(fnamepos):
            open(fnamepos, 'wb').close()  # touch file
        self.fpos = open(fnamepos, 'r+b')

        self.fread = None
        self.frnum = None

        self.fwrite = None
        self.fwnum = None

        with lock(self.fpos, 3) as fpos:
            fpos.seek(0)
            try:
                frnum, _ = marshal.load(fpos)
            except (EOFError, ValueError, TypeError):
                frnum = 0  # New, perhaps empty or corrupt pos file
        self._open_write(frnum)

    def _cleanup(self, fnum):
        """
        Deletes all files for the queue up to, and including, fnum.

        """
        while os.path.exists('%s.%s' % (self.name, fnum)):
            try:
                fname = '%s.%s' % (self.name, fnum)
                os.unlink(fname)
                # print('cleaned up file:', fname, file=sys.stderr)
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
        # print('new read bucket:', self.frnum, file=sys.stderr)

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
        # print('new write bucket:', self.fwnum, file=sys.stderr)

    def __del__(self):
        if self.fpos:
            self.fpos.close()
        if self.fwrite:
            self.fwrite.close()
        if self.fread:
            self.fread.close()

    def get(self, block=True, timeout=None):
        while True:
            try:
                # Try acquiring the semaphore (in case there's something to read)
                self.sem.acquire(block and timeout or None)
            except BusyError:
                raise Queue.Empty
            try:
                with lock(self.fpos, 3) as fpos:
                    fpos.seek(0)
                    try:
                        frnum, offset = marshal.load(fpos)
                    except (EOFError, ValueError, TypeError):
                        frnum = offset = 0  # New, perhaps empty or corrupt pos file
                    # print('@', (frnum, offset), file=sys.stderr)
                    self._open_read(frnum)
                    self.fread.seek(offset)
                    try:
                        value = marshal.load(self.fread)
                        offset = self.fread.tell()
                        if offset > self.bucket_size:
                            self._cleanup(frnum - 1)
                            self._open_read(frnum + 1)
                            offset = 0
                        peek = self.fread.read(1)
                        if len(peek):
                            # If there's something further in the file, release
                            # the semaphore. FIXME: There are two releases, which is wrong!
                            self.sem.release()
                        return value
                    except (EOFError, ValueError, TypeError):
                        pass  # The file could not be read, ignore
                    finally:
                        fpos.seek(0)
                        marshal.dump((self.frnum, offset), fpos)
                        fpos.flush()
            except TimeoutError:
                raise Queue.Empty

    def put(self, value, block=True, timeout=None):
        try:
            with lock(self.fwrite, 3) as fwrite:
                marshal.dump(value, fwrite)
                fwrite.flush()
                offset = fwrite.tell()
            if offset > self.bucket_size:
                self._open_write(self.fwnum + 1)
            self.sem.release()
        except TimeoutError:
            raise Queue.Full


# def main(argv):
#     queue = FileQueue('/tmp/testing')
#     queue.put('TEST')
#     queue.get()

# if __name__ == '__main__':
#     main(sys.argv)
