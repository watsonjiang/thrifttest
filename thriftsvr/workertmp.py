# -*- coding: utf-8 -
#
# This file is part of gunicorn released under the MIT license.
# See the NOTICE for more information.

import os
import platform
import tempfile

from thriftsvr import util

PLATFORM = platform.system()
IS_CYGWIN = PLATFORM.startswith('CYGWIN')


class WorkerTmp(object):

    def __init__(self):
        fdir = '/tmp'
        if fdir and not os.path.isdir(fdir):
            raise RuntimeError("%s doesn't exist. Can't create workertmp." % fdir)
        fd, name = tempfile.mkstemp(prefix="thriftsvr-", dir=fdir)

        # unlink the file so we don't leak tempory files
        try:
            util.unlink(name)
            self._tmp = os.fdopen(fd, 'w+b', 1)
        except:
            os.close(fd)
            raise

        self.spinner = 0

    def notify(self):
        try:
            self.spinner = (self.spinner + 1) % 2
            os.fchmod(self._tmp.fileno(), self.spinner)
        except AttributeError:
            # python < 2.6
            self._tmp.truncate(0)
            os.write(self._tmp.fileno(), b"X")

    def last_update(self):
        return os.fstat(self._tmp.fileno()).st_ctime

    def fileno(self):
        return self._tmp.fileno()

    def close(self):
        return self._tmp.close()
