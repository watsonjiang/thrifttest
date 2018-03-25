# -*- coding: utf-8 -
#

from __future__ import print_function

import fcntl
import io
import os
import pwd
import random
import socket
import sys
import time
import traceback
import errno
import warnings
import logging
import re

from thriftsvr.errors import AppImportError
from thriftsvr.app import ThriftApplication

REDIRECT_TO = getattr(os, 'devnull', '/dev/null')

try:
    from setproctitle import setproctitle

    def _setproctitle(title):
        setproctitle("thriftsvr: %s" % title)
except ImportError:
    def _setproctitle(title):
        return


try:
    from importlib import import_module
except ImportError:
    def _resolve_name(name, package, level):
        """Return the absolute name of the module to be imported."""
        if not hasattr(package, 'rindex'):
            raise ValueError("'package' not set to a string")
        dot = len(package)
        for _ in range(level, 1, -1):
            try:
                dot = package.rindex('.', 0, dot)
            except ValueError:
                msg = "attempted relative import beyond top-level package"
                raise ValueError(msg)
        return "%s.%s" % (package[:dot], name)

    def import_module(name, package=None):
        """Import a module.

           The 'package' argument is required when performing a relative import. It
           specifies the package to use as the anchor point from which to resolve the
           relative import to an absolute import.

        """
        if name.startswith('.'):
            if not package:
                raise TypeError("relative imports require the 'package' argument")
            level = 0
            for character in name:
                if character != '.':
                    break
                level += 1
            name = _resolve_name(name[level:], package, level)
        __import__(name)
        return sys.modules[name]


def get_username(uid):
    """ get the username for a user id"""
    return pwd.getpwuid(uid).pw_name


def set_owner_process(uid, gid, initgroups=False):
    """ set user and group of workers processes """

    if gid:
        if uid:
            try:
                username = get_username(uid)
            except KeyError:
                initgroups = False

        # versions of python < 2.6.2 don't manage unsigned int for
        # groups like on osx or fedora
        gid = abs(gid) & 0x7FFFFFFF

        if initgroups:
            os.initgroups(username, gid)
        elif gid != os.getgid():
            os.setgid(gid)

    if uid:
        os.setuid(uid)


def chown(path, uid, gid):
    gid = abs(gid) & 0x7FFFFFFF  # see note above.
    os.chown(path, uid, gid)


if sys.platform.startswith("win"):
    def _waitfor(func, pathname, waitall=False):
        # Peform the operation
        func(pathname)
        # Now setup the wait loop
        if waitall:
            dirname = pathname
        else:
            dirname, name = os.path.split(pathname)
            dirname = dirname or '.'
        # Check for `pathname` to be removed from the filesystem.
        # The exponential backoff of the timeout amounts to a total
        # of ~1 second after which the deletion is probably an error
        # anyway.
        # Testing on a i7@4.3GHz shows that usually only 1 iteration is
        # required when contention occurs.
        timeout = 0.001
        while timeout < 1.0:
            # Note we are only testing for the existance of the file(s) in
            # the contents of the directory regardless of any security or
            # access rights.  If we have made it this far, we have sufficient
            # permissions to do that much using Python's equivalent of the
            # Windows API FindFirstFile.
            # Other Windows APIs can fail or give incorrect results when
            # dealing with files that are pending deletion.
            L = os.listdir(dirname)
            if not L if waitall else name in L:
                return
            # Increase the timeout and try again
            time.sleep(timeout)
            timeout *= 2
        warnings.warn('tests may fail, delete still pending for ' + pathname,
                      RuntimeWarning, stacklevel=4)

    def _unlink(filename):
        _waitfor(os.unlink, filename)
else:
    _unlink = os.unlink


def unlink(filename):
    try:
        _unlink(filename)
    except OSError as error:
        # The filename need not exist.
        if error.errno not in (errno.ENOENT, errno.ENOTDIR):
            raise


def is_ipv6(addr):
    try:
        socket.inet_pton(socket.AF_INET6, addr)
    except socket.error:  # not a valid address
        return False
    except ValueError:  # ipv6 not supported on this platform
        return False
    return True


def parse_address(netloc, default_port=8000):
    if re.match(r'unix:(//)?', netloc):
        return re.split(r'unix:(//)?', netloc)[-1]

    if netloc.startswith("tcp://"):
        netloc = netloc.split("tcp://")[1]

    # get host
    if '[' in netloc and ']' in netloc:
        host = netloc.split(']')[0][1:].lower()
    elif ':' in netloc:
        host = netloc.split(':')[0].lower()
    elif netloc == "":
        host = "0.0.0.0"
    else:
        host = netloc.lower()

    #get port
    netloc = netloc.split(']')[-1]
    if ":" in netloc:
        port = netloc.split(':', 1)[1]
        if not port.isdigit():
            raise RuntimeError("%r is not a valid port number." % port)
        port = int(port)
    else:
        port = default_port
    return (host, port)


def close_on_exec(fd):
    flags = fcntl.fcntl(fd, fcntl.F_GETFD)
    flags |= fcntl.FD_CLOEXEC
    fcntl.fcntl(fd, fcntl.F_SETFD, flags)


def set_non_blocking(fd):
    flags = fcntl.fcntl(fd, fcntl.F_GETFL) | os.O_NONBLOCK
    fcntl.fcntl(fd, fcntl.F_SETFL, flags)


def close(sock):
    try:
        sock.close()
    except socket.error:
        pass

try:
    from os import closerange
except ImportError:
    def closerange(fd_low, fd_high):
        # Iterate through and close all file descriptors.
        for fd in range(fd_low, fd_high):
            try:
                os.close(fd)
            except OSError:  # ERROR, fd wasn't open to begin with (ignored)
                pass


def write(sock, data, chunked=False):
    if chunked:
        return write_chunk(sock, data)
    sock.sendall(data)


def write_nonblock(sock, data, chunked=False):
    timeout = sock.gettimeout()
    if timeout != 0.0:
        try:
            sock.setblocking(0)
            return write(sock, data, chunked)
        finally:
            sock.setblocking(1)
    else:
        return write(sock, data, chunked)

def import_app(module):
    parts = module.split(":", 1)
    if len(parts) == 1:
        module, obj = module, "application"
    else:
        module, obj = parts[0], parts[1]

    try:
        __import__(module)
    except ImportError:
        if module.endswith(".py") and os.path.exists(module):
            msg = "Failed to find application, did you mean '%s:%s'?"
            raise ImportError(msg % (module.rsplit(".", 1)[0], obj))
        else:
            raise

    mod = sys.modules[module]

    try:
        app = eval(obj, vars(mod))
    except NameError:
        raise AppImportError("Failed to find application object %r in %r" % (obj, module))

    if app is None:
        raise AppImportError("Failed to find application object: %r" % obj)

    if not isinstance(app, ThriftApplication):
        raise AppImportError("application should be instance of ThriftApplication")

    return app

def getcwd():
    # get current path, try to use PWD env first
    try:
        a = os.stat(os.environ['PWD'])
        b = os.stat(os.getcwd())
        if a.st_ino == b.st_ino and a.st_dev == b.st_dev:
            cwd = os.environ['PWD']
        else:
            cwd = os.getcwd()
    except:
        cwd = os.getcwd()
    return cwd

def daemonize(enable_stdio_inheritance=False):
    """\
    Standard daemonization of a process.
    http://www.svbug.com/documentation/comp.unix.programmer-FAQ/faq_2.html#SEC16
    """
    if 'GUNICORN_FD' not in os.environ:
        if os.fork():
            os._exit(0)
        os.setsid()

        if os.fork():
            os._exit(0)

        os.umask(0o22)

        # In both the following any file descriptors above stdin
        # stdout and stderr are left untouched. The inheritence
        # option simply allows one to have output go to a file
        # specified by way of shell redirection when not wanting
        # to use --error-log option.

        if not enable_stdio_inheritance:
            # Remap all of stdin, stdout and stderr on to
            # /dev/null. The expectation is that users have
            # specified the --error-log option.

            closerange(0, 3)

            fd_null = os.open(REDIRECT_TO, os.O_RDWR)

            if fd_null != 0:
                os.dup2(fd_null, 0)

            os.dup2(fd_null, 1)
            os.dup2(fd_null, 2)

        else:
            fd_null = os.open(REDIRECT_TO, os.O_RDWR)

            # Always redirect stdin to /dev/null as we would
            # never expect to need to read interactive input.

            if fd_null != 0:
                os.close(0)
                os.dup2(fd_null, 0)

            # If stdout and stderr are still connected to
            # their original file descriptors we check to see
            # if they are associated with terminal devices.
            # When they are we map them to /dev/null so that
            # are still detached from any controlling terminal
            # properly. If not we preserve them as they are.
            #
            # If stdin and stdout were not hooked up to the
            # original file descriptors, then all bets are
            # off and all we can really do is leave them as
            # they were.
            #
            # This will allow 'gunicorn ... > output.log 2>&1'
            # to work with stdout/stderr going to the file
            # as expected.
            #
            # Note that if using --error-log option, the log
            # file specified through shell redirection will
            # only be used up until the log file specified
            # by the option takes over. As it replaces stdout
            # and stderr at the file descriptor level, then
            # anything using stdout or stderr, including having
            # cached a reference to them, will still work.

            def redirect(stream, fd_expect):
                try:
                    fd = stream.fileno()
                    if fd == fd_expect and stream.isatty():
                        os.close(fd)
                        os.dup2(fd_null, fd)
                except AttributeError:
                    pass

            redirect(sys.stdout, 1)
            redirect(sys.stderr, 2)


def seed():
    try:
        random.seed(os.urandom(64))
    except NotImplementedError:
        random.seed('%s.%s' % (time.time(), os.getpid()))


def check_is_writeable(path):
    try:
        f = open(path, 'a')
    except IOError as e:
        raise RuntimeError("Error: '%s' isn't writable [%r]" % (path, e))
    f.close()

def has_fileno(obj):
    if not hasattr(obj, "fileno"):
        return False

    # check BytesIO case and maybe others
    try:
        obj.fileno()
    except (AttributeError, IOError, io.UnsupportedOperation):
        return False

    return True


def warn(msg):
    print("!!!", file=sys.stderr)

    lines = msg.splitlines()
    for i, line in enumerate(lines):
        if i == 0:
            line = "WARNING: %s" % line
        print("!!! %s" % line, file=sys.stderr)

    print("!!!\n", file=sys.stderr)
    sys.stderr.flush()

