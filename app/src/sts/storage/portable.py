"""The places where POSIX and Windows differ, kept in one module.

Exclusive file locks and directory fsync are the two primitives this codebase
uses that have no single portable spelling. Collecting them here keeps every
call site platform-agnostic and gives the differences one place to be explained.

Locking
-------

The publish path, the CSV publication lock and the upload manifest all rely on an
exclusive lock held for the length of a short critical section. POSIX has
``fcntl.flock`` and Windows has ``msvcrt.locking``; neither module imports on the
other platform, so the choice is made once here rather than at every call site.

The two primitives differ in ways that matter for how they are used:

* ``flock`` locks the whole file whatever the file position is. ``msvcrt`` locks
  a byte range starting at the current position, so the Windows branch locks a
  single byte at offset zero — the conventional stand-in for a whole-file lock,
  and sufficient here because every holder agrees on that same byte. It restores
  the previous file position afterwards: one caller locks the descriptor of a
  manifest stream it is about to read and append to, and moving that stream's
  position would corrupt the manifest.
* ``msvcrt`` locks are mandatory rather than advisory, so other processes are
  refused access to the locked byte instead of merely being told about it. That
  is stricter than POSIX and harms no caller here, because the locked byte is
  only ever touched through the descriptor holding the lock.
* ``LK_LOCK`` retries for roughly ten seconds and then raises ``OSError`` rather
  than blocking indefinitely. A caller that would have waited longer on POSIX
  therefore fails on Windows; every critical section guarded here is a rename or
  a small write, so passing that bound means something is wrong, not merely slow.

Directory fsync
---------------

Publishing a file means writing a temporary, fsyncing it, renaming it into place
and then fsyncing the containing directory so the name itself survives a crash.
That last step has no Windows equivalent: a directory cannot be opened for
reading, so there is no handle to flush, and the rename's metadata is committed
on the filesystem's own schedule. On Windows the call is therefore a no-op and
the durability guarantee is weaker than on POSIX — a crash in the window after a
rename can lose the published name even though the file's contents were flushed.
README states this as a platform difference rather than leaving it implied.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

if sys.platform == "win32":  # pragma: no cover - exercised on Windows only
    import msvcrt

    _LOCK_BYTES = 1

    def _locking(descriptor: int, mode: int) -> None:
        position = os.lseek(descriptor, 0, os.SEEK_CUR)
        os.lseek(descriptor, 0, os.SEEK_SET)
        try:
            msvcrt.locking(descriptor, mode, _LOCK_BYTES)
        finally:
            os.lseek(descriptor, position, os.SEEK_SET)

    def lock_exclusive(descriptor: int) -> None:
        _locking(descriptor, msvcrt.LK_LOCK)

    def unlock(descriptor: int) -> None:
        _locking(descriptor, msvcrt.LK_UNLCK)

    def fsync_directory(directory: Path) -> None:
        return None

else:
    import fcntl

    def lock_exclusive(descriptor: int) -> None:
        fcntl.flock(descriptor, fcntl.LOCK_EX)

    def unlock(descriptor: int) -> None:
        fcntl.flock(descriptor, fcntl.LOCK_UN)

    def fsync_directory(directory: Path) -> None:
        descriptor = os.open(directory, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


__all__ = ["fsync_directory", "lock_exclusive", "unlock"]
