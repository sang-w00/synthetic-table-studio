from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

from sts.storage.portable import fsync_directory, lock_exclusive, unlock


def test_exclusive_lock_round_trips_without_moving_the_file_position(
    tmp_path: Path,
) -> None:
    """The upload manifest is locked through the descriptor it is read from.

    Windows locks a byte range relative to the current position, so the lock
    helper seeks; if it forgot to seek back, the next append would land in the
    wrong place. The position must look untouched on either platform.
    """
    target = tmp_path / "manifest"
    target.write_bytes(b"0123456789")
    descriptor = os.open(target, os.O_RDWR)
    try:
        os.lseek(descriptor, 4, os.SEEK_SET)
        lock_exclusive(descriptor)
        assert os.lseek(descriptor, 0, os.SEEK_CUR) == 4
        assert os.read(descriptor, 2) == b"45"
        unlock(descriptor)
        assert os.lseek(descriptor, 0, os.SEEK_CUR) == 6
    finally:
        os.close(descriptor)


def test_exclusive_lock_works_on_an_empty_file(tmp_path: Path) -> None:
    """Publication locks are created empty, so the locked byte is past the end."""
    target = tmp_path / ".publication.lock"
    descriptor = os.open(target, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        lock_exclusive(descriptor)
        unlock(descriptor)
    finally:
        os.close(descriptor)


def test_fsync_directory_accepts_a_directory(tmp_path: Path) -> None:
    (tmp_path / "published").mkdir()
    fsync_directory(tmp_path / "published")


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX-only durability guarantee")
def test_fsync_directory_rejects_a_missing_directory(tmp_path: Path) -> None:
    """On POSIX the call really opens the directory, so a missing one is an error.

    Windows cannot make that guarantee at all, which is why it is a no-op there
    and why this expectation is not asserted cross-platform.
    """
    with pytest.raises(FileNotFoundError):
        fsync_directory(tmp_path / "absent")
