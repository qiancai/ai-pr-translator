"""Small, crash-safe file writing helpers for target repository updates."""

import os
import stat
import tempfile


def _replacement_mode(file_path):
    try:
        current = os.stat(file_path, follow_symlinks=False)
    except FileNotFoundError:
        return 0o644
    if stat.S_ISREG(current.st_mode):
        return stat.S_IMODE(current.st_mode)
    return 0o644


def _atomic_replace(file_path, mode, writer, **open_kwargs):
    """Write through a unique sibling file, then atomically replace the target."""
    directory = os.path.dirname(file_path) or "."
    temp_path = ""
    descriptor = None
    try:
        descriptor, temp_path = tempfile.mkstemp(
            dir=directory,
            prefix=f".{os.path.basename(file_path)}.",
            suffix=".tmp",
        )
        with os.fdopen(descriptor, mode, **open_kwargs) as output:
            descriptor = None
            writer(output)
            output.flush()
            os.fsync(output.fileno())
        os.chmod(temp_path, _replacement_mode(file_path))
        os.replace(temp_path, file_path)
        temp_path = ""
    finally:
        if descriptor is not None:
            os.close(descriptor)
        if temp_path:
            try:
                os.remove(temp_path)
            except FileNotFoundError:
                pass


def atomic_write_text(file_path, content, newline=None):
    def write(output):
        output.write(content)

    _atomic_replace(
        file_path,
        "w",
        write,
        encoding="utf-8",
        newline=newline,
    )


def atomic_write_bytes(file_path, content):
    def write(output):
        output.write(content)

    _atomic_replace(file_path, "wb", write)
