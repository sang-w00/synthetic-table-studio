from __future__ import annotations

from .cli import main, run_worker

__all__ = ["main", "run_worker"]

if __name__ == "__main__":
    raise SystemExit(main())
