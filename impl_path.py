"""
basic extensions of the pathlib module

usually for project-level stuff, like dynamically locating an asset file
"""

from datetime import datetime
from pathlib import Path
from typing import Callable, Generator, TypeVar

# =============================================================================
# scaffolding

T = TypeVar("T")
MaybePath = Path | None
Paths = Generator[Path, None, None]


def ctime(f: Path) -> datetime:
    """the date the file was created"""
    return datetime.fromtimestamp(f.stat().st_ctime)


def maybe_gen(gen: Generator[T, None, None]) -> T | None:
    """the first thing in the generator or None"""
    try:
        return next(gen)
    except StopIteration:
        return None


def recursive_traverse(p: Path) -> Paths:
    """DFS-traverse and yield all paths (both dirs AND files)"""
    assert p.exists(), f"does not exist: {p}"
    yield p
    if p.is_file():
        return
    for f in p.iterdir():
        yield from recursive_traverse(f)

# =============================================================================
# basic search functionality

def find_by_filter(f: Callable[[Path], bool], root: Path) -> Paths:
    yield from (p for p in recursive_traverse(root) if f(p))


def find_file(name: str, root: Path) -> Paths:
    yield from find_by_filter(lambda p: p.name == name and p.is_file(), root)


def find_dir(name: str, root: Path) -> Paths:
    yield from find_by_filter(lambda p: p.name == name and p.is_dir(), root)

# =============================================================================
# short-circuited form of the above

def find_first_by_filter(f: Callable[[Path], bool], root: Path) -> MaybePath:
    return maybe_gen(find_by_filter(f, root))


def find_first_file(name: str, root: Path) -> MaybePath:
    return maybe_gen(find_file(name, root))


def find_first_dir(name: str, root: Path) -> MaybePath:
    return maybe_gen(find_dir(name, root))

# =============================================================================

def main():
    root = Path.home() / "Desktop" / "code"
    from pprint import pprint
    pprint(maybe_gen(find_file("aaa.py", root)))


if __name__ == "__main__":
    main()
