from argparse import ArgumentParser
import itertools
from pathlib import Path
from typing import Any, Generator


GIT = ".git"
T = "t"
F = "f"


def _rec_traverse_dirs(start: Path) -> Generator[Path | Any, Any, None]:
    for p in start.iterdir():
        if p.is_file():
            continue
        else:
            yield p
            yield from _rec_traverse_dirs(p)


def _find_git_dirs(root: Path, _abs: bool) -> Generator[str, Any, None]:
    fs = _rec_traverse_dirs(root)
    fs = (f for f in fs if GIT in f.parts)
    if _abs:
        fs = (f.resolve() for f in fs)
    fs = map(str, fs)
    fs = (f.split(GIT)[0] for f in fs)
    fs = (f for f, _ in itertools.groupby(fs))
    yield from fs


def _parse_args() -> tuple[Path, bool] | None:
    parser = ArgumentParser(
        prog=f"python3 {Path(__file__).name}",
        description="find .git directories",
    )
    parser.add_argument(
        "root_dir", 
        help="root dir from which the recursive search starts"
    )
    parser.add_argument(
        "-a", help=f"represent dirs as absolute paths. defaults to {T} for true",
        choices=(T, F), type=str, default=T
    )
    parsed = parser.parse_args()
    root = Path(parsed.root_dir)
    if not root.exists():
        print(f"not found: {root}")
        return None
    if not root.is_dir():
        print(f"is not a directory: {root}")
        return None
    return root, (parsed.a == T)


def main() -> int:
    args = _parse_args()
    if args is None:
        return 1
    fs = _find_git_dirs(args[0], args[1])
    for f in fs:
        print(f)
    return 0


if __name__ == "__main__":
    main()

