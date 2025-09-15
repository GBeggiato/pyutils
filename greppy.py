"""
bad grep replica, needed as a cli on windows 
because at work I can't install cool linux stuff
"""

import argparse
from pathlib import Path
import re
import typing as ty


class Args(ty.NamedTuple):
    expr: re.Pattern
    path: Path
    r: bool
    i: bool
    n: bool

    @classmethod
    def from_cli(cls):
        parser = argparse.ArgumentParser(
            prog=f"python3 {Path(__file__).name}",
            description="bad grep replica",
        )
        parser.add_argument("expr", help="regex expression. not everything is supported")
        parser.add_argument("path", help="input file or directory")
        parser.add_argument("-r",   help="recursively traverse dirs",               required=False, action="store_true")
        parser.add_argument("-i",   help="insensitive case",                        required=False, action="store_true")
        parser.add_argument("-n",   help="print file and line number of the match", required=False, action="store_true")
        parsed = parser.parse_args()
        path = Path(parsed.path)
        if not path.exists():
            raise FileNotFoundError(f"Could not find {path = }")
        if parsed.i:
            expr = re.compile(parsed.expr, re.I)
        else:
            expr = re.compile(parsed.expr)
        return cls(expr, path, parsed.r, parsed.i, parsed.n)

    def do_one_file(self, f: Path):
        with f.open("r") as fp:
            try:
                for i, line in enumerate(fp, start=1):
                    line = line.strip("\n")
                    match = re.findall(self.expr, line)
                    if not match:
                        continue
                    if self.n:
                        print(f, i, line, sep=": ")
                    else:
                        print(line)
            except UnicodeDecodeError:
                print(f"UnicodeDecodeError on file: {f}")
                return

    def traverse_filesys(self, f: Path):
        if not f.exists():
            return
        if f.is_file():
            self.do_one_file(f)
        elif f.is_dir():
            for fp in f.iterdir():
                self.traverse_filesys(fp)


def main():
    args = Args.from_cli()
    if args is None:
        return
    args.traverse_filesys(args.path.absolute())


if __name__ == "__main__":
    main()

