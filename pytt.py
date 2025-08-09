from argparse import ArgumentParser
import ast
from datetime import datetime
import itertools
from pathlib import Path
from typing import Iterable


CLASS_KEY = "C"
FUNC_KEY = "F"
EMPTY = ""
INDENT = "    "
PASS = f"{INDENT*2}pass"


def parse_args() -> tuple[Path, Path] | None:
    parser = ArgumentParser(
        prog=f"python3 {Path(__file__).name}",
        description="generates the boilerplate required for unittesting your code",
    )
    parser.add_argument("input", help="input file to generate the test boilerplate for")
    parser.add_argument("-o", help="output directory location, defaults to current dir")
    parsed = parser.parse_args()

    tested = Path(parsed.input)
    if not tested.exists():
        print(f"not found: {tested}")
        return None
    if not tested.suffix == ".py":
        print(f"is not python file: {tested}")
        return None
    if parsed.o:
        out_dir = Path(parsed.o)
        if not out_dir.exists():
            print(f"not found: {out_dir}")
            return None
    else:
        out_dir = Path(__file__).parent
    return tested, out_dir


def walk_ast(node: ast.AST):
    yield node
    for child in ast.iter_child_nodes(node):
        yield from walk_ast(child)


def parse_lines(tested: Path) -> dict[str, list[str]]:
    parsed = ast.parse(tested.read_text())

    funcs = []
    classes = []
    for node in walk_ast(parsed):
        if isinstance(node, ast.FunctionDef):
            funcs.append(node.name)
        elif isinstance(node, ast.ClassDef):
            classes.append(node.name)
    return {
        FUNC_KEY: funcs,
        CLASS_KEY: classes
    }

def concat_lines(testables: dict[str, list[str]], tested: Path) -> map[str]:
    mod = tested.stem
    d = datetime.now().strftime("%Y-%m-%d %H:%M")
    parts: list[Iterable[str]] = [(
        f"# automatically generated on {d}", EMPTY,
        "import unittest", EMPTY,
        f"import {mod}",
    )]
    if testables[CLASS_KEY]:
        parts.append((
            f"from {mod} import {n}" for n in testables[CLASS_KEY]
        ))
    parts.append((
        EMPTY,
        EMPTY,
        f"class Test{mod.title()}(unittest.TestCase):",
        EMPTY,
    ))
    if testables[CLASS_KEY]:
        parts.append((
            f"{INDENT}def setUp(self):", PASS, EMPTY,
            f"{INDENT}def tearDown(self):", PASS, EMPTY,
        ))
    f_parts = itertools.chain.from_iterable(map(
        lambda fl: (f"{INDENT}def test_{fl}(self):", PASS, f"{EMPTY}"),
        testables[FUNC_KEY]
    ))
    parts.append(f_parts)
    parts.append((
        EMPTY, 'if __name__ == "__main__":', f"{INDENT}unittest.main()", EMPTY,
    ))
    return map(lambda s: f"{s}\n", itertools.chain.from_iterable(parts))


def main():
    parsed = parse_args()
    if parsed is None:
        print("could not properly parse inputs")
        return
    tested, out_dir = parsed

    testables = parse_lines(tested)

    lines = concat_lines(testables, tested)

    out = out_dir / tested.with_name(f"test_{tested.stem}.py")
    with out.open("w") as fp:
        fp.writelines(lines)
    print(f"created boilerplate for [{tested.name}] in {out}")


if __name__ == "__main__":
    main()

