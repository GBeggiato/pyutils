from collections import namedtuple
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
import re
from typing import Dict, NamedTuple
from xml.etree import cElementTree as ET
from zipfile import ZipFile


XML_SPACE_ATTR:str = "{http://www.w3.org/XML/1998/namespace}space"
XML_WHITESPACE:str = "\t\n \r"
U_SSML12:str = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
XL_CELL_EMPTY:int = 0
XL_CELL_TEXT:int = 1
XL_CELL_NUMBER:int = 2
XL_CELL_DATE:int = 3
XL_CELL_BOOLEAN:int = 4
XL_CELL_ERROR:int = 5
V_TAG:str = U_SSML12 + "v" # value
F_TAG:str = U_SSML12 + "f" # formula
IS_TAG:str = U_SSML12 + "is" # inline string
epoch_1904:datetime = datetime(1904, 1, 1)
epoch_1900:datetime = datetime(1899, 12, 31)
epoch_1900_minus_1:datetime = datetime(1899, 12, 30)
error_code_from_text = {
    "#DIV/0!": 7,
    "#N/A": 42,
    "#NAME?": 29,
    "#NULL!": 0,
    "#NUM!": 36,
    "#REF!": 23,
    "#VALUE!": 15
}


RE_SHEET = re.compile(r"xl/worksheets/sheet\d+.xml")
RE_INT = re.compile(r"\d+")


def identify_sheets(component_names: dict[str, str]) -> dict[int, str]:
    out = dict()
    for key in component_names.keys():
        m = RE_SHEET.match(key)
        if m:
            out[int(RE_INT.findall(key)[0])] = key
    return out


class _XLsheetReader:

    def __init__(self, filepath: Path) -> None:
        self.filepath = filepath
        self.sharedstrings = []
        self.xf_index_to_xl_type = {0: XL_CELL_NUMBER}  
        self.xf_type = None
        self.xf_counts = [0, 0]
        self.fmt_is_date = {i:1 for i in (list(range(14, 23)) + list(range(45, 48)))}
        self.tag2meth = {
            "cellStyleXfs" : self.do_cellstyle,
            "cellXfs"      : self.do_cellxfs,
            "xf"           : self.do_xf, 
            "row"          : self.do_row
        }
        for x in tuple(self.tag2meth.keys()):
            self.tag2meth[U_SSML12 + x] = self.tag2meth[x]

    def _peek_components(self) -> dict[str, str]:
        with self.filepath.open("rb") as f:
            peek = f.read(4)
        if peek != b"PK\x03\x04":
            raise TypeError("Not a good file")
        with ZipFile(self.filepath) as zf:
            component_names = dict([(name.replace('\\', '/').lower(), name) for name in zf.namelist()])
        return component_names

    def open_read(self, sheet: int) -> tuple[tuple]:
        component_names = self._peek_components()
        with ZipFile(self.filepath) as zf:
            if "xl/workbook.xml" not in component_names:
                raise TypeError("Not a good file")
            with zf.open(component_names["xl/workbook.xml"]) as zfo:
                self.process_zipfile(zfo) 
            needle = "xl/styles.xml"
            if needle in component_names:
                with zf.open(component_names[needle]) as zfo:
                    self.process_zipfile(zfo)
            needle = "xl/sharedstrings.xml"
            if needle in component_names:
                with zf.open(component_names[needle]) as zfo:
                    self.process_sharedstrings(zfo)
            to_read = identify_sheets(component_names).get(sheet)
            if to_read is None:
                raise ValueError("not a valid sheet")
            with zf.open(component_names[to_read]) as zfo: # only the first sheet
                return self.load_data(zfo)

    def sheets(self) -> dict[int, str]:
        return identify_sheets(self._peek_components())

    def process_zipfile(self, zfo):
        for elem in ET.parse(zfo).iter():
            method = self.tag2meth.get(elem.tag)
            if method:
                method(elem)

    def do_cellstyle(self, _):
        self.xf_type = 0

    def do_cellxfs(self, _):
        self.xf_type = 1

    def do_xf(self, elem):
        if self.xf_type != 1:
            return
        xfx = self.xf_counts[self.xf_type]
        self.xf_counts[self.xf_type] = xfx + 1
        numFmtId = int(elem.get("numFmtId", 0))
        is_date = self.fmt_is_date.get(numFmtId, 0)
        self.xf_index_to_xl_type[xfx] = is_date + 2

    def process_sharedstrings(self, zfo):
        si_tag = U_SSML12 + "si"
        for _, elem in ET.iterparse(zfo):
            if elem.tag == si_tag:
                self.sharedstrings.append( _get_text_from_si_or_is(elem))
                elem.clear()

    def load_data(self, zfo) -> tuple[tuple]:
        """ 
        creates data row by row and stores it into the object 
        
        Note: first element are (usually) the column names
        """
        data = []
        row_tag = U_SSML12 + "row"
        for _, elem in ET.iterparse(zfo):
            if elem.tag == row_tag:
                data.append(self.do_row(elem))
                elem.clear()
        return tuple(data)

    def do_row(self, row_elem) -> tuple:
        """ creates a single row and returns it """
        row = []
        for cell_elem in row_elem:
            xf_index = int(cell_elem.get("s", 0))
            cell_type = cell_elem.get("t", "n")
            t = None
            for child in cell_elem:
                t = self.do_child(t, child, cell_type)
            r = self.get_value(t, cell_type, xf_index)
            row.append(r)
        return tuple(row)

    def get_cell(self, ctype, value, xf_index):
        """ converts a single cell from xml info to python dtype """
        if ctype is None:
            ctype = self.xf_index_to_xl_type[xf_index]
        return _parse_cell(value, ctype)

    def get_value(self, t, cell_type, xf_index):
        """ properly extract the text from an xml cell """
        if (t) and (cell_type == "n"):
            v = float(t)
            r = self.get_cell(None, v, xf_index)
        elif (t) and (cell_type == "s"):
            v = self.sharedstrings[int(t)]
            r = self.get_cell(XL_CELL_TEXT, v, xf_index)
        elif cell_type in ("str", "inlineStr"):
            r = self.get_cell(XL_CELL_TEXT, t, xf_index)
        elif cell_type == "b":
            v = _xsd_to_boolean(t)
            r = self.get_cell(XL_CELL_BOOLEAN, v, xf_index)
        elif cell_type == "e":
            _v = "#N/A" if t is None else t
            v = error_code_from_text[_v]
            r = self.get_cell(XL_CELL_ERROR, v, xf_index)
        else:
            raise Exception("Unknown cell type") 
        return r

    def do_child(self, t, child, cell_type):
        """from child to input for dtype conversion"""
        child_tag = child.tag
        if child_tag == V_TAG:
            if cell_type in ("n", "s", "b", "e", "inlineStr"):
                t = child.text
            elif cell_type == "str":
                t = _cook_text(child)
            else:
                raise Exception("Unknown cell type")
        elif child_tag == F_TAG:
            pass
        elif (child_tag == IS_TAG) and (cell_type == "inlineStr"):
            t = _get_text_from_si_or_is(self, child)
        else:
            raise Exception("bad child tag")
        return t


def _unescape(s:str, subber=None, repl=None):
    if subber is None:
        subber = re.compile(r'_x[0-9A-Fa-f]{4,4}_', re.UNICODE).sub
    if repl is None:
        repl = lambda mobj: chr(int(mobj.group(0)[2:6], 16))
    if "_" in s:
        return subber(repl, s)
    return s


def _cook_text(elem):
    t = elem.text
    if t is None:
        return ''
    if elem.get(XML_SPACE_ATTR) != 'preserve':
        t = t.strip(XML_WHITESPACE)
    return _unescape(t)


def _parse_cell(cell_contents, cell_type):
    """converts the contents of the cell into an appropriate object"""
    if cell_type == XL_CELL_DATE:
        try:
            cell_contents = _xldate_as_datetime(float(cell_contents))
        except OverflowError:
            return cell_contents
        year = (cell_contents.timetuple())[0:3]
        if (not epoch_1904 and year == (1899, 12, 31)) or (epoch_1904 and year == (1904, 1, 1)):
            cell_contents = datetime.time(cell_contents.hour, cell_contents.minute, cell_contents.second, cell_contents.microsecond)
    elif cell_type == XL_CELL_ERROR:
        cell_contents = None
    elif cell_type == XL_CELL_EMPTY:
        cell_contents = None
    elif cell_type == XL_CELL_TEXT and cell_contents=="":
        cell_contents = None
    elif cell_type == XL_CELL_BOOLEAN:
        cell_contents = bool(cell_contents)
    elif cell_type == XL_CELL_NUMBER:
        val = int(cell_contents)
        if val == cell_contents:
            cell_contents = val
    return cell_contents


def _xldate_as_datetime(xldate:float) -> datetime:
    epoch = epoch_1900 if xldate < 60 else epoch_1900_minus_1
    days = int(xldate)
    fraction = xldate - days
    seconds = int(round(fraction*86400000.0))
    seconds, milliseconds = divmod(seconds, 1000)
    return epoch + timedelta(days, seconds, 0, milliseconds)


def _get_text_from_si_or_is(elem, r_tag=U_SSML12+'r', t_tag=U_SSML12+'t'):
    accum = []
    for child in elem:
        tag = child.tag
        if tag == t_tag:
            t = _cook_text(child)
            if t:
                accum.append(t)
        elif tag == r_tag:
            for tnode in child:
                if tnode.tag == t_tag:
                    t = _cook_text(tnode)
                    if t:
                        accum.append(t)
    return ''.join(accum)


def _xsd_to_boolean(s:str) -> int:
    if not s:
        return 0
    if s in ("1", "true", "on"):
        return 1
    if s in ("0", "false", "off"):
        return 0
    raise ValueError("unexpected xsd:boolean value: %r" % s)


def read_xlsheet(xlfilepath:Path, row_name:str="row", sheet: int = 1) -> tuple[NamedTuple, ...]:
    """
    - opens the given excel file
    - reads all data and organizes it in a single immutable structure

    Args:
        xlfilepath (Path)               : path to the excel file
        row_name   (str, default="row") : row name to be used for the namedtuple
        sheet      (int, default=1)     : sheet number from 1 to n
 
    ## Notes
    - only the first sheet is read
    - very little handling overall
    
    ## Assumptions
    - first row is header
    - NO empty cells (causes error)
    - formulas are ok
    - formula errors are substituded with 'None'
    """
    assert isinstance(xlfilepath, Path)
    assert xlfilepath.exists()
    assert xlfilepath.is_file()
    assert xlfilepath.suffix in (".xls", ".xlsx")
    assert isinstance(row_name, str)
    assert isinstance(sheet, int)
    data = _XLsheetReader(xlfilepath).open_read(sheet)
    row_factory = namedtuple(row_name, field_names=data[0])
    return tuple(row_factory(*row) for row in data[1:])


def read_all_xlsheets(xlfilepath: Path, row_name: str="row") -> dict[int, tuple[NamedTuple, ...]]:
    sheets = list(_XLsheetReader(xlfilepath).sheets().keys())
    return {s: read_xlsheet(xlfilepath, row_name, s) for s in sheets}


def xl2csv(xlfile: Path, csvfile: Path):
    """ converts the xlsheet to a csv """
    data = read_xlsheet(xlfile)
    with csvfile.open("w") as fp:
        w = csv.DictWriter(fp, data[0]._fields)
        w.writeheader()
        data = (r._asdict() for r in data)
        w.writerows(data)

