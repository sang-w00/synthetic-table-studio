"""Deterministic OWPML (HWPX) writer implemented with the standard library only.

An HWPX package is a ZIP container of XML parts defined by KS X 6101 (OWPML).
This module writes the smallest well-formed document that the Hangul word
processor opens: one section, a fixed reference table (fonts, borders,
character and paragraph properties), and a small block vocabulary of titles,
headings, body paragraphs, bullets, captions and bordered tables.

No third-party dependency is introduced, so the application's locked supply
chain is unchanged. Output is byte-for-byte deterministic for identical input:
ZIP entries use a fixed timestamp and a fixed order, so the published artifact
hash is reproducible like every other artifact in the workspace.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass
from io import BytesIO
from typing import Literal
from zipfile import ZIP_DEFLATED, ZIP_STORED, ZipFile, ZipInfo

# HWPUNIT is 1/7200 inch. A4 portrait with 30 mm side and 20 mm vertical margins.
PAGE_WIDTH = 59528
PAGE_HEIGHT = 84188
MARGIN_LEFT = 8504
MARGIN_RIGHT = 8504
MARGIN_TOP = 5669
MARGIN_BOTTOM = 4252
MARGIN_HEADER = 4252
MARGIN_FOOTER = 4252
TEXT_WIDTH = PAGE_WIDTH - MARGIN_LEFT - MARGIN_RIGHT

_FIXED_TIMESTAMP = (1980, 1, 1, 0, 0, 0)
_ILLEGAL_XML = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\ufffe\uffff]")
_SURROGATE = re.compile(r"[\ud800-\udfff]")

ParagraphStyle = Literal["title", "subtitle", "heading", "body", "bullet", "caption"]

# style name -> (paraPr id, charPr id)
_PARAGRAPH_STYLES: dict[str, tuple[int, int]] = {
    "title": (1, 1),
    "subtitle": (3, 3),
    "heading": (2, 2),
    "body": (0, 0),
    "bullet": (5, 0),
    "caption": (4, 4),
}
_TABLE_HEADER_STYLE = (7, 5)
_TABLE_CELL_STYLE = (6, 6)


def _sanitize(value: object) -> str:
    text = str(value).replace("\r\n", "\n").replace("\r", "\n")
    text = _SURROGATE.sub("", text)
    return _ILLEGAL_XML.sub("", text)


def _escape(value: object) -> str:
    return (
        _sanitize(value)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


@dataclass(frozen=True, slots=True)
class Paragraph:
    """One block of running text rendered with a named paragraph style."""

    text: str
    style: ParagraphStyle = "body"


@dataclass(frozen=True, slots=True)
class Table:
    """A bordered table with a shaded header row.

    Column widths are relative weights; they are scaled to the text column so a
    caller never has to know HWPUNIT.
    """

    headers: Sequence[str]
    rows: Sequence[Sequence[str]]
    weights: Sequence[int] | None = None

    def __post_init__(self) -> None:
        if not self.headers:
            raise ValueError("a table needs at least one column")
        width = len(self.headers)
        for row in self.rows:
            if len(row) != width:
                raise ValueError("every table row must match the header width")
        if self.weights is not None and len(self.weights) != width:
            raise ValueError("column weights must match the header width")
        if self.weights is not None and any(weight <= 0 for weight in self.weights):
            raise ValueError("column weights must be positive")


Block = Paragraph | Table


def _column_widths(table: Table) -> list[int]:
    count = len(table.headers)
    weights = list(table.weights) if table.weights is not None else [1] * count
    total = sum(weights)
    widths = [max(1, TEXT_WIDTH * weight // total) for weight in weights]
    widths[-1] = TEXT_WIDTH - sum(widths[:-1])
    return widths


_XML_DECLARATION = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n'
_LANGUAGES = ("hangul", "latin", "hanja", "japanese", "other", "symbol", "user")
_FONT_LANGUAGES = ("HANGUL", "LATIN", "HANJA", "JAPANESE", "OTHER", "SYMBOL", "USER")
_FONT_FACES = ("함초롬바탕", "함초롬돋움")

# (charPr id, height in 1/100 pt, font index, bold, text color)
_CHAR_PROPERTIES: tuple[tuple[int, int, int, bool, str], ...] = (
    (0, 1000, 0, False, "#000000"),
    (1, 1900, 1, True, "#000000"),
    (2, 1300, 1, True, "#000000"),
    (3, 1100, 1, False, "#3C4A3F"),
    (4, 900, 0, False, "#595959"),
    (5, 950, 1, True, "#000000"),
    (6, 950, 0, False, "#000000"),
)

# (paraPr id, alignment, line spacing %, left margin, indent, space before, space after, keep)
_PARAGRAPH_PROPERTIES: tuple[tuple[int, str, int, int, int, int, int, int], ...] = (
    (0, "JUSTIFY", 165, 0, 0, 0, 300, 0),
    (1, "CENTER", 150, 0, 0, 0, 500, 1),
    (2, "LEFT", 150, 0, 0, 800, 250, 1),
    (3, "CENTER", 150, 0, 0, 0, 900, 0),
    (4, "LEFT", 140, 0, 0, 100, 350, 0),
    (5, "LEFT", 160, 900, -450, 0, 120, 0),
    (6, "LEFT", 135, 0, 0, 0, 0, 0),
    (7, "CENTER", 135, 0, 0, 0, 0, 0),
)

_BORDER_FILL_PLAIN = 1
_BORDER_FILL_CELL = 2
_BORDER_FILL_HEADER = 3


def _repeat(attribute_value: object) -> str:
    return " ".join(f'{name}="{attribute_value}"' for name in _LANGUAGES)


def _font_reference(index: int) -> str:
    return f"<hh:fontRef {_repeat(index)}/>"


def _border(name: str, style: str) -> str:
    return f'<hh:{name} type="{style}" width="0.12 mm" color="#000000"/>'


def _border_fill(identifier: int, style: str, face_color: str | None) -> str:
    fill = (
        f'<hc:fillBrush><hc:winBrush faceColor="{face_color}" '
        'hatchColor="#999999" alpha="0"/></hc:fillBrush>'
        if face_color is not None
        else ""
    )
    return (
        f'<hh:borderFill id="{identifier}" threeD="0" shadow="0" centerLine="NONE" '
        'breakCellSeparateLine="0">'
        '<hh:slash type="NONE" Crooked="0" isCounter="0"/>'
        '<hh:backSlash type="NONE" Crooked="0" isCounter="0"/>'
        f"{_border('leftBorder', style)}{_border('rightBorder', style)}"
        f"{_border('topBorder', style)}{_border('bottomBorder', style)}"
        '<hh:diagonal type="SOLID" width="0.12 mm" color="#000000"/>'
        f"{fill}</hh:borderFill>"
    )


def _character_property(entry: tuple[int, int, int, bool, str]) -> str:
    identifier, height, font, bold, color = entry
    return (
        f'<hh:charPr id="{identifier}" height="{height}" textColor="{color}" '
        'shadeColor="none" useFontSpace="0" useKerning="0" symMark="NONE" '
        f'borderFillIDRef="{_BORDER_FILL_PLAIN}">'
        f"{_font_reference(font)}"
        f"<hh:ratio {_repeat(100)}/>"
        f"<hh:spacing {_repeat(0)}/>"
        f"<hh:relSz {_repeat(100)}/>"
        f"<hh:offset {_repeat(0)}/>"
        f"{'<hh:bold/>' if bold else ''}"
        '<hh:underline type="NONE" shape="SOLID" color="#000000"/>'
        '<hh:strikeout shape="NONE" color="#000000"/>'
        '<hh:outline type="NONE"/>'
        '<hh:shadow type="NONE" color="#B2B2B2" offsetX="10" offsetY="10"/>'
        "</hh:charPr>"
    )


def _paragraph_property(entry: tuple[int, str, int, int, int, int, int, int]) -> str:
    identifier, align, spacing, left, indent, before, after, keep = entry
    return (
        f'<hh:paraPr id="{identifier}" tabPrIDRef="0" condense="0" fontLineHeight="0" '
        'snapToGrid="1" suppressLineNumbers="0" checked="0">'
        f'<hh:align horizontal="{align}" vertical="BASELINE"/>'
        '<hh:heading type="NONE" idRef="0" level="0"/>'
        '<hh:breakSetting breakLatinWord="KEEP_WORD" breakNonLatinWord="KEEP_WORD" '
        f'widowOrphan="0" keepWithNext="{keep}" keepLines="0" pageBreakBefore="0" '
        'lineWrap="BREAK"/>'
        '<hh:autoSpacing eAsianEng="0" eAsianNum="0"/>'
        "<hh:margin>"
        f'<hc:intent value="{indent}" unit="HWPUNIT"/>'
        f'<hc:left value="{left}" unit="HWPUNIT"/>'
        '<hc:right value="0" unit="HWPUNIT"/>'
        f'<hc:prev value="{before}" unit="HWPUNIT"/>'
        f'<hc:next value="{after}" unit="HWPUNIT"/>'
        "</hh:margin>"
        f'<hh:lineSpacing type="PERCENT" value="{spacing}" unit="HWPUNIT"/>'
        f'<hh:border borderFillIDRef="{_BORDER_FILL_PLAIN}" offsetLeft="0" offsetRight="0" '
        'offsetTop="0" offsetBottom="0" connect="0" ignoreMargin="0"/>'
        "</hh:paraPr>"
    )


def _numbering() -> str:
    heads = "".join(
        f'<hh:paraHead start="1" level="{level}" align="LEFT" useInstWidth="1" '
        'autoIndent="1" widthAdjust="0" textOffsetType="PERCENT" textOffset="50" '
        f'numFormat="DIGIT" charPrIDRef="4294967295" checkable="0">^{level}.</hh:paraHead>'
        for level in range(1, 8)
    )
    return f'<hh:numbering id="1" start="1">{heads}</hh:numbering>'


def _header_xml() -> str:
    fonts = "".join(
        f'<hh:fontface lang="{language}" fontCnt="{len(_FONT_FACES)}">'
        + "".join(
            f'<hh:font id="{index}" face="{face}" type="TTF" isEmbedded="0"/>'
            for index, face in enumerate(_FONT_FACES)
        )
        + "</hh:fontface>"
        for language in _FONT_LANGUAGES
    )
    border_fills = (
        _border_fill(_BORDER_FILL_PLAIN, "NONE", None)
        + _border_fill(_BORDER_FILL_CELL, "SOLID", None)
        + _border_fill(_BORDER_FILL_HEADER, "SOLID", "#E7EDE7")
    )
    characters = "".join(_character_property(entry) for entry in _CHAR_PROPERTIES)
    paragraphs = "".join(_paragraph_property(entry) for entry in _PARAGRAPH_PROPERTIES)
    return (
        _XML_DECLARATION
        + f'<hh:head xmlns:hh="{_NS_HEAD}" xmlns:hp="{_NS_PARAGRAPH}" xmlns:hc="{_NS_CORE}" '
        'version="1.4" secCnt="1">'
        '<hh:beginNum page="1" footnote="1" endnote="1" pic="1" tbl="1" equation="1"/>'
        "<hh:refList>"
        f'<hh:fontfaces itemCnt="{len(_FONT_LANGUAGES)}">{fonts}</hh:fontfaces>'
        f'<hh:borderFills itemCnt="3">{border_fills}</hh:borderFills>'
        f'<hh:charProperties itemCnt="{len(_CHAR_PROPERTIES)}">{characters}</hh:charProperties>'
        '<hh:tabProperties itemCnt="1">'
        '<hh:tabPr id="0" autoTabLeft="0" autoTabRight="0"/>'
        "</hh:tabProperties>"
        f'<hh:numberings itemCnt="1">{_numbering()}</hh:numberings>'
        f'<hh:paraProperties itemCnt="{len(_PARAGRAPH_PROPERTIES)}">'
        f"{paragraphs}</hh:paraProperties>"
        '<hh:styles itemCnt="1">'
        '<hh:style id="0" type="PARA" name="바탕글" engName="Normal" paraPrIDRef="0" '
        'charPrIDRef="0" nextStyleIDRef="0" langID="1042" lockForm="0"/>'
        "</hh:styles>"
        "</hh:refList>"
        '<hh:compatibleDocument targetProgram="HWP201X">'
        "<hh:layoutCompatibility/>"
        "</hh:compatibleDocument>"
        "</hh:head>"
    )


_NS_HEAD = "http://www.hancom.co.kr/hwpml/2011/head"
_NS_PARAGRAPH = "http://www.hancom.co.kr/hwpml/2011/paragraph"
_NS_SECTION = "http://www.hancom.co.kr/hwpml/2011/section"
_NS_CORE = "http://www.hancom.co.kr/hwpml/2011/core"
_NS_APP = "http://www.hancom.co.kr/hwpml/2011/app"
_NS_VERSION = "http://www.hancom.co.kr/hwpml/2011/version"
_NS_OCF = "urn:oasis:names:tc:opendocument:xmlns:container"
_NS_ODF_MANIFEST = "urn:oasis:names:tc:opendocument:xmlns:manifest:1.0"
_NS_OPF = "http://www.idpf.org/2007/opf/"
_NS_HPF = "http://www.hancom.co.kr/schema/2011/hpf"

_SECTION_PROPERTIES = (
    '<hp:secPr id="" textDirection="HORIZONTAL" spaceColumns="1134" tabStop="8000" '
    'tabStopVal="4000" tabStopUnit="HWPUNIT" outlineShapeIDRef="1" memoShapeIDRef="0" '
    'textVerticalWidthHead="0" masterPageCnt="0">'
    '<hp:grid lineGrid="0" charGrid="0" wonggojiFormat="0" strtnum="0"/>'
    '<hp:startNum pageStartsOn="BOTH" page="0" pic="0" tbl="0" equation="0"/>'
    '<hp:visibility hideFirstHeader="0" hideFirstFooter="0" hideFirstMasterPage="0" '
    'border="SHOW_ALL" fill="SHOW_ALL" hideFirstPageNum="0" hideFirstEmptyLine="0" '
    'showLineNumber="0"/>'
    '<hp:lineNumberShape restartType="0" countBy="0" distance="0" startNumber="0"/>'
    f'<hp:pagePr landscape="WIDELY" width="{PAGE_WIDTH}" height="{PAGE_HEIGHT}" '
    'gutterType="LEFT_ONLY">'
    f'<hp:margin header="{MARGIN_HEADER}" footer="{MARGIN_FOOTER}" gutter="0" '
    f'left="{MARGIN_LEFT}" right="{MARGIN_RIGHT}" top="{MARGIN_TOP}" bottom="{MARGIN_BOTTOM}"/>'
    "</hp:pagePr>"
    "<hp:footNotePr>"
    '<hp:autoNumFormat type="DIGIT" userChar="" prefixChar="" suffixChar=")" supscript="0"/>'
    '<hp:noteLine length="-1" type="SOLID" width="0.12 mm" color="#000000"/>'
    '<hp:noteSpacing betweenNotes="850" belowLine="567" aboveLine="850"/>'
    '<hp:numbering type="CONTINUOUS" newNum="1"/>'
    '<hp:placement place="EACH_COLUMN" beneathText="0"/>'
    "</hp:footNotePr>"
    "<hp:endNotePr>"
    '<hp:autoNumFormat type="DIGIT" userChar="" prefixChar="" suffixChar=")" supscript="0"/>'
    '<hp:noteLine length="14692344" type="SOLID" width="0.12 mm" color="#000000"/>'
    '<hp:noteSpacing betweenNotes="0" belowLine="567" aboveLine="850"/>'
    '<hp:numbering type="CONTINUOUS" newNum="1"/>'
    '<hp:placement place="END_OF_DOCUMENT" beneathText="0"/>'
    "</hp:endNotePr>"
    + "".join(
        f'<hp:pageBorderFill type="{side}" borderFillIDRef="{_BORDER_FILL_PLAIN}" '
        'textBorder="PAPER" headerInside="0" footerInside="0" fillArea="PAPER">'
        '<hp:offset left="1417" right="1417" top="1417" bottom="1417"/>'
        "</hp:pageBorderFill>"
        for side in ("BOTH", "EVEN", "ODD")
    )
    + "</hp:secPr>"
    '<hp:ctrl><hp:colPr id="" type="NEWSPAPER" layout="LEFT" colCount="1" sameSz="1" '
    'sameGap="0"/></hp:ctrl>'
)


class _Counter:
    """Monotonic identifier source for paragraph and table elements."""

    def __init__(self) -> None:
        self._value = 0

    def next(self) -> int:
        self._value += 1
        return self._value


def _paragraph_xml(
    counter: _Counter,
    *,
    para_pr: int,
    char_pr: int,
    text: str,
    prefix: str = "",
) -> str:
    body = f"<hp:t>{_escape(text)}</hp:t>" if text else "<hp:t></hp:t>"
    return (
        f'<hp:p id="{counter.next()}" paraPrIDRef="{para_pr}" styleIDRef="0" pageBreak="0" '
        'columnBreak="0" merged="0">'
        f'<hp:run charPrIDRef="{char_pr}">{prefix}{body}</hp:run>'
        "</hp:p>"
    )


def _cell_xml(
    counter: _Counter,
    *,
    text: str,
    column: int,
    row: int,
    width: int,
    header: bool,
) -> str:
    para_pr, char_pr = _TABLE_HEADER_STYLE if header else _TABLE_CELL_STYLE
    border_fill = _BORDER_FILL_HEADER if header else _BORDER_FILL_CELL
    paragraph = _paragraph_xml(counter, para_pr=para_pr, char_pr=char_pr, text=text)
    return (
        f'<hp:tc name="" header="{1 if header else 0}" hasMargin="0" protect="0" editable="0" '
        f'dirty="0" borderFillIDRef="{border_fill}">'
        '<hp:subList id="" textDirection="HORIZONTAL" lineWrap="BREAK" vertAlign="CENTER" '
        'linkListIDRef="0" linkListNextIDRef="0" textWidth="0" textHeight="0" hasTextRef="0" '
        f'hasNumRef="0">{paragraph}</hp:subList>'
        f'<hp:cellAddr colAddr="{column}" rowAddr="{row}"/>'
        '<hp:cellSpan colSpan="1" rowSpan="1"/>'
        f'<hp:cellSz width="{width}" height="1200"/>'
        '<hp:cellMargin left="510" right="510" top="141" bottom="141"/>'
        "</hp:tc>"
    )


def _table_xml(counter: _Counter, table: Table) -> str:
    widths = _column_widths(table)
    rows = [tuple(table.headers), *(tuple(row) for row in table.rows)]
    body = ""
    for row_index, row in enumerate(rows):
        cells = "".join(
            _cell_xml(
                counter,
                text=value,
                column=column_index,
                row=row_index,
                width=widths[column_index],
                header=row_index == 0,
            )
            for column_index, value in enumerate(row)
        )
        body += f"<hp:tr>{cells}</hp:tr>"
    height = 1200 * len(rows)
    table_xml = (
        f'<hp:tbl id="{counter.next()}" zOrder="0" numberingType="TABLE" '
        'textWrap="TOP_AND_BOTTOM" textFlow="BOTH_SIDES" lock="0" dropcapstyle="None" '
        f'pageBreak="CELL" repeatHeader="1" rowCnt="{len(rows)}" colCnt="{len(table.headers)}" '
        f'cellSpacing="0" borderFillIDRef="{_BORDER_FILL_CELL}" noAdjust="0">'
        f'<hp:sz width="{TEXT_WIDTH}" widthRelTo="ABSOLUTE" height="{height}" '
        'heightRelTo="ABSOLUTE" protect="0"/>'
        '<hp:pos treatAsChar="1" affectLSpacing="0" flowWithText="1" allowOverlap="0" '
        'holdAnchorAndSO="0" vertRelTo="PARA" horzRelTo="COLUMN" vertAlign="TOP" '
        'horzAlign="LEFT" vertOffset="0" horzOffset="0"/>'
        '<hp:outMargin left="0" right="0" top="0" bottom="0"/>'
        '<hp:inMargin left="510" right="510" top="141" bottom="141"/>'
        f"{body}</hp:tbl>"
    )
    return (
        f'<hp:p id="{counter.next()}" paraPrIDRef="6" styleIDRef="0" pageBreak="0" '
        'columnBreak="0" merged="0">'
        f'<hp:run charPrIDRef="6">{table_xml}</hp:run>'
        "</hp:p>"
    )


def _section_xml(blocks: Sequence[Block]) -> str:
    counter = _Counter()
    parts: list[str] = []
    pending = _SECTION_PROPERTIES
    for block in blocks:
        if isinstance(block, Table):
            if pending:
                parts.append(_paragraph_xml(counter, para_pr=0, char_pr=0, text="", prefix=pending))
                pending = ""
            parts.append(_table_xml(counter, block))
            continue
        if block.style not in _PARAGRAPH_STYLES:
            raise ValueError(f"unknown paragraph style: {block.style}")
        para_pr, char_pr = _PARAGRAPH_STYLES[block.style]
        text = f"· {block.text}" if block.style == "bullet" else block.text
        parts.append(
            _paragraph_xml(counter, para_pr=para_pr, char_pr=char_pr, text=text, prefix=pending)
        )
        pending = ""
    if pending:
        parts.append(_paragraph_xml(counter, para_pr=0, char_pr=0, text="", prefix=pending))
    return (
        _XML_DECLARATION + f'<hs:sec xmlns:hs="{_NS_SECTION}" xmlns:hp="{_NS_PARAGRAPH}" '
        f'xmlns:hc="{_NS_CORE}" xmlns:hh="{_NS_HEAD}">' + "".join(parts) + "</hs:sec>"
    )


def _version_xml() -> str:
    return (
        _XML_DECLARATION
        + f'<hv:HCFVersion xmlns:hv="{_NS_VERSION}" tagetApplication="WORDPROCESSOR" '
        'major="5" minor="0" micro="5" buildNumber="0" os="1" xmlVersion="1.4" '
        'application="Synthetic Table Studio" appVersion="1.0"/>'
    )


def _container_xml() -> str:
    return (
        _XML_DECLARATION + f'<ocf:container xmlns:ocf="{_NS_OCF}" xmlns:hpf="{_NS_HPF}">'
        "<ocf:rootfiles>"
        '<ocf:rootfile full-path="Contents/content.hpf" '
        'media-type="application/hwpml-package+xml"/>'
        "</ocf:rootfiles>"
        "</ocf:container>"
    )


def _odf_manifest_xml() -> str:
    entries = (
        ("/", "application/hwp+zip"),
        ("version.xml", "application/xml"),
        ("settings.xml", "application/xml"),
        ("Contents/content.hpf", "application/xml"),
        ("Contents/header.xml", "application/xml"),
        ("Contents/section0.xml", "application/xml"),
        ("Preview/PrvText.txt", "text/plain"),
    )
    items = "".join(
        f'<odf:file-entry odf:full-path="{path}" odf:media-type="{media}"/>'
        for path, media in entries
    )
    return (
        _XML_DECLARATION
        + f'<odf:manifest xmlns:odf="{_NS_ODF_MANIFEST}" odf:version="1.0">{items}</odf:manifest>'
    )


def _content_hpf(title: str) -> str:
    return (
        _XML_DECLARATION + f'<opf:package xmlns:opf="{_NS_OPF}" xmlns:ha="{_NS_APP}" version="" '
        'unique-identifier="" id="">'
        "<opf:metadata>"
        f"<opf:title>{_escape(title)}</opf:title>"
        "<opf:language>ko</opf:language>"
        '<opf:meta name="generator" content="Synthetic Table Studio"/>'
        "</opf:metadata>"
        "<opf:manifest>"
        '<opf:item id="header" href="Contents/header.xml" media-type="application/xml"/>'
        '<opf:item id="section0" href="Contents/section0.xml" media-type="application/xml"/>'
        '<opf:item id="settings" href="settings.xml" media-type="application/xml"/>'
        "</opf:manifest>"
        "<opf:spine>"
        '<opf:itemref idref="header" linear="yes"/>'
        '<opf:itemref idref="section0" linear="yes"/>'
        "</opf:spine>"
        "</opf:package>"
    )


def _settings_xml() -> str:
    return (
        _XML_DECLARATION + f'<ha:HWPApplicationSetting xmlns:ha="{_NS_APP}">'
        '<ha:CaretPosition listIDRef="0" paraIDRef="0" pos="0"/>'
        "</ha:HWPApplicationSetting>"
    )


def _preview_text(blocks: Sequence[Block]) -> str:
    lines: list[str] = []
    for block in blocks:
        if isinstance(block, Paragraph):
            lines.append(_sanitize(block.text))
        else:
            lines.append(" | ".join(_sanitize(header) for header in block.headers))
    return "\n".join(line for line in lines if line)[:4000]


def build_hwpx(title: str, blocks: Sequence[Block]) -> bytes:
    """Render blocks into a complete HWPX package.

    The result is deterministic: identical input always produces identical bytes.
    """

    if not title.strip():
        raise ValueError("an HWPX document needs a title")
    parts: list[tuple[str, bytes, int]] = [
        ("mimetype", b"application/hwp+zip", ZIP_STORED),
        ("version.xml", _version_xml().encode("utf-8"), ZIP_DEFLATED),
        ("META-INF/container.xml", _container_xml().encode("utf-8"), ZIP_DEFLATED),
        ("META-INF/manifest.xml", _odf_manifest_xml().encode("utf-8"), ZIP_DEFLATED),
        ("Contents/content.hpf", _content_hpf(title).encode("utf-8"), ZIP_DEFLATED),
        ("Contents/header.xml", _header_xml().encode("utf-8"), ZIP_DEFLATED),
        ("Contents/section0.xml", _section_xml(blocks).encode("utf-8"), ZIP_DEFLATED),
        ("settings.xml", _settings_xml().encode("utf-8"), ZIP_DEFLATED),
        ("Preview/PrvText.txt", _preview_text(blocks).encode("utf-8"), ZIP_DEFLATED),
    ]
    buffer = BytesIO()
    with ZipFile(buffer, "w") as archive:
        for name, payload, compression in parts:
            info = ZipInfo(name, date_time=_FIXED_TIMESTAMP)
            info.compress_type = compression
            info.external_attr = 0o644 << 16
            info.create_system = 0
            archive.writestr(info, payload)
    return buffer.getvalue()
