import argparse
import textwrap
from pathlib import Path


PAGE_WIDTH = 595
PAGE_HEIGHT = 842
MARGIN_X = 40
MARGIN_TOP = 40
MARGIN_BOTTOM = 40
FONT_SIZE = 10
LINE_HEIGHT = 13
MAX_LINE_CHARS = 108


def escape_pdf_text(text: str) -> str:
    return (
        text.replace("\\", "\\\\")
        .replace("(", "\\(")
        .replace(")", "\\)")
    )


def wrap_text_lines(text: str, width: int = MAX_LINE_CHARS) -> list[str]:
    lines: list[str] = []
    for raw_line in text.splitlines():
        if not raw_line.strip():
            lines.append("")
            continue

        line = raw_line.rstrip()
        leading = len(line) - len(line.lstrip(" "))
        indent = " " * leading
        wrapped = textwrap.wrap(
            line,
            width=width,
            initial_indent="",
            subsequent_indent=indent,
            break_long_words=False,
            break_on_hyphens=False,
        )
        if wrapped:
            lines.extend(wrapped)
        else:
            lines.append("")
    return lines


def paginate(lines: list[str]) -> list[list[str]]:
    usable_height = PAGE_HEIGHT - MARGIN_TOP - MARGIN_BOTTOM
    lines_per_page = max(1, usable_height // LINE_HEIGHT)
    return [lines[i:i + lines_per_page] for i in range(0, len(lines), lines_per_page)]


def build_content_stream(lines: list[str]) -> bytes:
    if not lines:
        lines = [""]

    start_y = PAGE_HEIGHT - MARGIN_TOP - FONT_SIZE
    commands = [
        "BT",
        f"/F1 {FONT_SIZE} Tf",
        f"{LINE_HEIGHT} TL",
        f"{MARGIN_X} {start_y} Td",
    ]

    first = True
    for line in lines:
        escaped = escape_pdf_text(line)
        if first:
            commands.append(f"({escaped}) Tj")
            first = False
        else:
            commands.append("T*")
            commands.append(f"({escaped}) Tj")

    commands.append("ET")
    stream_data = "\n".join(commands).encode("utf-8")
    return b"<< /Length " + str(len(stream_data)).encode("ascii") + b" >>\nstream\n" + stream_data + b"\nendstream"


def build_pdf(pages: list[list[str]]) -> bytes:
    objects: list[bytes | None] = []

    def new_obj(data: bytes | None) -> int:
        objects.append(data)
        return len(objects)

    catalog_id = new_obj(None)
    pages_id = new_obj(None)
    font_id = new_obj(b"<< /Type /Font /Subtype /Type1 /BaseFont /Courier >>")

    page_obj_ids: list[int] = []
    for page_lines in pages:
        content_id = new_obj(build_content_stream(page_lines))
        page_obj = (
            f"<< /Type /Page /Parent {pages_id} 0 R "
            f"/MediaBox [0 0 {PAGE_WIDTH} {PAGE_HEIGHT}] "
            f"/Resources << /Font << /F1 {font_id} 0 R >> >> "
            f"/Contents {content_id} 0 R >>"
        ).encode("ascii")
        page_id = new_obj(page_obj)
        page_obj_ids.append(page_id)

    kids = " ".join(f"{page_id} 0 R" for page_id in page_obj_ids)
    objects[pages_id - 1] = (
        f"<< /Type /Pages /Kids [{kids}] /Count {len(page_obj_ids)} >>"
    ).encode("ascii")
    objects[catalog_id - 1] = (
        f"<< /Type /Catalog /Pages {pages_id} 0 R >>"
    ).encode("ascii")

    output = b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n"
    offsets = [0]
    for idx, obj in enumerate(objects, start=1):
        if obj is None:
            raise ValueError(f"Object {idx} is undefined")
        offsets.append(len(output))
        output += f"{idx} 0 obj\n".encode("ascii")
        output += obj
        output += b"\nendobj\n"

    xref_pos = len(output)
    output += f"xref\n0 {len(objects) + 1}\n".encode("ascii")
    output += b"0000000000 65535 f \n"
    for offset in offsets[1:]:
        output += f"{offset:010d} 00000 n \n".encode("ascii")
    output += (
        f"trailer\n<< /Size {len(objects) + 1} /Root {catalog_id} 0 R >>\n"
        f"startxref\n{xref_pos}\n%%EOF"
    ).encode("ascii")
    return output


def main() -> None:
    parser = argparse.ArgumentParser(description="Convert plain text to PDF (no external deps).")
    parser.add_argument("--input", required=True, help="Input plain text file path")
    parser.add_argument("--output", required=True, help="Output PDF file path")
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    text = input_path.read_text(encoding="utf-8")
    wrapped_lines = wrap_text_lines(text)
    pages = paginate(wrapped_lines)
    pdf_bytes = build_pdf(pages)
    output_path.write_bytes(pdf_bytes)

    print(f"PDF written: {output_path}")
    print(f"Pages: {len(pages)}")


if __name__ == "__main__":
    main()
