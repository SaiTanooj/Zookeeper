"""Build docs/Day2_Architecture.pdf from docs/Day2_Architecture.md.

    pip install markdown
    python docs/build_pdf.py

Converts the Markdown to a styled HTML file, then prints it to PDF with
headless Microsoft Edge or Google Chrome (no extra PDF libraries needed).
"""

import os
import shutil
import subprocess
import sys

import markdown

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(HERE, "Day2_Architecture.md")
HTML = os.path.join(HERE, "Day2_Architecture.html")
PDF = os.path.join(HERE, "Day2_Architecture.pdf")

CSS = """
@page { size: A4; margin: 18mm 16mm 18mm 16mm; }
body { font-family: "Segoe UI", Calibri, Arial, sans-serif; font-size: 10.5pt;
       line-height: 1.5; color: #1f2328; }
h1 { color: #1f3a5f; font-size: 20pt; border-bottom: 2px solid #1f3a5f;
     padding-bottom: 4px; margin-top: 0; break-before: page; }
h1:first-of-type { break-before: avoid; font-size: 24pt; border: none; text-align: center;
                   margin-top: 60mm; }
h1:first-of-type + h2 { text-align: center; color: #44546a; border: none; font-size: 15pt; }
h2 { color: #1f3a5f; font-size: 14pt; margin-top: 18px; border-bottom: 1px solid #d0d7de; }
h3 { color: #2b4a73; font-size: 12pt; }
p, li { text-align: left; }
code { font-family: Consolas, "Courier New", monospace; font-size: 9pt;
       background: #f3f5f7; padding: 1px 4px; border-radius: 3px; }
pre { background: #f6f8fa; border: 1px solid #d0d7de; border-radius: 5px;
      padding: 8px 10px; font-size: 8.4pt; line-height: 1.35; white-space: pre;
      overflow: hidden; }
pre code { background: none; padding: 0; font-size: inherit; }
table { border-collapse: collapse; width: 100%; margin: 8px 0 12px; font-size: 9.4pt; }
tr { break-inside: avoid; }
h1, h2, h3 { break-after: avoid; }
th code { background: none; color: inherit; }
th { background: #1f3a5f; color: white; text-align: left; padding: 5px 7px; }
td { border: 1px solid #d0d7de; padding: 4px 7px; vertical-align: top; }
tr:nth-child(even) td { background: #f6f8fa; }
hr { border: none; border-top: 1px solid #d0d7de; margin: 14px 0; }
.toc { break-before: page; }
.toc::before { content: "Contents"; display: block; font-size: 20pt; color: #1f3a5f;
               font-weight: 600; border-bottom: 2px solid #1f3a5f; margin-bottom: 8px; }
.toc ul { list-style: none; padding-left: 16px; margin: 2px 0; }
.toc > ul { padding-left: 0; }
.toc > ul > li:first-child { display: none; }   /* the title itself */
.toc > ul > li { margin-top: 6px; font-weight: 600; }
.toc > ul > li li { font-weight: normal; font-size: 9.5pt; }
.toc a { color: #1f2328; text-decoration: none; }
"""


def find_browser():
    candidates = [
        r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
        r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
        r"C:\Program Files\Google\Chrome\Application\chrome.exe",
        shutil.which("google-chrome"), shutil.which("chromium"), shutil.which("msedge"),
    ]
    for path in candidates:
        if path and os.path.exists(path):
            return path
    sys.exit("No Edge or Chrome found: open Day2_Architecture.html and print it to PDF by hand.")


def main():
    with open(SRC, encoding="utf-8") as f:
        body = markdown.markdown(
            f.read(), extensions=["tables", "fenced_code", "toc"],
            extension_configs={"toc": {"toc_depth": "1-2"}},
        )
    with open(HTML, "w", encoding="utf-8") as f:
        f.write(f"<!DOCTYPE html><html><head><meta charset='utf-8'>"
                f"<title>Day 2 Architecture</title><style>{CSS}</style></head>"
                f"<body>{body}</body></html>")

    browser = find_browser()
    subprocess.run([browser, "--headless", "--disable-gpu", "--no-pdf-header-footer",
                    f"--print-to-pdf={PDF}", "file:///" + HTML.replace(os.sep, "/")],
                   check=True, capture_output=True)
    print(f"wrote {PDF}")


if __name__ == "__main__":
    main()
