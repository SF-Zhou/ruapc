#!/usr/bin/env python3
"""Validate the static landing page before GitHub Pages deploys it.

This is intentionally dependency-free so CI only needs Python. It catches the
mistakes that otherwise become broken links after a deploy: missing assets,
stale in-page anchors, duplicate ids, malformed ARIA references and missing
social metadata.
"""

from __future__ import annotations

import re
import sys
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import unquote, urlsplit

SITE = Path(__file__).resolve().parents[1]
INDEX = SITE / "index.html"
SITE_URL = "https://sf-zhou.github.io/ruapc/"

problems: list[str] = []
warnings: list[str] = []


class PageParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.ids: dict[str, int] = {}
        self.local_refs: list[tuple[str, str]] = []
        self.anchors: list[str] = []
        self.aria_refs: list[tuple[str, str]] = []
        self.meta: dict[str, str] = {}
        self.links: list[dict[str, str]] = []
        self.scripts: list[dict[str, str]] = []
        self.images: list[dict[str, str]] = []
        self.title_parts: list[str] = []
        self.in_title = False
        self.forms: list[dict[str, str]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        data = {key: value or "" for key, value in attrs}

        if "id" in data:
            self.ids[data["id"]] = self.ids.get(data["id"], 0) + 1

        if tag == "a" and data.get("href"):
            href = data["href"].strip()
            if href.startswith("#"):
                self.anchors.append(href[1:])
            elif not _is_external(href):
                self.local_refs.append((href, f"<a href={href!r}>"))

        if tag in {"img", "script", "source"}:
            for attr in ("src", "srcset"):
                value = data.get(attr, "").strip()
                if not value or _is_external(value) or value.startswith("data:"):
                    continue
                candidates = [value] if attr == "src" else _srcset_urls(value)
                for candidate in candidates:
                    self.local_refs.append((candidate, f"<{tag} {attr}>"))

        if tag == "link":
            self.links.append(data)
            href = data.get("href", "").strip()
            if href and not _is_external(href) and not href.startswith("data:"):
                self.local_refs.append((href, f"<link rel={data.get('rel', '')!r}>"))

        if tag == "script":
            self.scripts.append(data)

        if tag == "img":
            self.images.append(data)

        if tag == "meta":
            key = data.get("property") or data.get("name")
            if key:
                self.meta[key] = data.get("content", "")

        if tag == "form":
            self.forms.append(data)

        if tag == "title":
            self.in_title = True

        for attr in ("aria-controls", "aria-labelledby", "aria-describedby"):
            if data.get(attr):
                for ref in data[attr].split():
                    self.aria_refs.append((ref, f"<{tag} {attr}>"))

    def handle_endtag(self, tag: str) -> None:
        if tag == "title":
            self.in_title = False

    def handle_data(self, data: str) -> None:
        if self.in_title:
            self.title_parts.append(data)


def _is_external(value: str) -> bool:
    parsed = urlsplit(value)
    return bool(parsed.scheme and parsed.scheme not in {"http", "https"}) or bool(
        parsed.netloc
    ) or value.startswith("//")


def _srcset_urls(value: str) -> list[str]:
    urls: list[str] = []
    for part in value.split(","):
        url = part.strip().split()[0] if part.strip() else ""
        if url and not url.startswith("data:"):
            urls.append(url)
    return urls


def _local_path(ref: str, base: Path) -> Path:
    parsed = urlsplit(ref)
    path = unquote(parsed.path)
    return (base / path).resolve()


def _check_local_ref(ref: str, where: str) -> None:
    if ref.startswith("/"):
        problems.append(f"{where}: use a relative path for project Pages, got {ref!r}")
        return
    target = _local_path(ref, SITE)
    if not target.is_file():
        problems.append(f"{where}: missing file {ref!r} (resolved to {target})")


def _check_css_urls() -> None:
    for css_file in sorted((SITE / "assets").glob("*.css")):
        text = css_file.read_text(encoding="utf-8")
        for match in re.finditer(r"url\(\s*(['\"]?)(.*?)\1\s*\)", text):
            ref = match.group(2).strip()
            if not ref or ref.startswith(("data:", "http:", "https:", "//", "#")):
                continue
            target = _local_path(ref, css_file.parent)
            if not target.is_file():
                problems.append(f"{css_file.relative_to(SITE)}: missing CSS asset {ref!r}")


def main() -> int:
    if not INDEX.is_file():
        print("error: site/index.html is missing", file=sys.stderr)
        return 1

    parser = PageParser()
    parser.feed(INDEX.read_text(encoding="utf-8"))
    parser.close()

    for duplicate, count in parser.ids.items():
        if count > 1:
            problems.append(f"duplicate id {duplicate!r} appears {count} times")

    for anchor in parser.anchors:
        if not anchor:
            problems.append("empty in-page anchor")
        elif anchor not in parser.ids:
            problems.append(f"in-page anchor #{anchor} has no matching id")

    for ref, where in parser.local_refs:
        _check_local_ref(ref, where)

    for ref, where in parser.aria_refs:
        if ref not in parser.ids:
            problems.append(f"{where}: ARIA reference {ref!r} has no matching id")

    required_meta = {
        "description",
        "viewport",
        "og:title",
        "og:description",
        "og:image",
        "og:url",
        "twitter:card",
        "theme-color",
    }
    for key in sorted(required_meta):
        if not parser.meta.get(key):
            problems.append(f"missing or empty meta {key!r}")

    title = "".join(parser.title_parts).strip()
    if not title:
        problems.append("missing <title>")

    canonical = next(
        (
            link.get("href", "")
            for link in parser.links
            if "canonical" in link.get("rel", "").split()
        ),
        "",
    )
    if canonical.rstrip("/") != SITE_URL.rstrip("/"):
        problems.append(f"canonical URL is {canonical!r}, expected {SITE_URL!r}")

    og_image = parser.meta.get("og:image", "")
    if og_image and not og_image.startswith(SITE_URL):
        problems.append(f"og:image must use {SITE_URL}: got {og_image!r}")

    for image in parser.images:
        if "alt" not in image:
            problems.append(f"<img src={image.get('src', '')!r}> is missing alt")

    for form in parser.forms:
        if not form.get("action"):
            warnings.append("form has no action; static site forms do nothing")

    if not parser.scripts:
        problems.append("no scripts found; expected assets/main.js")

    if not re.search(r"<html[^>]+lang=[\"'][a-zA-Z-]+[\"']", INDEX.read_text(encoding="utf-8")):
        problems.append("<html> is missing a language attribute")

    viewport = parser.meta.get("viewport", "")
    if "width=device-width" not in viewport:
        problems.append("viewport meta must include width=device-width")

    og_url = parser.meta.get("og:url", "")
    if canonical and og_url.rstrip("/") != canonical.rstrip("/"):
        problems.append("og:url must match the canonical URL")

    for key in ("og:image", "twitter:image"):
        url = parser.meta.get(key, "")
        if url.startswith(SITE_URL):
            rel = url[len(SITE_URL) :]
            if not (SITE / rel).is_file():
                problems.append(f"{key} points at missing local file {rel!r}")

    _check_css_urls()

    for name in ("robots.txt", "sitemap.xml"):
        path = SITE / name
        if not path.is_file():
            problems.append(f"missing crawler metadata file {name!r}")

    sitemap = SITE / "sitemap.xml"
    if sitemap.is_file() and SITE_URL not in sitemap.read_text(encoding="utf-8"):
        problems.append(f"sitemap.xml does not contain {SITE_URL!r}")

    robots = SITE / "robots.txt"
    if robots.is_file() and f"{SITE_URL}sitemap.xml" not in robots.read_text(encoding="utf-8"):
        problems.append("robots.txt does not point at the sitemap")

    page_bytes = INDEX.stat().st_size
    asset_count = sum(1 for path in (SITE / "assets").rglob("*") if path.is_file())

    print(f"Checked {INDEX.name}: {page_bytes:,} bytes, {asset_count} asset files")
    for warning in warnings:
        print(f"warning: {warning}")
    if problems:
        print("\nProblems:", file=sys.stderr)
        for problem in problems:
            print(f"  - {problem}", file=sys.stderr)
        return 1
    print("OK: links, local assets, ids, ARIA references and social metadata")
    return 0


if __name__ == "__main__":
    sys.exit(main())
