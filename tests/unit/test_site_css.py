"""Unit: the committed stylesheet matches the markup it is supposed to style.

Tailwind emits only the classes it finds when it scans the templates, and the
result is committed. Change a class without rebuilding and the utility simply
does not exist: the page loses that padding, column or size silently, with no
error anywhere. That happened to every section's vertical rhythm at once.
"""

import pathlib
import re

import pytest

SITE = pathlib.Path(__file__).resolve().parents[2] / "site"
CSS = SITE / "web" / "static" / "app.css"
TEMPLATES = sorted((SITE / "web" / "templates").rglob("*.html"))

# Utilities whose absence changes the layout rather than a colour: if one of
# these is missing the page is visibly broken, which is what makes them worth
# checking. Variants (sm:, dark:, hover:) are checked through their base class.
LAYOUT = re.compile(
    r"^(p|px|py|pt|pb|pl|pr|m|mx|my|mt|mb|gap|gap-x|gap-y|w|h|max-w|min-w|"
    r"grid-cols|col-span|text|leading|tracking|rounded|border|ring|inset)-[\w./\[\]%-]+$"
)


def _classes() -> set[str]:
    out: set[str] = set()
    for template in TEMPLATES:
        for attr in re.findall(r'class="([^"]*)"', template.read_text()):
            if "{{" in attr or "{%" in attr:  # built at render time, not scannable
                continue
            for token in attr.split():
                # Keep the variant: `sm:py-16` is emitted as its own selector
                # inside a media query, and no bare `.py-16` need exist.
                if LAYOUT.match(token.split(":")[-1]):
                    out.add(token)
    return out


def _escaped(css: str) -> str:
    """Undo CSS identifier escaping so selectors read like the class names do.

    A leading digit cannot start an identifier, so Tailwind writes `2xl:px-24`
    as `.\\32 xl\\:px-24` - a hex escape with a trailing space. Everything else
    (`.` `/` `[` `]` `%` `:`) is escaped with a plain backslash.
    """
    css = re.sub(r"\\3([0-9a-f]) ", r"\1", css)
    return css.replace("\\", "")


def _defines(css: str, name: str) -> bool:
    """Whether the stylesheet defines this class, not merely one that starts with
    its name: `.w-2.5` is present in every build and must not answer for `w-2`.

    A class name continues through letters, digits, `_`, `-`, and the characters
    Tailwind escapes into a selector (`.` `/` `%` `[`), so anything else ends it.
    """
    return re.search(re.escape(f".{name}") + r"(?![\w./%\[-])", css) is not None


@pytest.mark.skipif(not CSS.exists(), reason="the site's stylesheet is not built")
def test_every_layout_utility_the_markup_uses_exists_in_the_stylesheet():
    css = _escaped(CSS.read_text())
    used = _classes()
    assert used, "found no layout classes in the templates"
    missing = sorted(c for c in used if not _defines(css, c))
    assert missing == [], (
        f"{len(missing)} utilities are used but absent from app.css, so they do nothing: "
        f"{missing}. Rebuild with ./tailwindcss -i styles/input.css -o web/static/app.css --minify"
    )
