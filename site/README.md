# The toro website

The published page is static HTML, built from these templates:

    uv run python build_static.py /toro      # writes dist/, what GitHub Pages serves

The same templates render live while you edit them, which is the only reason this
is a FastAPI app rather than a folder of files:

    uv run uvicorn web.app:app --port 8100

The htmx example switcher only ever does a GET for HTML, which a file on disk
answers as well as a route, so it works in both renders. The code shown on the
page is checked against the library by `tests/unit/test_site_snippets.py`.

The stylesheet is built with Tailwind's standalone CLI and committed, the way
matador commits its own:

    ./tailwindcss -i styles/input.css -o web/static/app.css --minify

`tests/unit/test_site_css.py` fails if the markup uses a utility this file does
not contain, which is what a forgotten rebuild looks like.
