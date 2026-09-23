# The toro website

Served two ways from one set of templates, so the marketing page and the product
cannot drift apart:

    uv run uvicorn web.app:app --port 8100   # live: a real queue, real workers,
                                             # matador mounted at /dashboard
    uv run python build_static.py /toro      # static: what GitHub Pages serves

The live queue panel needs a server, so the static build replaces it with a
labelled snapshot that links to the deployed demo. Everything else, including the
htmx example switcher, is identical: htmx only ever does a GET for HTML, which a
file on disk answers as well as a route.

The stylesheet is built with Tailwind's standalone CLI and committed, the way
matador commits its own:

    ./tailwindcss -i styles/input.css -o web/static/app.css --minify
