#!/bin/sh
# Refuses to release anything but a v* tag whose version is the one just built.
# The tag starts the release and the manifest supplies the version; nothing else makes
# them agree, and a PyPI upload cannot be taken back. A manual run from a branch has no
# tag to compare, so it fails here rather than publishing whatever the branch holds.
set -eu
: "${REF:?the workflow passes github.ref}"
case "$REF" in
  refs/tags/v*) ;;
  *)
    echo "a release runs from a v* tag, not $REF" >&2
    exit 1
    ;;
esac
tag=${REF#refs/tags/}
set -- dist/*.whl
if [ ! -f "$1" ]; then
  echo "no wheel in dist/ to compare $tag with" >&2
  exit 1
fi
built=$(basename "$1" | awk -F- '{print $2}')
if [ "$built" != "${tag#v}" ]; then
  echo "tag $tag does not match the built version $built" >&2
  exit 1
fi
