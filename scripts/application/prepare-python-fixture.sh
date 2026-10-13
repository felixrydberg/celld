#!/bin/sh
# Prepare the Python handler project of the actual-VM check in a new directory:
# the fixture plus the Workers SDK vendored into python_modules/ (what
# `pywrangler sync` writes). PyPI is read once and the wheel's SHA-256 is pinned;
# the Pyodide files are fetched, and pinned the same way, by the celld build.
set -eu
out=${1:?new project directory}
version=1.9.3
sha256=b3982ef01b824703258bbdd2c65980b8acc64f9dc5797f6b193eaf4f0808860f
url=https://files.pythonhosted.org/packages/aa/06/9472faa56bf7f3925d1128fda0532a9ec8adf3291a6003a4cf9c9d7972d0/workers_runtime_sdk-$version-py3-none-any.whl
test ! -e "$out"
mkdir -p "$out"
cp -r "$(dirname "$0")/../../crates/celld/tests/fixtures/python-worker/." "$out/"
download=$(mktemp -d)
trap 'rm -rf "$download"' EXIT
curl -fsSL -o "$download/sdk.whl" "$url"
echo "$sha256  $download/sdk.whl" | sha256sum -c -
python3 -I -c 'import sys, zipfile; zipfile.ZipFile(sys.argv[1]).extractall(sys.argv[2])' "$download/sdk.whl" "$out/python_modules"
