#!/bin/sh
# Model-checks RelayLiveness.tla in every configuration and compares each
# verdict with the one its first line expects: "violated" means the checker
# must find a counterexample, anything else means it must find none.
#
#   specs/check.sh path/to/tla2tools.jar
set -u
jar=${1:?usage: check.sh path/to/tla2tools.jar}
cd "$(dirname "$0")"
meta=$(mktemp -d)
status=0
for cfg in RelayLiveness*.cfg; do
    expect=pass
    head -1 "$cfg" | grep -q violated && expect=fail
    started=$(date +%s)
    out=$(java -XX:+UseParallelGC -cp "$jar" tlc2.TLC -workers auto \
        -metadir "$meta/$cfg" -config "$cfg" RelayLiveness.tla 2>&1)
    if echo "$out" | grep -q "No error has been found"; then
        got=pass
    elif echo "$out" | grep -q "is violated"; then
        got=fail
    else
        got=error
    fi
    states=$(echo "$out" | grep -o '[0-9,]* distinct states found' | tail -1)
    echo "$cfg: expected $expect, got $got ($states, $(( $(date +%s) - started ))s)"
    if [ "$got" != "$expect" ]; then
        status=1
        echo "$out" | tail -40
    fi
done
rm -rf "$meta"
exit $status
