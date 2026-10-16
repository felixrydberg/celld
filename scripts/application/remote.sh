#!/bin/sh
# Runs a command on a Linux/KVM test host over SSH, in a fresh export of this
# checkout: every file git does not ignore, as it is now, with LF line endings.
# Cargo's target directory survives between runs, and the run's evidence/
# comes back to evidence/<host>-<time>/. It logs in with ~/.ssh/celld-test.
#
#   scripts/application/remote.sh [user@]host 'command'
set -eu
host=${1:?usage: remote.sh [user@]host command}
command=${2:?usage: remote.sh [user@]host command}
dir=${CELLD_REMOTE_DIR:-celld-remote}
ssh() { command ssh -i "$HOME/.ssh/celld-test" "$@"; }
cd "$(git rev-parse --show-toplevel)"
# A throwaway index leaves the real one alone; `input` stores LF whatever
# this checkout's line endings are.
index=$(mktemp)
trap 'rm -f "$index"' EXIT
cp "$(git rev-parse --git-path index)" "$index"
GIT_INDEX_FILE=$index git -c core.autocrlf=input -c core.safecrlf=false add -A
tree=$(GIT_INDEX_FILE=$index git write-tree)
git archive "$tree" |
    ssh "$host" "rm -rf $dir/src && mkdir -p $dir/src && tar -xm -C $dir/src"
status=0
ssh -t "$host" "cd $dir/src && export PATH=\$HOME/.cargo/bin:\$PATH CARGO_TARGET_DIR=\$HOME/$dir/target && $command" || status=$?
if ssh "$host" "test -d $dir/src/evidence"; then
    evidence=evidence/${host#*@}-$(date -u +%Y%m%dT%H%M%SZ)
    mkdir -p "$evidence"
    ssh "$host" "tar -c -C $dir/src/evidence ." | tar -xm -C "$evidence"
    echo "evidence: $evidence" >&2
fi
exit $status
