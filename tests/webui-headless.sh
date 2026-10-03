#!/usr/bin/env bash
# Headless-Chrome check of the embedded web UI's snapshots page (plan 32
# Step 7): start a daemon with the web UI on the local file backend, give it
# a directory, a policy root and two manual snapshots (one held by
# `csi:test`), load /snapshots.html in headless Chrome, and assert on the
# rendered DOM: the table rows, the "externally held" chip, the space-bar
# segments, the policy card, the (hidden) silent-failure banner, and no JS
# error (the page writes a `data-js-error` marker on window errors,
# unhandled rejections and console.error; Chrome's stderr is grepped for
# `Uncaught` too). Then it opens the policy editor on /proj through the URL
# (`#edit=/proj&preset=standard`: the editor loads the root's policy and
# applies the Standard preset) and asserts the rows, the daemon's canonical
# form, and the retention timeline SVG (one lane per tier plus held/manual,
# ticks — future ones and the csi pin among them — and the count chart).
# Screenshots of both are kept for the report.
#
# Usage: tests/webui-headless.sh
#
# Knobs:
#   CHROME_BIN          the browser (default: google-chrome, then
#                       google-chrome-stable, chromium, chromium-browser on
#                       PATH). Absent: SKIP, exit 0. Anything taking
#                       Chrome's flags works, e.g. a wrapper running
#                       Chromium in a container (with host networking and
#                       /tmp shared, so the screenshot lands on the host).
#   CONSTELLATION_BIN   binary under test (default
#                       ${CARGO_TARGET_DIR:-target}/debug/constellation)
#   WEBUI_SHOT          screenshot path (default: a fresh file under /tmp)
#   WEBUI_EDITOR_SHOT   the editor's screenshot (default: next to WEBUI_SHOT)
#   CONSTELLATION_SNAPSCHED  passed to the daemon; defaults to 0 here (rows
#                       are deterministic); 1 runs the scheduler against
#                       the paused policy
#   KEEP=1              keep the work directory
set -euo pipefail

say() { echo "== $*"; }
fail() { echo "FAIL: $*" >&2; exit 1; }

chrome="${CHROME_BIN:-}"
if [ -z "$chrome" ]; then
    for c in google-chrome google-chrome-stable chromium chromium-browser; do
        if command -v "$c" >/dev/null 2>&1; then chrome="$c"; break; fi
    done
fi
if [ -z "$chrome" ]; then
    echo "SKIP: webui-headless: no google-chrome/chromium on PATH (set CHROME_BIN)"
    exit 0
fi

root="$(cd "$(dirname "$0")/.." && pwd)"
target="${CARGO_TARGET_DIR:-$root/target}"
bin="${CONSTELLATION_BIN:-$target/debug/constellation}"
[ -x "$bin" ] || fail "constellation binary not found at $bin (cargo build, or set CONSTELLATION_BIN)"
command -v curl >/dev/null || fail "curl is needed"

work="$(mktemp -d /tmp/webui-headless.XXXXXX)"
backend="$work/backend" mnt="$work/mnt" state="$work/state" log="$work/mount.log"
shot="${WEBUI_SHOT:-$work/snapshots.png}"
edshot="${WEBUI_EDITOR_SHOT:-$(dirname "$shot")/editor.png}"
mkdir -p "$mnt" "$state"
pid=""
cleanup() {
    if mountpoint -q "$mnt" 2>/dev/null; then
        fusermount3 -u "$mnt" 2>/dev/null || fusermount -u "$mnt" 2>/dev/null || true
    fi
    if [ -n "$pid" ]; then
        for _ in $(seq 100); do kill -0 "$pid" 2>/dev/null || break; sleep 0.1; done
        kill "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
    fi
    # The screenshots outlive the work dir when they live in it.
    if [ "${KEEP:-0}" != 1 ]; then
        find "$work" -mindepth 1 -maxdepth 1 ! -name "$(basename "$shot")" ! -name "$(basename "$edshot")" \
            -exec rm -rf {} + 2>/dev/null || true
        rmdir "$work" 2>/dev/null || true
    fi
}
trap cleanup EXIT

port="$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1])')"
base="http://127.0.0.1:$port"

say "fs create + mount with --web-ui $port ($backend)"
"$bin" fs create webui --s3 "$backend" >>"$log" 2>&1 || { tail -20 "$log" >&2; fail "fs create"; }
CONSTELLATION_SNAPSCHED="${CONSTELLATION_SNAPSCHED:-0}" "$bin" mount / "$mnt" --s3 "$backend" --state-dir "$state" --foreground --web-ui "$port" \
    </dev/null >>"$log" 2>&1 &
pid=$!
for _ in $(seq 100); do
    mountpoint -q "$mnt" && curl -sf "$base/api/status" >/dev/null 2>&1 && break
    kill -0 "$pid" 2>/dev/null || { cat "$log" >&2; fail "mount process died"; }
    sleep 0.1
done
mountpoint -q "$mnt" || fail "mount did not appear"
curl -sf "$base/api/status" >/dev/null || fail "web UI not answering on $base"

api() {
    curl -sf -X POST -H 'content-type: application/json' \
        --data "{\"method\":\"$1\",\"params\":$2}" "$base/api"
}

say "a directory, a policy root, two manual snapshots (one held by csi:test)"
mkdir -p "$mnt/proj/sub"
head -c 3145728 /dev/urandom >"$mnt/proj/a.bin"
"$bin" snapshot create /proj@first --state-dir "$state" >>"$log" 2>&1 || fail "snapshot create /proj@first"
head -c 1048576 /dev/urandom >"$mnt/proj/sub/b.bin"
"$bin" snapshot create /proj@second --by csi:test --state-dir "$state" >>"$log" 2>&1 \
    || fail "snapshot create /proj@second --by csi:test"
api snapshot.policy.set '{"path":"/proj","expr":"1h:1d 1d:7d"}' >/dev/null \
    || fail "snapshot.policy.set /proj"
# Paused, so a live scheduler (CONSTELLATION_SNAPSCHED=1) adds no auto snapshot
# and the row count stays deterministic.
api snapshot.policy.pause '{"path":"/proj","paused":true}' >/dev/null \
    || fail "snapshot.policy.pause /proj"

# The page renders the API's answers; wait until the accounting index has
# caught up so the sizes and the bar are real numbers (it is built on the
# first size request, which this is).
for _ in $(seq 150); do
    if api snapshot.space '{}' | grep -q '"building":false'; then break; fi
    sleep 0.2
done
api snapshot.space '{}' | grep -q '"building":false' || fail "snapshot.space still building"

flags=(--headless=new --disable-gpu --no-sandbox --hide-scrollbars --window-size=1280,1600
       --virtual-time-budget=8000)
say "headless Chrome: dump the DOM ($chrome)"
dom="$work/dom.html" errlog="$work/chrome.log"
"$chrome" "${flags[@]}" --enable-logging=stderr --v=0 --dump-dom "$base/snapshots.html" \
    >"$dom" 2>"$errlog" || { tail -20 "$errlog" >&2; fail "chrome --dump-dom failed"; }

# Only RENDERED markup counts: the page's own <script> holds the same
# strings as templates, so drop it (and comments) before grepping.
rendered="$work/rendered.html"
python3 - "$dom" "$rendered" <<'PY'
import re, sys
t = open(sys.argv[1], encoding="utf-8", errors="replace").read()
t = re.sub(r"<script\b.*?</script>", "", t, flags=re.S | re.I)
t = re.sub(r"<!--.*?-->", "", t, flags=re.S)
open(sys.argv[2], "w", encoding="utf-8").write(t)
PY
check() { grep -q -- "$1" "$rendered" || fail "DOM lacks $2"; echo "ok: $2"; }
rows="$(grep -c '<tr class="[^"]*" data-id="' "$rendered" || true)"
[ "$rows" -eq 2 ] || fail "expected 2 table rows, found $rows"
echo "ok: exactly 2 table rows"
check '/proj<span class="muted">@</span>first' "the /proj@first row"
check '/proj<span class="muted">@</span>second' "the /proj@second row"
check '<span class="pill ext" title="held by csi:test' 'the "externally held" chip'
check '>⚑ externally held · csi' 'the chip text'
check '<button type="button" class="pin" disabled="" aria-pressed="true" title="held by csi:test' "the disabled pin of the csi hold"
[ "$(grep -o '<g class="seg' "$rendered" | wc -l || true)" -ge 2 ] || fail "fewer than 2 space-bar segments"
echo "ok: space-bar segments"
check '<svg id="spaceBar"' "the space bar"
check 'unique to one snapshot (' "the space legend"
check 'USED values do not sum to the total' 'the "USED values do not sum" note'
check '<article class="root" data-ino="' "a policy-root card"
check '1h:1d 1d:7d\|1d:7d 1h:1d' "the canonical expression"
check '<svg id="writtenChart"' 'the "written over time" chart'
check 'as of commit [0-9]' "the as-of footer"
# Plan 32 Step 9: the silent-failure banner is there, and hidden on this
# healthy node (no unparseable or capped root, no refused tick).
check '<div id="snapWarn" role="alert" hidden="">' "the silent-failure banner, hidden while healthy"
if grep -q 'data-js-error' "$dom"; then
    grep -o '<div class="js-error"[^<]*' "$dom" >&2
    fail "the page raised a JS error"
fi
if grep -E 'Uncaught|CONSOLE.*(Error|error)' "$errlog" >&2; then
    fail "JS error in Chrome's log"
fi
echo "ok: no JS error"

say "headless Chrome: screenshot"
"$chrome" "${flags[@]}" --screenshot="$shot" "$base/snapshots.html" >/dev/null 2>&1 \
    || fail "chrome --screenshot failed"
[ -s "$shot" ] || fail "no screenshot at $shot"
echo "screenshot: $shot"

# The policy editor (7.3) and the retention timeline (7.4), opened by the
# URL: /proj's policy loaded, then the Standard preset applied. Everything
# asserted below is drawn from snapshot.policy.check/simulate answers.
edurl="$base/snapshots.html#edit=/proj&preset=standard"
edflags=(--headless=new --disable-gpu --no-sandbox --hide-scrollbars --window-size=1500,1250
         --virtual-time-budget=12000)
say "headless Chrome: the policy editor ($edurl)"
eddom="$work/editor-dom.html"
"$chrome" "${edflags[@]}" --enable-logging=stderr --v=0 --dump-dom "$edurl" \
    >"$eddom" 2>"$errlog" || { tail -20 "$errlog" >&2; fail "chrome --dump-dom (editor) failed"; }
python3 - "$eddom" "$rendered" <<'PY2'
import re, sys
t = open(sys.argv[1], encoding="utf-8", errors="replace").read()
t = re.sub(r"<script\b.*?</script>", "", t, flags=re.S | re.I)
t = re.sub(r"<!--.*?-->", "", t, flags=re.S)
open(sys.argv[2], "w", encoding="utf-8").write(t)
PY2
count() { grep -o -- "$1" "$rendered" | wc -l; }
check '<dialog id="policyEditor"[^>]* open' "the editor dialog, open"
[ "$(count '<div class="tier" role="listitem"')" -eq 4 ] || fail "expected the Standard preset's 4 tier rows, found $(count '<div class="tier" role="listitem"')"
echo "ok: 4 tier rows (the Standard preset)"
check '<code id="edCanonical">15m:1d 1h:2d 1d:30d 1mo:1y; paused</code>' "the daemon's canonical form of the preset (the root's paused flag kept)"
check '<svg id="timelineChart"' "the retention timeline"
lanes="$(count '<g class="lane" data-lane="')"
[ "$lanes" -ge 5 ] || fail "expected >= 5 timeline lanes (4 tiers + held/manual), found $lanes"
for lane in tier:15m tier:1h tier:1d tier:1mo held; do
    check "data-lane=\"$lane\"" "the $lane lane"
done
ticks="$(count 'class="mk tick')"
[ "$ticks" -ge 3 ] || fail "expected timeline ticks, found $ticks"
echo "ok: $ticks timeline ticks"
check 'class="mk tick future"' "future (simulated) ticks"
check '<g class="mk tick held"' "the held snapshot's pin"
check '>csi</text>' "the pin's owner namespace label"
check '<svg id="countChart"' "the snapshot-count step chart"
if grep -q 'data-js-error' "$eddom"; then
    grep -o '<div class="js-error"[^<]*' "$eddom" >&2
    fail "the editor raised a JS error"
fi
if grep -E 'Uncaught|CONSOLE.*(Error|error)' "$errlog" >&2; then
    fail "JS error in Chrome's log (editor)"
fi
echo "ok: no JS error (editor)"

# Text → check: an expression typed into the field (`&expr=`, a `7m` tier)
# comes back from snapshot.policy.check as an error at a byte offset, shown
# with a caret under it as the CLI prints it; a valid one fills the rows
# from the daemon's canonical form.
say "headless Chrome: an invalid and a valid typed expression"
for case in bad good; do
    if [ "$case" = bad ]; then expr='5m:1d%207m:1d'; else expr='1d:7d%3B%20tz%3DEurope%2FBudapest%201h:1d'; fi
    "$chrome" "${edflags[@]}" --dump-dom "$base/snapshots.html#edit=/proj&expr=$expr" \
        >"$eddom" 2>"$errlog" || fail "chrome --dump-dom ($case expression) failed"
    python3 - "$eddom" "$rendered" <<'PY2'
import re, sys
t = open(sys.argv[1], encoding="utf-8", errors="replace").read()
t = re.sub(r"<script\b.*?</script>", "", t, flags=re.S | re.I)
open(sys.argv[2], "w", encoding="utf-8").write(t)
PY2
    grep -q 'data-js-error' "$eddom" && fail "the editor raised a JS error ($case expression)"
    if [ "$case" = bad ]; then
        # The caret sits under byte 6, the `7m` tier: `5m:1d 7m:1d` then
        # six spaces and `^ `.
        python3 - "$rendered" <<'PY2' || fail "no caret under byte 6 of the invalid expression"
import html, re, sys
t = open(sys.argv[1], encoding="utf-8").read()
m = re.search(r'<pre class="caret"[^>]*>(.*?)</pre>', t, re.S)
assert m, "no caret block"
lines = html.unescape(m.group(1)).split("\n")
assert lines[0] == "5m:1d 7m:1d" and lines[1].startswith(" " * 6 + "^ "), lines
print("ok: caret:", lines[1].strip())
PY2
    else
        check '<code id="edCanonical">1h:1d 1d:7d; tz=Europe/Budapest</code>' "the canonical form of the typed expression"
        [ "$(count '<div class="tier" role="listitem"')" -eq 2 ] || fail "the typed expression did not fill 2 tier rows"
        check '<option value="1h" selected="">1 hour</option>' "the 1h row"
        check '<option value="1d" selected="">1 day</option>' "the 1d row"
        echo "ok: text → rows (2 rows from the canonical form)"
    fi
done

"$chrome" "${edflags[@]}" --screenshot="$edshot" "$edurl" >/dev/null 2>&1 \
    || fail "chrome --screenshot (editor) failed"
[ -s "$edshot" ] || fail "no screenshot at $edshot"
echo "screenshot: $edshot"
echo "PASS: webui-headless"
