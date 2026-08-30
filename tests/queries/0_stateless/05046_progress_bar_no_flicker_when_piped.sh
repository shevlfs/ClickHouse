#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Issue #80056: the progress bar must not flicker when the query result is
# written to a pipe or a file while the progress is rendered on the terminal.
# In that case the data cannot interleave with the progress rendering, so the
# client must not clear the progress bar before flushing every data block.
# When the data itself goes to the terminal, or when the progress goes to the
# same pipe as the data (`--progress err 2>&1 | ...`), the bar must still be
# cleared before every data block, otherwise the data would interleave with
# the bar.
#
# The helper runs a streaming query with the progress rendering enabled and
# stdout going to different destinations per mode:
#   piped:    stderr on a pty (progress on the terminal), stdout to /dev/null
#             (like piping the result into another tool);
#   terminal: stderr and stdout both on the same pty (interactive terminal);
#   merged:   no terminal at all, stdout and stderr merged into one pipe with
#             `--progress err` (progress and data interleave in the pipe).
# It prints the number of progress redraws and of erase sequences
# ("\r" ESC "[K") observed in the stream: a redraw is "\r<bar>...ESC[K" and an
# erase is exactly "\r" ESC "[K", so the erase count is the number of times
# the bar was wiped. The counts depend only on the number of flushed blocks,
# not on timing.

HELPER="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_pty.py"
trap 'rm -f "$HELPER"' EXIT

cat > "$HELPER" << 'EOF'
import fcntl
import os
import pty
import re
import select
import struct
import sys
import termios

mode = sys.argv[1]  # "piped", "terminal" or "merged": where stdout goes
command = sys.argv[2]

if mode == "merged":
    master, slave = os.pipe()
else:
    master, slave = pty.openpty()
    fcntl.ioctl(master, termios.TIOCSWINSZ, struct.pack("HHHH", 30, 120, 0, 0))

pid = os.fork()
if pid == 0:
    os.close(master)
    devnull = os.open(os.devnull, os.O_RDWR)
    os.dup2(devnull, 0)
    os.dup2(slave if mode in ("terminal", "merged") else devnull, 1)
    os.dup2(slave, 2)
    os.close(slave)
    os.close(devnull)
    os.execv("/bin/sh", ["/bin/sh", "-c", command])
    os._exit(127)

os.close(slave)

data = b""
while True:
    r, _, _ = select.select([master], [], [], 60.0)
    if not r:
        break
    try:
        chunk = os.read(master, 65536)
    except OSError:
        break
    if not chunk:
        break
    data += chunk
os.close(master)
os.waitpid(pid, 0)

redraws = data.count(b"Progress:")
# clearProgressOutput emits exactly "\r\x1b[K" with nothing in between, while a
# progress redraw is "\r<bar content>...\x1b[K", so this counts only the erases.
erases = len(re.findall(rb"\r\x1b\[K", data))

if redraws == 0:
    print(f"{mode}: FAIL: no progress was rendered")
elif mode == "piped" and erases > 5:
    print(f"{mode}: FAIL: the progress bar flickers: {erases} erase sequences for {redraws} redraws")
elif mode in ("terminal", "merged") and erases < 5:
    print(f"{mode}: FAIL: the progress bar is not cleared before the data: {erases} erase sequences for {redraws} redraws")
else:
    print(f"{mode}: OK")
EOF

# 3 million rows in blocks of 100000: every flushed block used to erase and
# redraw the bar, so the erase count tells whether the bar flickered.
python3 "$HELPER" piped \
    "$CLICKHOUSE_LOCAL --progress --format TSV --query 'SELECT * FROM numbers(3000000) SETTINGS max_block_size = 100000'"

# When the data goes to the terminal, the bar must be cleared before every
# data block (fewer rows: the data itself goes through the pty).
python3 "$HELPER" terminal \
    "$CLICKHOUSE_LOCAL --progress --format TSV --query 'SELECT * FROM numbers(200000) SETTINGS max_block_size = 10000'"

# When the progress goes into the same pipe as the data, the bar must also be
# cleared before every data block, or the data is appended to stale bar lines.
python3 "$HELPER" merged \
    "$CLICKHOUSE_LOCAL --progress err --format TSV --query 'SELECT * FROM numbers(200000) SETTINGS max_block_size = 10000' 2>&1"
