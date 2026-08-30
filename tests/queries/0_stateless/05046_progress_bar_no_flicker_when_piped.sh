#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Issue #80056: the progress bar must not flicker when the query result is
# written to a pipe or a file while the progress is rendered on the terminal.
# In that case the data cannot interleave with the progress rendering, so the
# client must not clear the progress bar before flushing every data block.
# When the data itself goes to the terminal, the bar must still be cleared
# before every data block, otherwise the data would interleave with the bar.
#
# The helper runs a streaming query with stderr attached to a pty (so the
# progress bar is rendered) and stdout either redirected to /dev/null (like
# piping the result into another tool) or attached to the same pty (like an
# interactive terminal). It prints the number of progress redraws and of
# erase sequences ("\r" ESC "[K") observed in the pty stream: a redraw is
# "\r<bar>...ESC[K" and an erase is exactly "\r" ESC "[K", so the erase count
# is the number of times the bar was wiped from the screen.

cat > "${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_pty.py" << 'EOF'
import fcntl
import os
import pty
import re
import select
import struct
import sys
import termios

mode = sys.argv[1]  # "piped" or "terminal": where stdout goes
command = sys.argv[2]

master, slave = pty.openpty()
fcntl.ioctl(master, termios.TIOCSWINSZ, struct.pack("HHHH", 30, 120, 0, 0))

pid = os.fork()
if pid == 0:
    os.close(master)
    devnull = os.open(os.devnull, os.O_RDWR)
    os.dup2(devnull, 0)
    os.dup2(slave if mode == "terminal" else devnull, 1)
    os.dup2(slave, 2)
    os.close(slave)
    os.close(devnull)
    os.execv("/bin/sh", ["/bin/sh", "-c", command])
    os._exit(127)

os.close(slave)

data = b""
while True:
    r, _, _ = select.select([master], [], [], 300.0)
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
erases = len(re.findall(rb"\r\x1b\[K", data))

if redraws == 0:
    print(f"{mode}: FAIL: no progress was rendered")
elif mode == "piped" and erases > 5:
    print(f"{mode}: FAIL: the progress bar flickers: {erases} erase sequences for {redraws} redraws")
elif mode == "terminal" and erases < 5:
    print(f"{mode}: FAIL: the progress bar is not cleared before the data: {erases} erase sequences for {redraws} redraws")
else:
    print(f"{mode}: OK")
EOF

# 3 million rows in blocks of 100000: every flushed block used to erase and
# redraw the bar, so the erase count tells whether the bar flickered.
python3 "${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_pty.py" piped \
    "$CLICKHOUSE_LOCAL --progress --format TSV --query 'SELECT * FROM numbers(3000000) SETTINGS max_block_size = 100000'"

# When the data goes to the terminal, the bar must be cleared before every
# data block (fewer rows: the data itself goes through the pty).
python3 "${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_pty.py" terminal \
    "$CLICKHOUSE_LOCAL --progress --format TSV --query 'SELECT * FROM numbers(200000) SETTINGS max_block_size = 10000'"

rm "${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_pty.py"
