#!/usr/bin/env bash
#
# record-demo.sh — Record and convert iOS app demo GIFs for README
#
# Usage:
#   ./scripts/record-demo.sh              # Interactive: record all 5 scenes
#   ./scripts/record-demo.sh 3            # Record only scene 3
#   ./scripts/record-demo.sh convert X.mov 01-bubbles.gif  # Convert a .mov directly
#
set -euo pipefail

DEMO_DIR="docs/demo"
GIF_WIDTH=320
GIF_FPS=15
BOOTED_DEVICE=""

# ── Scene definitions ────────────────────────────────────────────────────────

declare -a SCENE_NAMES=(
    "01-bubbles"
    "02-sessions"
    "03-dates"
    "04-watchlist"
    "05-detail"
)

declare -a SCENE_TITLES=(
    "Bubble Chart Overview"
    "Session Navigation"
    "Date Navigation"
    "Watchlist Interactions"
    "Detail View"
)

declare -a SCENE_INSTRUCTIONS=(
    "Launch the app and wait for bubbles to settle.
  Show the tier-colored rings (green/yellow/red) and close dials.
  Let the physics simulation run for ~5 seconds."

    "Starting from PRE mode, swipe UP through each session:
  PRE (indigo) → REG (forest) → DAY (black) → NEXT (maroon).
  Pause briefly on each to show the background change."

    "Swipe LEFT a few times to go back in history.
  Then swipe RIGHT to return to live.
  Show the date changing in the toolbar."

    "Tap a bubble to add it to watchlist (becomes purple square).
  Tap another. Pinch to filter watchlist-only.
  Shake to clear all watchlist symbols."

    "Long-press a bubble to open the detail view.
  Show the session cards (OHLC, trades, turnover).
  Scroll down to see news and history chart."
)

# ── Helpers ──────────────────────────────────────────────────────────────────

has_cmd() { command -v "$1" &>/dev/null; }

check_ffmpeg() {
    if has_cmd ffmpeg; then
        return
    fi
    echo "ffmpeg not found. Installing via Homebrew..."
    if ! has_cmd brew; then
        echo "Error: Homebrew not installed. Install ffmpeg manually."
        exit 1
    fi
    brew install ffmpeg
}

detect_simulator() {
    if ! has_cmd xcrun; then
        return 1
    fi
    BOOTED_DEVICE=$(xcrun simctl list devices booted -j 2>/dev/null \
        | python3 -c "
import sys, json
data = json.load(sys.stdin)
for runtime, devices in data.get('devices', {}).items():
    for d in devices:
        if d.get('state') == 'Booted':
            print(d['udid'])
            sys.exit(0)
sys.exit(1)
" 2>/dev/null) || return 1
    [ -n "$BOOTED_DEVICE" ]
}

# Convert .mov to optimized GIF using ffmpeg 2-pass palette method
convert_to_gif() {
    local input="$1"
    local output="$2"

    if [ ! -f "$input" ]; then
        echo "Error: input file not found: $input"
        return 1
    fi

    echo "Converting: $input → $output"
    echo "  Settings: ${GIF_WIDTH}px wide, ${GIF_FPS}fps, optimized palette"

    local palette="/tmp/demo-palette.png"
    local filters="fps=${GIF_FPS},scale=${GIF_WIDTH}:-1:flags=lanczos"

    # Pass 1: generate palette
    ffmpeg -y -i "$input" \
        -vf "${filters},palettegen=stats_mode=diff" \
        "$palette" \
        -hide_banner -loglevel warning

    # Pass 2: encode GIF with palette
    ffmpeg -y -i "$input" -i "$palette" \
        -lavfi "${filters} [x]; [x][1:v] paletteuse=dither=bayer:bayer_scale=5:diff_mode=rectangle" \
        "$output" \
        -hide_banner -loglevel warning

    rm -f "$palette"

    local size
    size=$(stat -f%z "$output" 2>/dev/null || stat --printf="%s" "$output" 2>/dev/null)
    local size_mb
    size_mb=$(echo "scale=1; $size / 1048576" | bc)
    echo "  Output: $output (${size_mb}MB)"

    if (( $(echo "$size > 10485760" | bc -l) )); then
        echo "  Warning: GIF exceeds 10MB. Consider a shorter recording."
    fi
}

# Record from simulator
record_simulator() {
    local scene_name="$1"
    local mov_file="/tmp/${scene_name}.mov"

    echo ""
    echo "Recording from simulator (device: $BOOTED_DEVICE)..."
    echo "Press ENTER to start recording."
    read -r

    xcrun simctl io "$BOOTED_DEVICE" recordVideo --codec=h264 "$mov_file" &
    local rec_pid=$!

    echo "Recording... Press ENTER to stop."
    read -r

    kill -INT "$rec_pid" 2>/dev/null
    wait "$rec_pid" 2>/dev/null || true
    sleep 1

    echo "$mov_file"
}

# Prompt for a .mov file path
prompt_for_file() {
    local scene_name="$1"
    echo ""
    echo "No simulator detected. Provide a pre-recorded .mov file."
    echo "  (Record your iPhone screen, AirDrop the .mov, then paste the path)"
    echo ""
    read -rp "Path to .mov file: " mov_path

    # Strip quotes if user wraps path in them
    mov_path="${mov_path//\"/}"
    mov_path="${mov_path//\'/}"

    if [ ! -f "$mov_path" ]; then
        echo "Error: file not found: $mov_path"
        return 1
    fi

    echo "$mov_path"
}

# ── Record a single scene ───────────────────────────────────────────────────

record_scene() {
    local idx="$1"
    local name="${SCENE_NAMES[$idx]}"
    local title="${SCENE_TITLES[$idx]}"
    local instructions="${SCENE_INSTRUCTIONS[$idx]}"
    local gif_file="${DEMO_DIR}/${name}.gif"

    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  Scene $((idx + 1))/5: $title"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo ""
    echo "Instructions:"
    echo "$instructions"
    echo ""

    local mov_file
    if [ -n "$BOOTED_DEVICE" ]; then
        mov_file=$(record_simulator "$name")
    else
        mov_file=$(prompt_for_file "$name") || return 1
    fi

    convert_to_gif "$mov_file" "$gif_file"
}

# ── Main ─────────────────────────────────────────────────────────────────────

main() {
    # Direct conversion mode
    if [ "${1:-}" = "convert" ]; then
        if [ $# -lt 3 ]; then
            echo "Usage: $0 convert <input.mov> <output.gif>"
            exit 1
        fi
        check_ffmpeg
        mkdir -p "$DEMO_DIR"
        convert_to_gif "$2" "$3"
        exit 0
    fi

    echo "Jupitor iOS App — Demo GIF Recorder"
    echo ""

    check_ffmpeg
    mkdir -p "$DEMO_DIR"

    if detect_simulator; then
        echo "Simulator detected: $BOOTED_DEVICE"
    else
        echo "No booted simulator found. Will prompt for .mov files."
    fi

    # Single scene or all scenes
    if [ $# -ge 1 ]; then
        local scene_num="$1"
        if (( scene_num < 1 || scene_num > 5 )); then
            echo "Error: scene number must be 1-5"
            exit 1
        fi
        record_scene $((scene_num - 1))
    else
        for i in 0 1 2 3 4; do
            record_scene "$i"
            if [ "$i" -lt 4 ]; then
                echo ""
                read -rp "Press ENTER for next scene (or Ctrl-C to stop)... "
            fi
        done
    fi

    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  Done! GIFs saved to ${DEMO_DIR}/"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    ls -lh "${DEMO_DIR}"/*.gif 2>/dev/null || echo "  (no GIFs generated yet)"
}

main "$@"
