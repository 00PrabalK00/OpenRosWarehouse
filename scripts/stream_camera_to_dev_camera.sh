#!/usr/bin/env bash
set -euo pipefail

SOURCE_DEVICE="${SOURCE_DEVICE:-}"
SOURCE_BY_ID_DEFAULT="/dev/v4l/by-id/usb-046d_C270_HD_WEBCAM_F6E57210-video-index0"
LOOPBACK_DEVICE="${LOOPBACK_DEVICE:-/dev/video20}"
ALIAS_PATH="${ALIAS_PATH:-/dev/camera}"
CARD_LABEL="${CARD_LABEL:-camera}"
WIDTH="${WIDTH:-640}"
HEIGHT="${HEIGHT:-480}"
FPS="${FPS:-30}"

usage() {
  cat <<'EOF'
Usage:
  scripts/stream_camera_to_dev_camera.sh [source-device]

Environment overrides:
  SOURCE_DEVICE    Physical V4L2 source device to read from
  LOOPBACK_DEVICE  Virtual V4L2 device to publish into (default: /dev/video20)
  ALIAS_PATH       Symlink path for the virtual camera (default: /dev/camera)
  CARD_LABEL       v4l2loopback card label (default: camera)
  WIDTH            Output width (default: 640)
  HEIGHT           Output height (default: 480)
  FPS              Output fps (default: 30)

Examples:
  scripts/stream_camera_to_dev_camera.sh
  SOURCE_DEVICE=/dev/video0 LOOPBACK_DEVICE=/dev/video30 scripts/stream_camera_to_dev_camera.sh
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ $# -gt 0 ]]; then
  SOURCE_DEVICE="$1"
fi

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

run_as_root() {
  if [[ "$(id -u)" -eq 0 ]]; then
    "$@"
  else
    sudo "$@"
  fi
}

detect_source_device() {
  if [[ -n "$SOURCE_DEVICE" ]]; then
    printf '%s\n' "$SOURCE_DEVICE"
    return
  fi

  if [[ -e "$SOURCE_BY_ID_DEFAULT" ]]; then
    printf '%s\n' "$SOURCE_BY_ID_DEFAULT"
    return
  fi

  for dev in /dev/video*; do
    [[ -e "$dev" ]] || continue
    if udevadm info --name="$dev" 2>/dev/null | grep -q 'ID_V4L_CAPABILITIES=:capture:'; then
      printf '%s\n' "$dev"
      return
    fi
  done

  echo "Could not find a capture-capable webcam device." >&2
  exit 1
}

require_cmd gst-launch-1.0
require_cmd modprobe
require_cmd udevadm

SOURCE_DEVICE="$(detect_source_device)"

if [[ ! -e "$SOURCE_DEVICE" ]]; then
  echo "Source device does not exist: $SOURCE_DEVICE" >&2
  exit 1
fi

VIDEO_NR="${LOOPBACK_DEVICE#/dev/video}"
if [[ "$VIDEO_NR" == "$LOOPBACK_DEVICE" || ! "$VIDEO_NR" =~ ^[0-9]+$ ]]; then
  echo "LOOPBACK_DEVICE must look like /dev/videoN, got: $LOOPBACK_DEVICE" >&2
  exit 1
fi

if [[ ! -e "$LOOPBACK_DEVICE" ]]; then
  run_as_root modprobe v4l2loopback \
    video_nr="$VIDEO_NR" \
    card_label="$CARD_LABEL" \
    exclusive_caps=1
fi

if [[ ! -e "$LOOPBACK_DEVICE" ]]; then
  cat >&2 <<EOF
Failed to create $LOOPBACK_DEVICE.
If v4l2loopback is already loaded with different options, unload it first:
  sudo modprobe -r v4l2loopback
Then re-run this script.
EOF
  exit 1
fi

run_as_root ln -sfn "$LOOPBACK_DEVICE" "$ALIAS_PATH"

echo "Streaming $SOURCE_DEVICE -> $LOOPBACK_DEVICE"
echo "Alias: $ALIAS_PATH -> $LOOPBACK_DEVICE"
echo "Press Ctrl+C to stop."

exec gst-launch-1.0 -e \
  v4l2src device="$SOURCE_DEVICE" do-timestamp=true ! \
  image/jpeg,width="$WIDTH",height="$HEIGHT",framerate="$FPS"/1 ! \
  jpegdec ! \
  videoconvert ! \
  video/x-raw,format=YUY2,width="$WIDTH",height="$HEIGHT",framerate="$FPS"/1 ! \
  v4l2sink device="$LOOPBACK_DEVICE" sync=false
