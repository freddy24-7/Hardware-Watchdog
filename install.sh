#!/usr/bin/env bash
set -euo pipefail

INGEST_URL="https://ingest-production-0c1b.up.railway.app/ingest"
GRAFANA_URL="https://grafana-production-184a.up.railway.app"
REPO="freddy24-7/Hardware-Watchdog"
INSTALL_DIR="/usr/local/bin"
BINARY_NAME="hw-watchdog-agent"
ID_FILE="$HOME/.hw-watchdog-id"

# ── Detect platform ────────────────────────────────────────────────────────────
OS="$(uname -s)"
ARCH="$(uname -m)"

case "$OS/$ARCH" in
  Darwin/arm64)  ASSET="agent-aarch64-apple-darwin" ;;
  Darwin/x86_64) ASSET="agent-x86_64-apple-darwin" ;;
  Linux/x86_64)  ASSET="agent-x86_64-unknown-linux-gnu" ;;
  *)
    echo "Unsupported platform: $OS/$ARCH"
    echo "Supported: macOS Apple Silicon, macOS Intel, Linux x86_64"
    exit 1
    ;;
esac

# ── Find latest release ────────────────────────────────────────────────────────
echo "Fetching latest release..."
LATEST=$(curl -fsSL "https://api.github.com/repos/$REPO/releases/latest" \
  | grep '"tag_name"' | sed 's/.*"tag_name": *"\(.*\)".*/\1/')

DOWNLOAD_URL="https://github.com/$REPO/releases/download/$LATEST/$ASSET"

# ── Download binary ────────────────────────────────────────────────────────────
TMP="$(mktemp)"
echo "Downloading $ASSET ($LATEST)..."
curl -fsSL "$DOWNLOAD_URL" -o "$TMP"
chmod +x "$TMP"

# ── Install ────────────────────────────────────────────────────────────────────
if [ -w "$INSTALL_DIR" ]; then
  mv "$TMP" "$INSTALL_DIR/$BINARY_NAME"
else
  sudo mv "$TMP" "$INSTALL_DIR/$BINARY_NAME"
fi

echo "Installed to $INSTALL_DIR/$BINARY_NAME"

# ── Generate machine ID if not present ────────────────────────────────────────
if [ ! -f "$ID_FILE" ]; then
  if command -v uuidgen &>/dev/null; then
    uuidgen | tr '[:upper:]' '[:lower:]' > "$ID_FILE"
  else
    cat /proc/sys/kernel/random/uuid > "$ID_FILE" 2>/dev/null || \
      python3 -c "import uuid; print(uuid.uuid4())" > "$ID_FILE"
  fi
fi
MACHINE_ID="$(cat "$ID_FILE")"

# ── Print instructions ─────────────────────────────────────────────────────────
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Hardware Watchdog installed!"
echo ""
echo "  Start the agent:"
echo "    INGEST_URL=$INGEST_URL $BINARY_NAME"
echo ""
echo "  Your personal dashboard:"
echo "  $GRAFANA_URL/d/hw-watchdog-v1?var-machine_id=$MACHINE_ID"
echo ""
echo "  Keep the agent running to see live data."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
