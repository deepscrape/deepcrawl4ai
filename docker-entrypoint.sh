#!/bin/sh
set -eu

# Initialize NOVNC_PID at the top of the script
NOVNC_PID=""

# Ensure the X11 socket directory exists and has correct permissions
# This is done as root before switching to appuser
mkdir -p /tmp/.X11-unix
chmod 1777 /tmp/.X11-unix
chown appuser:appuser /tmp/.X11-unix

# Start Xvfb in the background
Xvfb :99 -screen 0 1280x720x24 -ac &
XVFB_PID=$!

# Setup VNC authentication
# Optional: set VNC_PASSWORD via env; default to disabling if missing in production
if [ -n "${VNC_PASSWORD:-}" ]; then
  mkdir -p "$HOME/.vnc"
  x11vnc -storepasswd "$VNC_PASSWORD" "$HOME/.vnc/passwd" >/dev/null 2>&1
  AUTH_ARGS="-rfbauth $HOME/.vnc/passwd"
else
  # In dev only; never leave unauthenticated in production
  AUTH_ARGS="-nopw"
fi

# Start x11vnc with auth in the background
# Bind to localhost; expose only via websockify
x11vnc -display :99 ${AUTH_ARGS} -forever -shared -rfbport 5900 -localhost -quiet &
X11VNC_PID=$!

# Define cleanup function
cleanup() {
  echo "Shutting down background processes..."
  
  # Kill processes with proper signal handling
  if [ -n "$XVFB_PID" ]; then
    kill -TERM $XVFB_PID 2>/dev/null || true
  fi
  
  if [ -n "$X11VNC_PID" ]; then
    kill -TERM $X11VNC_PID 2>/dev/null || true
  fi
  
  if [ -n "$NOVNC_PID" ]; then
    kill -TERM $NOVNC_PID 2>/dev/null || true
  fi
  
  # Wait for processes to terminate
  wait $XVFB_PID 2>/dev/null || true
  wait $X11VNC_PID 2>/dev/null || true
  [ -n "$NOVNC_PID" ] && wait $NOVNC_PID 2>/dev/null || true
  
  echo "Cleanup complete."
}

# Set up signal trapping
trap cleanup INT TERM QUIT

# Export DISPLAY for child processes
export DISPLAY=:99

# Log successful setup
echo "✅ X11 environment setup complete. Display: $DISPLAY"

# Check if this is a worker process
if echo "$@" | grep -q "celery"; then
  echo "Running as worker process, starting minimal X11 services"
  # Skip starting noVNC for worker processes
else
  echo "Running as app process, starting all services"
  # Start noVNC only for app process
  if [ -d "/opt/noVNC" ]; then
    /opt/noVNC/utils/websockify/run --web /opt/noVNC 0.0.0.0:6080 localhost:5900 &
    NOVNC_PID=$!
  else
    NOVNC_PID=""
  fi
fi

# Execute the main command passed to the entrypoint
exec "$@"
