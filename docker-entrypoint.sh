#!/bin/sh

# Ensure the X11 socket directory exists and has correct permissions
# This is done as root before switching to appuser
mkdir -p /tmp/.X11-unix
chmod 1777 /tmp/.X11-unix
chown appuser:appuser /tmp/.X11-unix

# Start Xvfb and x11vnc in the background
Xvfb :99 -screen 0 1280x720x24 -ac &
x11vnc -display :99 -nopw -forever -shared -rfbport 5900 -quiet &

# Execute the main command passed to the entrypoint
exec "$@"
