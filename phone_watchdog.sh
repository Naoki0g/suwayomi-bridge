#!/usr/bin/env bash
# Watchdog on PC to ensure the Android phone proxy & tunnel are always running
# whenever the phone is connected via ADB (e.g. after PC suspend/resume USB bus resets).

set -u

PHONE_DAEMON="/data/local/tmp/suwayomi-proxy/phone_daemon.sh"
PHONE_CURL="/data/local/tmp/suwayomi-proxy/root/usr/bin/curl"

while true; do
    if adb get-state >/dev/null 2>&1; then
        health=$(adb shell "$PHONE_CURL -fsS --max-time 4 http://127.0.0.1:8192/health 2>/dev/null" 2>/dev/null | tr -d '\r\n')
        tunnel_ok=$(adb shell "ps -ef | grep '172.19.0.1:18192' | grep -v grep" 2>/dev/null)
        if [ "$health" != "OK" ] || [ -z "$tunnel_ok" ]; then
            echo "[$(date -Is)] Phone proxy/tunnel not healthy (health='$health', tunnel='${tunnel_ok:+up}'), starting phone_daemon.sh..."
            adb shell "$PHONE_DAEMON start"
        fi
    fi
    sleep 15
done
