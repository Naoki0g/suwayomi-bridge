#!/system/bin/sh
# Suwayomi Multi-Extension Proxy + Reverse SSH Tunnel Daemon for Android (/data/local/tmp/suwayomi-proxy)

BASE="/data/local/tmp/suwayomi-proxy"
ROOT="$BASE/root"
export PATH="$ROOT/usr/bin:$ROOT/bin:/system/bin:/system/xbin"
export HOME="$BASE"
export TMPDIR="$BASE/tmp"
export PROXY_CACHE_DIR="$BASE/cache"
export NO_PROXY="1"
export SSL_CERT_FILE="$ROOT/etc/ssl/cert.pem"
export CURL_CA_BUNDLE="$ROOT/etc/ssl/cert.pem"
export PYTHONUNBUFFERED="1"

if [ -f "$BASE/.env" ]; then
    set -a
    . "$BASE/.env"
    set +a
fi

VPS_USER="${VPS_USER:-usuario}"
VPS_HOST="${VPS_HOST:-SEU_IP_DA_VPS}"
VPS_SSH_KEY="${VPS_SSH_KEY:-$BASE/ssh-key.key}"
VPS_DOCKER_BRIDGE="${VPS_DOCKER_BRIDGE:-172.19.0.1}"
VPS_TUNNEL_PORT="${VPS_TUNNEL_PORT:-18192}"
VPS_WARP_IP="${VPS_WARP_IP:-172.19.0.5}"
VPS_WARP_PORT="${VPS_WARP_PORT:-1080}"

mkdir -p "$TMPDIR" "$PROXY_CACHE_DIR" "$BASE/logs"

ensure_runtime_files() {
    if [ ! -s /sdcard/dns.conf ]; then
        printf "nameserver 1.1.1.1\nnameserver 8.8.8.8\n" > /sdcard/dns.conf 2>/dev/null
    fi
    if [ ! -s /sdcard/pwd ]; then
        printf "root:x:0:0:root:%s:/system/bin/sh\nshell:x:2000:2000:shell:%s:/system/bin/sh\n" "$BASE" "$BASE" > /sdcard/pwd 2>/dev/null
    fi
    chmod 600 "$VPS_SSH_KEY" 2>/dev/null
    dumpsys deviceidle disable >/dev/null 2>&1
}

proxy_loop() {
    while true; do
        ensure_runtime_files
        echo "[$(date)] Starting proxy service on 127.0.0.1:8192..." >> "$BASE/logs/proxy.log"
        SERVICE_SCRIPT="$BASE/proxy_service.py"
        cd "$BASE" && "$ROOT/usr/bin/python3" "$SERVICE_SCRIPT" >> "$BASE/logs/proxy.log" 2>&1
        echo "[$(date)] Proxy service exited ($?), restarting in 2s..." >> "$BASE/logs/proxy.log"
        sleep 2
    done
}

tunnel_loop() {
    while true; do
        ensure_runtime_files
        echo "[$(date)] Starting reverse SSH tunnel to ${VPS_HOST}..." >> "$BASE/logs/tunnel.log"
        # Clean up any stale tunnel listener on the VPS before binding
        "$ROOT/usr/bin/ssh" -F /dev/null \
            -o ConnectTimeout=10 \
            -o StrictHostKeyChecking=no \
            -o UserKnownHostsFile="$BASE/known_hosts" \
            -i "$VPS_SSH_KEY" \
            "${VPS_USER}@${VPS_HOST}" \
            "sudo fuser -k ${VPS_TUNNEL_PORT}/tcp >/dev/null 2>&1 || true" >> "$BASE/logs/tunnel.log" 2>&1

        "$ROOT/usr/bin/ssh" -F /dev/null -N -T \
            -o ConnectTimeout=15 \
            -o ServerAliveInterval=15 \
            -o ServerAliveCountMax=3 \
            -o ExitOnForwardFailure=yes \
            -o StrictHostKeyChecking=no \
            -o UserKnownHostsFile="$BASE/known_hosts" \
            -i "$VPS_SSH_KEY" \
            -R "${VPS_DOCKER_BRIDGE}:${VPS_TUNNEL_PORT}:127.0.0.1:8192" \
            -L "127.0.0.1:11080:${VPS_WARP_IP}:${VPS_WARP_PORT}" \
            "${VPS_USER}@${VPS_HOST}" >> "$BASE/logs/tunnel.log" 2>&1
        echo "[$(date)] SSH tunnel exited ($?), reconnecting in 3s..." >> "$BASE/logs/tunnel.log"
        sleep 3
    done
}

stop_all() {
    if [ -f "$BASE/proxy_loop.pid" ]; then
        kill "$(cat "$BASE/proxy_loop.pid")" 2>/dev/null
        rm -f "$BASE/proxy_loop.pid"
    fi
    if [ -f "$BASE/tunnel_loop.pid" ]; then
        kill "$(cat "$BASE/tunnel_loop.pid")" 2>/dev/null
        rm -f "$BASE/tunnel_loop.pid"
    fi
    pkill -f "$BASE/proxy_service.py" 2>/dev/null
    pkill -f ":${VPS_TUNNEL_PORT}:127.0.0.1:8192" 2>/dev/null
    pkill -f "$BASE/phone_daemon.sh _proxy_loop" 2>/dev/null
    pkill -f "$BASE/phone_daemon.sh _tunnel_loop" 2>/dev/null
}

start_all() {
    stop_all
    ensure_runtime_files
    # Keep Wi-Fi, CPU, and background network active when screen is off
    settings put global wifi_sleep_policy 2 >/dev/null 2>&1
    settings put global wifi_power_save 0 >/dev/null 2>&1
    settings put global low_power 0 >/dev/null 2>&1
    settings put global low_power_back_data_off 0 >/dev/null 2>&1
    settings put global psm_network_power_saving "0,0" >/dev/null 2>&1
    settings put global adb_allowed_connection_time 0 >/dev/null 2>&1
    settings put global settings_enable_monitor_phantom_procs false >/dev/null 2>&1
    settings put secure block_usb_lock 0 >/dev/null 2>&1
    dumpsys deviceidle whitelist +com.android.shell >/dev/null 2>&1
    dumpsys deviceidle disable >/dev/null 2>&1

    nohup /system/bin/sh "$BASE/phone_daemon.sh" _proxy_loop >/dev/null 2>&1 &
    echo $! > "$BASE/proxy_loop.pid"

    nohup /system/bin/sh "$BASE/phone_daemon.sh" _tunnel_loop >/dev/null 2>&1 &
    echo $! > "$BASE/tunnel_loop.pid"

    echo "Started proxy_loop (PID $(cat "$BASE/proxy_loop.pid")) and tunnel_loop (PID $(cat "$BASE/tunnel_loop.pid"))"
}

status_all() {
    echo "=== Processes on Phone ==="
    ps -ef | grep -E "proxy_service|${VPS_TUNNEL_PORT}|phone_daemon" | grep -v grep
    echo "=== Local Health Check ==="
    "$ROOT/usr/bin/curl" -fsS http://127.0.0.1:8192/health 2>/dev/null || echo "Health check failed"
    echo ""
}

case "$1" in
    _proxy_loop)
        proxy_loop
        ;;
    _tunnel_loop)
        tunnel_loop
        ;;
    start)
        start_all
        ;;
    stop)
        stop_all
        echo "Stopped proxy and tunnel on phone."
        ;;
    restart)
        start_all
        ;;
    status)
        status_all
        ;;
    logs)
        tail -n 30 "$BASE/logs/proxy.log" "$BASE/logs/tunnel.log"
        ;;
    *)
        echo "Usage: $0 {start|stop|restart|status|logs}"
        exit 1
        ;;
esac
