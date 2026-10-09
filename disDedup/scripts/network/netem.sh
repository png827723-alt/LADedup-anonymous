#!/usr/bin/env sh
set -eu

IFACE="${NET_IFACE:-eth0}"
RATE="${NET_RATE:-}"           # e.g. 100mbit
DELAY="${NET_DELAY:-}"         # e.g. 20ms
JITTER="${NET_JITTER:-0ms}"    # e.g. 5ms
LOSS="${NET_LOSS:-0%}"         # e.g. 0.1%
BURST="${NET_BURST:-32kbit}"   # for tbf
LATENCY="${NET_LATENCY:-400ms}"

is_valid_time() {
  echo "$1" | grep -Eq '^[0-9]+([.][0-9]+)?(us|ms|s)$'
}

is_zero_time() {
  case "$1" in
    0|0us|0ms|0s|0.0us|0.0ms|0.0s)
      return 0
      ;;
  esac
  return 1
}

echo "[netem] iface=$IFACE rate=$RATE delay=$DELAY jitter=$JITTER loss=$LOSS"

# 清理旧规则（重复启动不会叠加）
tc qdisc del dev "$IFACE" root 2>/dev/null || true

# 1) 先做限速（tbf），再叠加延迟/丢包（netem）
if [ -n "$RATE" ]; then
  tc qdisc add dev "$IFACE" root handle 1: tbf rate "$RATE" burst "$BURST" latency "$LATENCY"
  PARENT="parent 1:1"
else
  # 不限速就直接在 root 上挂 netem
  PARENT="root"
fi

# 2) netem：延迟/抖动/丢包（按需启用）
if [ -n "$DELAY" ] || [ "$LOSS" != "0%" ]; then
  # 组装 netem 参数
  NETEM_ARGS=""
  if [ -n "$DELAY" ]; then
    if is_valid_time "$JITTER" && ! is_zero_time "$JITTER"; then
      NETEM_ARGS="$NETEM_ARGS delay $DELAY $JITTER distribution normal"
    else
      NETEM_ARGS="$NETEM_ARGS delay $DELAY"
    fi
  fi
  if [ "$LOSS" != "0%" ]; then
    NETEM_ARGS="$NETEM_ARGS loss $LOSS"
  fi

  tc qdisc add dev "$IFACE" ${PARENT} handle 10: netem $NETEM_ARGS
fi

echo "[netem] applied."
