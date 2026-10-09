#!/usr/bin/env sh
set -eu

IFACE="${NET_IFACE:-eth0}"
BURST="${NET_BURST:-64kbit}"

# manager -> cloud
M2C_TARGETS="${M2C_TARGETS:-cloud}"
M2C_RATE="${M2C_RATE:-80mbit}"
M2C_DELAY="${M2C_DELAY:-40ms}"
M2C_JITTER="${M2C_JITTER:-5ms}"
M2C_LOSS="${M2C_LOSS:-0%}"

# manager -> edge
M2E_TARGETS="${M2E_TARGETS:-edge1 edge2 edge3 edge4 edge5 edge6 edge7 edge8 edge9 edge10}"
M2E_RATE="${M2E_RATE:-500mbit}"
M2E_DELAY="${M2E_DELAY:-5ms}"
M2E_JITTER="${M2E_JITTER:-1ms}"
M2E_LOSS="${M2E_LOSS:-0%}"

DEFAULT_RATE="${DEFAULT_RATE:-10gbit}"

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

resolve_to_cidr() {
  target="$1"

  # CIDR
  case "$target" in
    */*) echo "$target"; return 0 ;;
  esac

  # host:port -> host
  host="$target"
  case "$host" in
    *:*)
      host="${host%%:*}"
      ;;
  esac

  # IPv4 literal
  case "$host" in
    *.*.*.*)
      echo "$host/32"
      return 0
      ;;
  esac

  if command -v getent >/dev/null 2>&1; then
    ip="$(getent ahostsv4 "$host" | awk 'NR==1{print $1}')"
    if [ -n "${ip:-}" ]; then
      echo "$ip/32"
      return 0
    fi
  fi

  return 1
}

add_class_with_netem() {
  classid="$1"
  rate="$2"
  delay="$3"
  jitter="$4"
  loss="$5"

  tc class add dev "$IFACE" parent 1: classid "$classid" htb rate "$rate" ceil "$rate" burst "$BURST"

  netem_args=""
  if [ -n "$delay" ]; then
    if is_valid_time "$jitter" && ! is_zero_time "$jitter"; then
      netem_args="$netem_args delay $delay $jitter distribution normal"
    else
      netem_args="$netem_args delay $delay"
    fi
  fi
  if [ "$loss" != "0%" ]; then
    netem_args="$netem_args loss $loss"
  fi

  if [ -n "$netem_args" ]; then
    # shellcheck disable=SC2086
    tc qdisc add dev "$IFACE" parent "$classid" handle "${classid#1:}0:" netem $netem_args
  fi
}

echo "[netem-manager] iface=$IFACE m2c=($M2C_RATE,$M2C_DELAY,$M2C_JITTER,$M2C_LOSS) m2e=($M2E_RATE,$M2E_DELAY,$M2E_JITTER,$M2E_LOSS)"

# clear existing qdisc; avoid stacking across restarts
tc qdisc del dev "$IFACE" root 2>/dev/null || true

# root scheduler
tc qdisc add dev "$IFACE" root handle 1: htb default 30

# class 1:10 for manager->cloud, 1:20 for manager->edge, 1:30 default
add_class_with_netem 1:10 "$M2C_RATE" "$M2C_DELAY" "$M2C_JITTER" "$M2C_LOSS"
add_class_with_netem 1:20 "$M2E_RATE" "$M2E_DELAY" "$M2E_JITTER" "$M2E_LOSS"
tc class add dev "$IFACE" parent 1: classid 1:30 htb rate "$DEFAULT_RATE" ceil "$DEFAULT_RATE" burst "$BURST"

# classify manager->cloud traffic
for t in $M2C_TARGETS; do
  if cidr="$(resolve_to_cidr "$t" 2>/dev/null)"; then
    tc filter add dev "$IFACE" protocol ip parent 1: prio 10 u32 match ip dst "$cidr" flowid 1:10
  else
    echo "[netem-manager][WARN] unable to resolve cloud target: $t"
  fi
done

# classify manager->edge traffic
for t in $M2E_TARGETS; do
  if cidr="$(resolve_to_cidr "$t" 2>/dev/null)"; then
    tc filter add dev "$IFACE" protocol ip parent 1: prio 20 u32 match ip dst "$cidr" flowid 1:20
  else
    echo "[netem-manager][WARN] unable to resolve edge target: $t"
  fi
done

echo "[netem-manager] applied."
