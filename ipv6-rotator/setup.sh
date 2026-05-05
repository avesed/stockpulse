#!/usr/bin/env bash
#
# setup.sh — Add IPv6 addresses from the rotator pool to the system.
#
# Usage:
#   ./setup.sh --prefix 2607:fea8:5765:bc01::/64 \
#              --interface enp1s0.5 \
#              --gateway fe80::1 \
#              --pool-size 200
#
# What it does:
#   1. Creates addresses via ipv6-rotator --setup (or reads from stdin)
#   2. Adds a policy routing rule so outgoing traffic from the prefix
#      uses the correct interface and gateway
#
# Run as root.

set -euo pipefail

PREFIX=""
INTERFACE=""
GATEWAY=""
POOL_SIZE=200
TABLE=100
PRIO=100

usage() {
    echo "Usage: $0 --prefix <cidr> --interface <iface> --gateway <gw_ll> [--pool-size N] [--table N] [--prio N]"
    exit 1
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --prefix)     PREFIX="$2";    shift 2 ;;
        --interface)  INTERFACE="$2"; shift 2 ;;
        --gateway)    GATEWAY="$2";   shift 2 ;;
        --pool-size)  POOL_SIZE="$2"; shift 2 ;;
        --table)      TABLE="$2";     shift 2 ;;
        --prio)       PRIO="$2";      shift 2 ;;
        *)            usage ;;
    esac
done

if [[ -z "$PREFIX" || -z "$INTERFACE" || -z "$GATEWAY" ]]; then
    usage
fi

echo "=== IPv6 Rotator Setup ==="
echo "prefix:     $PREFIX"
echo "interface:  $INTERFACE"
echo "gateway:    $GATEWAY"
echo "pool size:  $POOL_SIZE"
echo "table:      $TABLE"
echo ""

# Step 1: Generate addresses and add them to the interface
echo "--- Generating and adding IPv6 addresses ---"

# Run the rotator binary to get addresses (it prints them to stdout).
# If the binary is available, use it; otherwise generate with a simple loop.
if command -v ipv6-rotator &>/dev/null; then
    ADDRS=$(ipv6-rotator --prefix "$PREFIX" --pool-size "$POOL_SIZE" --interface "$INTERFACE" --setup 2>/dev/null)
    echo "Added $POOL_SIZE addresses via ipv6-rotator --setup"
else
    echo "ipv6-rotator binary not found; adding addresses manually..."
    # Extract base prefix (without /NN) for Python one-liner generation
    BASE="${PREFIX%%/*}"
    BITS="${PREFIX##*/}"

    # Generate random addresses with Python and add them
    python3 -c "
import random, ipaddress, sys
net = ipaddress.IPv6Network('${PREFIX}', strict=False)
prefix_int = int(net.network_address)
host_bits = 128 - net.prefixlen
for _ in range(${POOL_SIZE}):
    suffix = random.getrandbits(host_bits) or 1
    addr = ipaddress.IPv6Address(prefix_int | suffix)
    print(addr)
" | while IFS= read -r addr; do
        ip -6 addr add "${addr}/128" dev "$INTERFACE" 2>/dev/null || true
        echo "  added $addr"
    done
fi

# Step 2: Set up policy routing
echo ""
echo "--- Configuring policy routing ---"

# Add the routing table entry (default route via gateway on the interface)
if ! ip -6 route show table "$TABLE" | grep -q "default"; then
    ip -6 route add default via "$GATEWAY" dev "$INTERFACE" table "$TABLE"
    echo "Added default route via $GATEWAY dev $INTERFACE table $TABLE"
else
    echo "Default route in table $TABLE already exists, skipping"
fi

# Add the rule: traffic from this prefix uses our table
if ! ip -6 rule show | grep -q "from $PREFIX"; then
    ip -6 rule add from "$PREFIX" table "$TABLE" prio "$PRIO"
    echo "Added rule: from $PREFIX -> table $TABLE (prio $PRIO)"
else
    echo "Rule for $PREFIX already exists, skipping"
fi

echo ""
echo "=== Setup complete ==="
echo ""
echo "Start the proxy:"
echo "  ipv6-rotator --prefix $PREFIX --interface $INTERFACE --port 1080 --pool-size $POOL_SIZE"
echo ""
echo "Test:"
echo "  curl -6 --proxy http://127.0.0.1:1080 https://ifconfig.co"
