#!/bin/bash

set -e

OP=$1
DEV=$2
IP=$3
TCP_PORT=$4
DELAY=$5

if [[ "$OP" == "flush" && ! -z "$DEV" ]]; then
    DELAY="dont_complain"
fi

if [[ -z "$DELAY" ]]; then
	echo "Usage: $0 enable|disable DEV PEER_IP TCP_PORT DELAY_MS"
	echo "       $0 flush DEV"
	echo ""
	echo "Introduce latency for a peer's use of a local service."
	echo "This script should be run on the server-side, i.e., the host listening on TCP_PORT."
	echo ""
	echo "Examples:"
	echo "  # Add 120ms latency for peer 'worker2' connecting to your TCP port 9092 (on eth1):"
	echo "  $0 enable eth1 worker2 9092 120"
	echo "  # Disable the same"
	echo "  $0 disable eth1 worker2 9092 120"
	echo ""
	echo "  # Remove all mangle rules and traffic shapers"
	echo "  # WARNING! This removes them all, not just the ones added by this script"
	echo "  #          Please verify first with:"
	echo "  iptables -L -t mangle"
	echo "  tc qdisc show"
	echo "  # To flush:"
	echo "  $0 flush"
	exit 1
fi

MARK=10${DELAY_MS}

if [[ "$OP" = "enable" ]]; then
    # Ingress shaping based on iptables MARKing is non-trivial so we'll only do it on egress.
    # This is okay though since we are only interested in the actual round-trip-time delay.

    echo 'Enable:'
    echo ' * Flow marking'
    iptables -t mangle -A POSTROUTING -o $DEV -p tcp --destination $IP --source-port $TCP_PORT -j MARK --set-mark $MARK

    echo ' * TC qdisc tree root'
    tc qdisc add dev $DEV root handle 1: prio || true
    echo ' * TC qdisc delay handler'
    tc qdisc add dev $DEV parent 1:1 handle $DELAY: netem delay ${DELAY}ms
    echo ' * TC mark filter'
    tc filter add dev $DEV parent 1:0 prio 1 handle $MARK fw flowid $DELAY:1
    echo ' * Done'

elif [[ "$OP" == "disable" ]]; then

    echo 'Disable:'
    echo ' * Flow marking'
    iptables -t mangle -D POSTROUTING -o $DEV -p tcp --destination $IP --source-port $TCP_PORT -j MARK --set-mark $MARK
    echo ' * TC qdisc delay handler'
    tc qdisc del dev $DEV parent 1:1 handle $DELAY: netem delay ${DELAY}ms
    echo ' * TC mark filter'
    tc filter del dev $DEV parent 1:0 prio 1 handle $MARK fw flowid $DELAY:1

elif [[ "$OP" == "flush" ]]; then
    echo "Flushing all rules and qdiscs"
    iptables -t mangle -F
    tc qdisc del dev $DEV root

else
    echo "See usage"
    exit 1
fi



