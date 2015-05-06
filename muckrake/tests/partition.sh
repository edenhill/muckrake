#!/bin/bash
#

set -e

OP=$1
DEV=$2
PEER_IP=$3
TCP_PORT=$4
TYPE=$5

if [[ -z "$TCP_PORT" ]]; then
    echo "Usage: $0 enable|disable DEV PEER_IP TCP_PORT [TYPE]"
    echo ''
    echo 'Partitions the network between our local TCP_PORT and the remote host PEER_IP.'
    echo ''
    echo 'TYPE is either:'
    echo ' * DROP - sessions will timeout, analogue to host going down or network partition'
    echo ' * REJECT - sessions will terminate immediately - analogue to service crashing but host and network intact'
    echo ''
    echo 'Example:'
    echo ' # Simulate network partition between local Kafka broker and broker3:'
    echo " $0 enable eth1 broker3 9092"
    echo ' # Repair partition'
    echo " $0 disable eth1 broker3 9092"
    echo ''
    echo ' # Simulate local broker crash for all connecting clients and brokers:'
    echo " $0 enable eth1 0.0.0.0/0 9092 REJECT"
    echo ' # Restore'
    echo " $0 disable eth1 0.0.0.0/0 9092 REJECT"
    exit 1
fi

if [[ -z "$TYPE" ]]; then
    TYPE="DROP"
fi


if [[ "$OP" = "enable" ]]; then
    echo "Add filter $TYPE $PEER_IP -> :$TCP_PORT"
    iptables -A INPUT -p tcp --source $PEER_IP --destination-port 9092 -j $TYPE --reject-with tcp-reset

elif [[ "$OP" = "disable" ]]; then
    echo "Remove filter $TYPE $PEER_IP -> :$TCP_PORT"
    iptables -D INPUT -p tcp --source $PEER_IP --destination-port 9092 -j $TYPE --reject-with tcp-reset

else
    echo "See usage"
    exit 1
fi


