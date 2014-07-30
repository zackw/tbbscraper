#! /bin/sh

mask2cidr () {
    local nbits dec
    nbits=0
    for dec in $(echo $1 | sed 's/\./ /g') ; do
        case "$dec" in
            (255) nbits=$(($nbits + 8)) ;;
            (254) nbits=$(($nbits + 7)) ;;
            (252) nbits=$(($nbits + 6)) ;;
            (248) nbits=$(($nbits + 5)) ;;
            (240) nbits=$(($nbits + 4)) ;;
            (224) nbits=$(($nbits + 3)) ;;
            (192) nbits=$(($nbits + 2)) ;;
            (128) nbits=$(($nbits + 1)) ;;
            (0)   ;;
            (*) echo "Error: $dec is not a valid netmask component" >&2
                exit 1
                ;;
        esac
    done
    echo "$nbits"
}

mask2network () {
    local host mask h m result
    host="$1."
    mask="$2."
    result=""
    while [ -n "$host" ]; do
        h="${host%%.*}"
        m="${mask%%.*}"
        host="${host#*.}"
        mask="${mask#*.}"
        result="$result.$(($h & $m))"
    done
    echo "${result#.}"
}

maybe_config_dns () {
    local n option servers
    n=1
    servers=""
    while [ $n -lt 100 ]; do
       eval option="\$foreign_option_$n"
       [ -n "$option" ] || break
       case "$option" in
           (*DNS*)
               set -- $option
               servers="$servers
nameserver $3"
               ;;
           (*) ;;
       esac
       n=$(($n + 1))
    done
    if [ -n "$servers" ]; then
        cat > /etc/netns/$tun_netns/resolv.conf <<EOF
# name servers for $tun_netns
$servers
EOF
    fi
}

config_inside_netns () {
    local ifconfig_cidr ifconfig_network

    ifconfig_cidr=$(mask2cidr $ifconfig_netmask)
    ifconfig_network=$(mask2network $ifconfig_local $ifconfig_netmask)

    ip link set dev lo up
    ip link set dev $dev mtu $tun_mtu
    ip link set dev $dev up

    ip addr add dev $dev \
        local $ifconfig_local/$ifconfig_cidr \
        broadcast $ifconfig_broadcast \
        scope link

    #ip route add $ifconfig_network/$ifconfig_cidr dev $dev
    ip route add default via $route_vpn_gateway dev $dev
}

PATH=/sbin:/bin:/usr/sbin:/usr/bin
export PATH

set -ex

tun_netns=vpn${dev#tun}
case "$tun_netns" in
     (vpn[0-9] | vpn[0-9][0-9] | vpn[0-9][0-9][0-9]) ;;
     (*) exit 1;;
esac

if [ $# -eq 1 ] && [ $1 = "INSIDE_NETNS" ]; then
    [ $(ip netns identify $$) = $tun_netns ] || exit 1
    config_inside_netns
else

    trap "rm -rf /etc/netns/$tun_netns ||:; ip netns del $tun_netns ||:" 0
    mkdir /etc/netns/$tun_netns
    maybe_config_dns

    ip netns add $tun_netns
    ip link set dev $dev netns $tun_netns

    ip netns exec $tun_netns $0 INSIDE_NETNS

    trap "" 0
fi
