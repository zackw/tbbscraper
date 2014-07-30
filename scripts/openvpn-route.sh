#! /bin/sh

PATH=/sbin:/bin:/usr/sbin:/usr/bin
export PATH

set -e

tun_netns=vpn${dev#tun}
case "$tun_netns" in
     (vpn[0-9] | vpn[0-9][0-9] | vpn[0-9][0-9][0-9]) ;;
     (*) exit 1;;
esac

[ -d /etc/netns/$tun_netns ] || exit 1
[ -f /var/run/netns/$tun_netns ] || exit 1

ip netns exec $tun_netns \
  ip route add default via $route_vpn_gateway dev $dev
ip netns exec $tun_netns \
  ip route add 128.0.0.0/1 via $route_vpn_gateway
