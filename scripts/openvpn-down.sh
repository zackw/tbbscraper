#! /bin/sh

PATH=/sbin:/bin:/usr/sbin:/usr/bin
export PATH

set -ex

tun_netns=tns${dev#tun}

case "$tun_netns" in
     (tns[0-9] | tns[0-9][0-9] | tns[0-9][0-9][0-9]) ;;
     (*) exit 1;;
esac

[ -d /etc/netns/$tun_netns ] || exit 1

pids=$(ip netns pids $tun_netns)
if [ -n "$pids" ]; then
    kill $pids
    sleep 5
    pids=$(ip netns pids $tun_netns)
    if [ -n "$pids" ]; then
        kill -9 $pids
    fi
fi

# this automatically cleans up the routes and tunnel device
ip netns delete "$tun_netns"
rm -rf /etc/netns/$tun_netns
