#! /usr/bin/python

# The dummy worker just spawns lots of connections and then doesn't do
# anything with them.  This is to stress-test the controller.

import cPickle as pickle
import pickletools
import sys
import time
import zmq
import zmq.ssh

import contextlib
import warnings

def pickled(cmd, *args):
    # The optimize() is here because pickle is tuned for backreferences at
    # the expense of wire output length when there are no backreferences.
    return pickletools.optimize(pickle.dumps((cmd, args),
                                             pickle.HIGHEST_PROTOCOL))

def unpickled(pickl):
    return pickle.loads(pickl)

class WorkerConnection(object):
    def __init__(self, address, tunnel=None):
        self.tunnel = tunnel
        self.address = address

    def __enter__(self):
        time.sleep(0.25)
        try:
            self.context = zmq.Context()
            self.req = self.context.socket(zmq.REQ)
            self.req.setsockopt(zmq.LINGER, 0)
            if self.tunnel:
                zmq.ssh.tunnel_connection(self.req, self.address, self.tunnel)
            else:
                self.req.connect(self.address)

            self.req.send(pickled("HELO"))
            (cmd, args) = unpickled(self.req.recv())
            if ((cmd == "DONE" and len(args) == 0) or
                (cmd == "HELO" and len(args) == 4)):
                return self

            raise RuntimeError("protocol error: expected HELO, got %s%s"
                               % (repr(cmd), repr(args)))
        except:
            self.__exit__()
            raise

    def __exit__(self, *dontcare):
        if self.req is not None:
            self.req.close()
        if self.context is not None:
            self.context.destroy()

def main(argv):
    if len(argv) == 3:
        (_, n, address) = argv
        tunnel = None
    elif len(argv) == 4:
        (_, n, address, tunnel) = argv
    else:
        raise SystemExit("usage: {} n-connections address [tunnel]"
                         .format(argv[0]))

    connections = tuple(WorkerConnection(address, tunnel)
                        for _ in range(int(n)))

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        with contextlib.nested(*connections):
            time.sleep(300)

if __name__ == '__main__':
    main(sys.argv)
