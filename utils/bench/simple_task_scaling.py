"""
A simple benchmark testing large graph handling and execution.

Two graph models are available:
* Graph simulating the fast fourier transform (indegree 2, log_2(N) layers, N nodes each).
* Random layered graph (customizable in-degree, depth and width).

Note that the FFT is not actually computed, in both cases the data blobs are only
concatenated. Also note that the data will grow considerably after few layers
(exponentially, roughly to size: start_size * (indeg ** layers). Use `--size 0` to avoid this.
"""

from rain.client import Client, tasks, blob
import random
import argparse


def build_random_layers(layers, n, inputs=2, datalen=1, session=None, rng=42, submit_every=None):
    if isinstance(rng, int):
        rng = random.Random(rng)
    dos = [blob('B' * datalen) for i in range(n)]
    for j in range(layers):
        if submit_every is not None and j % submit_every == 0:
            for d in dos:
                d.keep()
            session.submit()
            print("Submit before layer ", j)
        ts = [tasks.concat(set(dos[inp] for inp in rng.sample(range(n), inputs))) for i in range(n)]
        dos = [t.output for t in ts]
        for i, d in enumerate(dos):
            d.label = "L{}.{}".format(j, i)
    return dos


def build_fft_like(layers, datalen=1, session=None, submit_every=None):
    n = 1 << layers
    dos = [blob('B' * datalen) for i in range(n)]
    for j in range(layers):
        if submit_every is not None and j % submit_every == 0:
            for d in dos:
                d.keep()
            session.submit()
            print("Submit before layer ", j)
        ts = [tasks.concat((dos[i], dos[(i + (1 << j)) % n])) for i in range(n)]
        dos = [t.output for t in ts]
        for i, d in enumerate(dos):
            d.label = "L{}.{}".format(j, i)
    return dos


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("TYPE", type=str, help='Graph type: "net" or "fft"')
    parser.add_argument("--host", "-H", default="localhost", type=str)
    parser.add_argument("--port", "-P", default=7210, type=int)
    parser.add_argument("--layers", "-l", default=8, type=int, help='Number of layers')
    parser.add_argument("--width", "-w", default=256, type=int, help="Layer width (for 'net')")
    parser.add_argument("--deg", "-d", default=2, type=int, help="Indegree (for 'net')")
    parser.add_argument("--size", "-s", default=1, type=int, help='Base blob size')
    args = parser.parse_args()

    # Connect to server
    client = Client(args.host, args.port)

    # Create a new session
    with client.new_session() as session:

        # Build the benchmarking network
        if args.TYPE == 'fft':
            dos = build_fft_like(args.layers, datalen=args.size,
                                 session=session, submit_every=2)
            fin = args.size * (2 ** args.layers)
        elif args.TYPE == 'net':
            dos = build_random_layers(args.layers, args.width, session=session,
                                      inputs=args.deg, datalen=args.size, submit_every=16)
            fin = args.size * (2 ** args.deg)
        else:
            raise ValueError('invalid network type {}'.format(args.TYPE))
        for d in dos:
            d.keep()

        print("Expect {} objects of size {}, total {:.3f} MB".format(
              len(dos), fin, len(dos) * fin / 1024.0 / 1024.0))
        # Submit all crated tasks to server
        session.submit()

        # Wait for completion of task and fetch results and get it as bytes
        r = dos[0].fetch()
        assert r.get_bytes() == b'B' * fin
        session.wait_all()
