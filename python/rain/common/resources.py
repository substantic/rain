

class Resources:

    def __init__(self, n_cpus=1):
        self.n_cpus = n_cpus

    def to_capnp(self, builder):
        builder.nCpus = self.n_cpus


def cpus(n_cpus):
    """Create a resource containing only cpus"""
    return Resources(n_cpus=n_cpus)

cpu_1 = cpus(1)
