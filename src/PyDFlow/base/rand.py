from random import Random

def genrandom(*args, **kwargs):
    return gen(lambda r: r.randrandom(a, b), *args, **kwargs)


def genrandint(a, b, *args, **kwargs):
    return gen(lambda r: r.randint(a, b), *args, **kwargs)

def gensample(population, k, *args, **kwargs):
    return gen(lambda r: r.sample(population, k), *args, **kwargs)

def gen(f, seed=None):
    """
    Generate an unlimited supply of random numbers.
    Designed to be used with imap or izip.
    """
    r = Random()
    r.seed(seed)
    while True:
        yield f(r)
