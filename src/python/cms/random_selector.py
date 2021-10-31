"""
This modules selects random lines from the the data and
outputs the selected lines

"""

import gzip
from argparse import ArgumentParser

import sys

import os
import glob
import random

from nsaph_utils.utils.io_utils import fopen


SEED = 1


def select(pattern: str, destination: str, threshold: float):
    files = glob.glob(pattern)
    random.seed(SEED)
    for f in files:
        name = os.path.basename(f)
        if not name.endswith('.gz'):
            name += ".gz"
        dest = os.path.basename(os.path.dirname(f))
        dest = os.path.join(destination, dest)
        if not os.path.isdir(dest):
            os.makedirs(dest)
        dest = os.path.join(dest, name)
        if os.path.isfile(dest):
            print("Skipping: {}".format(f))
            continue
        print("{} ==> {}".format(f, dest))

        with fopen(f, "rt") as src, gzip.open(dest, "wt") as output:
            n1 = 0
            n2 = 0
            for line in src:
                n1 += 1
                if random.random() < threshold:
                    output.write(line)
                    n2 += 1
        print("{:d}/{:d}".format(n2, n1))
    print("All Done")


def args():
    parser = ArgumentParser ("Random records selector")
    parser.add_argument("--in",
                        help="pattern to select incoming files",
                        default="data/*/*.csv*",
                        dest="input",
                        required=False)
    parser.add_argument("--out",
                        help="Directory to output the random selection",
                        default="random_data",
                        required=False)
    parser.add_argument("--selector",
                        help="A float value specifying the "
                             + "share of data to be selected",
                        default=0.02,
                        type=float,
                        required=False)
    arguments = parser.parse_args()
    return arguments


if __name__ == '__main__':
    arg = args()
    select(arg.input, arg.out, arg.selector)

