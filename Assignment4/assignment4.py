#!/usr/bin/env python

## calculate N50 from fasta file
## N50 = contig length such that half of the contigs are longer and 1/2 of contigs are shorter


import numpy as np
import sys
from itertools import groupby

def compute_n50(fasta_stream):
    lengths = []

    # Group sequences by header
    groups = (x[1] for x in groupby(fasta_stream, lambda line: line[0] == ">"))

    for record in groups:
        # Join sequence lines
        seq = "".join(s.strip() for s in next(groups))
        lengths.append(len(seq))

    # Sort contigs from longest to shortest
    sorted_lengths = sorted(lengths, reverse=True)
    cumulative_sum = np.cumsum(sorted_lengths)

    total_length = sum(lengths)
    n2_value = total_length // 2

    # Find N50
    n50_index = np.searchsorted(cumulative_sum, n2_value)
    n50 = sorted_lengths[n50_index]

    return total_length, n50

if __name__ == "__main__":
    # Using sys.stdin for input
    N50_value = compute_n50(sys.stdin)

    # Writing N50 value to sys.stdout
    sys.stdout.write(str(N50_value) + '\n')
