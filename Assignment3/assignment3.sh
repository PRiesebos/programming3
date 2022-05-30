#!/bin/bash
#SBATCH --time 7:00:00
#SBATCH --nodes=1
#SBATCH --cpus-per-task=16
#SBATCH --mail-type=ALL
#SBATCH --mail-user=p.c.riesebos@st.hanze.nl
#SBATCH --partition=assemblix
mkdir -p output
export BLASTDB=/local-fs/datasets/refseq_protein
export time=/usr/bin/time
export time_file=output/timings.txt
export blastoutput=output/blastoutput.txt
for n in {1..16}
do $time --append -o time_file blastp -query MCRA.faa -db $BLASTDB -num_threads $n -outfmt 6 >> $blastoutput
done
# python3 assignment3.py