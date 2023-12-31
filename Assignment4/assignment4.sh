#!/bin/bash#!/bin/bash
#SBATCH --nodes=1
#SBATCH --cpus-per-task=16
#SBATCH --time 4:00:00
#SBATCH --job-name=assignment4
#SBATCH --partition=assemblix
#SBATCH --mail-type=ALL
#SBATCH --mail-user=p.c.riesebos@st.hanze.nl

export FILE1=/data/dataprocessing/MinIONData/MG5267/MG5267_TGACCA_L008_R1_001_BC24EVACXX.filt.fastq
export FILE2=/data/dataprocessing/MinIONData/MG5267/MG5267_TGACCA_L008_R2_001_BC24EVACXX.filt.fastq
export OUTPUT=/students/2021-2022/master/Peter_DSLS/output

mkdir -p  ${OUTPUT}
mkdir -p output


# Run velveth and store output in student folder 
seq 25 2 28 | parallel -j16 'velveth $OUTPUT/{} {} -longPaired -fastq $FILE1 $FILE2 && velvetg $OUTPUT/{} && cat $OUTPUT/{}/contigs.fa | (python3 assignment4.py && echo -e {}; ) >> output/output.csv'

# Run python cleanup file
python3 cleanup.py

# remove unneeded output files
rm -rf $OUTPUT