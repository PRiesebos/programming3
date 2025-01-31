"""
Substitute Assignment for Programming 3
Author: Peter Riesebos
Date: 31-01-2025

This script analyzes GBFF (GenBank Flat File) data using PySpark. It extracts and processes genomic
features, calculating various statistics such as the average number of features per genome,
coding-to-non-coding ratio, and feature length distributions. The parsed data is stored in a Spark DataFrame
for further analysis.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, count, avg, min, max, when
import re


class GBFFAnalyzer:
    """
    A class to analyze GBFF files and extract genomic features using PySpark.
    """

    def __init__(self, file_path):
        """
        Initializes the GBFFAnalyzer with a given file path and sets up the Spark session.

        :param file_path: Path to the GBFF file.
        """
        self.file_path = file_path
        self.spark = (
            SparkSession.builder.appName("GBFF Parser")
            .master("local[16]")
            .config("spark.driver.memory", "32g")
            .config("spark.executor.memory", "32g")
            .getOrCreate()
        )
        self.schema = StructType(
            [
                StructField("genome_id", StringType(), False),
                StructField("organism", StringType(), True),
                StructField("feature_type", StringType(), False),
                StructField("start", IntegerType(), False),
                StructField("end", IntegerType(), False),
                StructField("length", IntegerType(), False),
                StructField("strand", StringType(), False),
                StructField("cds_with_protein", IntegerType(), True),
                StructField("cds_without_protein", IntegerType(), True),
            ]
        )
        self.df = self.load_data()

    def parse_location(self, location):
        """
        Parses the location string from the GBFF file.

        :param location: The location string from the GBFF file.
        :return: Tuple containing (start, end, strand).
        """
        if location.startswith(("<", ">")):
            return None, None, None

        strand = "+"
        if "complement" in location:
            strand = "-"
            location = location.replace("complement(", "").replace(")", "")

        numbers = re.findall(r"\d+", location)

        try:
            return int(numbers[0]), int(numbers[1]), strand
        except:
            return None, None, strand

    def extract_cds_counts(self, record):
        """
        Extracts the CDS counts from a record.

        :param record: A list of lines representing a GBFF record.
        :return: Tuple of (cds_with_protein, cds_without_protein).
        """
        comment_block = " ".join(record)
        cds_with_protein = re.search(
            r"CDSs \(with protein\)\s+::\s+([\d,]+)", comment_block
        )
        cds_without_protein = re.search(
            r"CDSs \(without protein\)\s+::\s+([\d,]+)", comment_block
        )

        return (
            (
                int(cds_with_protein.group(1).replace(",", ""))
                if cds_with_protein
                else None
            ),
            (
                int(cds_without_protein.group(1).replace(",", ""))
                if cds_without_protein
                else None
            ),
        )

    def parse_gbff(self):
        """
        Parses the GBFF file and extracts feature information.

        :return: A list of dictionaries containing genomic features.
        """
        with open(self.file_path, "r") as f:
            lines = [line.rstrip() for line in f]

        records, current_record = [], []
        for line in lines:
            if line.startswith("LOCUS"):
                if current_record:
                    records.append(current_record)
                current_record = [line]
            elif line == "//":
                current_record.append(line)
                records.append(current_record)
                current_record = []
            else:
                current_record.append(line)
            if len(records) >= 20:
                break

        all_features = []
        for record in records:
            try:
                accession_line = next(
                    line for line in record if line.startswith("ACCESSION")
                )
                primary_accession = accession_line.split()[1]

                organism = None
                source_lines = [
                    line[21:] for line in record if line.startswith(" " * 21)
                ]
                source_text = " ".join(source_lines)
                organism_match = re.search(r'/organism="([^"]+)"', source_text)
                if organism_match:
                    organism = organism_match.group(1)

                cds_with_protein, cds_without_protein = self.extract_cds_counts(record)
            except (StopIteration, AttributeError):
                continue

            for feature in record:
                if feature.startswith("     gene"):
                    continue

                feature_type = feature[5:21].strip()
                start, end, strand = self.parse_location(feature[21:].strip())
                if start is None or end is None:
                    continue

                length = abs(end - start) + 1
                all_features.append(
                    {
                        "genome_id": primary_accession,
                        "organism": organism,
                        "feature_type": feature_type,
                        "start": start,
                        "end": end,
                        "length": length,
                        "strand": strand,
                        "cds_with_protein": cds_with_protein,
                        "cds_without_protein": cds_without_protein,
                    }
                )

        return all_features

    def load_data(self):
        """
        Loads the parsed GBFF data into a Spark DataFrame.
        """
        parsed_data = self.parse_gbff()
        df = self.spark.createDataFrame(parsed_data, schema=self.schema)
        df = df.withColumn(
            "coding_status",
            when(col("feature_type") == "CDS", "coding").otherwise("non-coding"),
        )
        return df

    def average_features_per_genome(self):
        """
        Calculates the average number of features per genome.
        """
        return (
            self.df.groupBy("genome_id")
            .agg(avg(count("*")).alias("avg_features_per_genome"))
            .collect()[0][0]
        )

    def coding_non_coding_ratio(self):
        """
        Computes the ratio of coding to non-coding features.
        """
        coding_non_coding_counts = (
            self.df.groupBy("coding_status").agg(count("*").alias("count")).collect()
        )
        coding_count = next(
            (row[1] for row in coding_non_coding_counts if row[0] == "coding"), 0
        )
        non_coding_count = next(
            (row[1] for row in coding_non_coding_counts if row[0] == "non-coding"), 1
        )
        return coding_count / non_coding_count

    def min_max_proteins_per_organism(self):
        """
        Returns the minimum and maximum number of proteins per organism.
        """
        return (
            self.df.groupBy("organism")
            .agg(
                min("cds_with_protein").alias("min_proteins"),
                max("cds_with_protein").alias("max_proteins"),
            )
            .collect()
        )

    def average_feature_length(self):
        """
        Computes the average length of genomic features.
        """
        return self.df.agg(
            avg(abs(col("length"))).alias("avg_feature_length")
        ).collect()[0][0]

    def save_coding_features(self, output_path):
        """
        Saves the coding features as a Parquet file.
        """
        self.df.filter(col("coding_status") == "coding").write.format("parquet").mode(
            "overwrite"
        ).save(output_path)


file_path = "/data/datasets/NCBI/refseq/ftp.ncbi.nlm.nih.gov/refseq/release/archaea/archaea.1.genomic.gbff"
analyzer = GBFFAnalyzer(file_path)

with open("output.txt", "w") as output_file:
    output_file.write(
        f"Average features per genome: {analyzer.average_features_per_genome()}\n"
    )
    ratio = analyzer.coding_non_coding_ratio()
    output_file.write(
        f"Coding to non-coding ratio: {ratio if ratio != float('inf') else 'All coding'}\n"
    )
    output_file.write("Minimum and maximum proteins per organism:\n")
    for organism, min_p, max_p in analyzer.min_max_proteins_per_organism():
        output_file.write(f"{organism}: Min={min_p}, Max={max_p}\n")
    output_file.write(f"Average feature length: {analyzer.average_feature_length()}\n")

analyzer.save_coding_features(
    "/homes/pcriesebos/Documents/programming3/Assignment6_alternative/dataframe_output_filtered"
)
