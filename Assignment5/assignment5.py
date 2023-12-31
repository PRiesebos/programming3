import os
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import functions as f
import csv

class Questions:
    def __init__(self):
        self.sc = SparkContext('local[16]')
        path="/data/dataprocessing/interproscan/all_bacilli.tsv"
        self.df = SQLContext(self.sc).read.csv(path, sep=r'\t', header=False, inferSchema= True)
        self.save_directory = "./output/"
        self.ensure_directory_exists(self.save_directory)
  
    @staticmethod
    def ensure_directory_exists(directory_path):
        if not os.path.exists(directory_path):
            os.makedirs(directory_path)

    def prepare_dataframe(self):
        self.df = self.df.withColumnRenamed('_c0', 'Protein_accession')\
            .withColumnRenamed('_c1', 'MD5')\
            .withColumnRenamed('_c2', 'Seq_len')\
            .withColumnRenamed('_c3', 'Analysis')\
            .withColumnRenamed('_c4', 'Signature_accession')\
            .withColumnRenamed('_c5', 'Signature_description')\
            .withColumnRenamed('_c6', 'Start')\
            .withColumnRenamed('_c7', 'Stop')\
            .withColumnRenamed('_c8', 'Score')\
            .withColumnRenamed('_c9', 'Status')\
            .withColumnRenamed('_c10', 'Date')\
            .withColumnRenamed('_c11', 'InterPro_accession')\
            .withColumnRenamed('_c12', 'InterPro_description')\
            .withColumnRenamed('_c13', 'GO_annotations')\
            .withColumnRenamed('_c14', 'Pathways')

    @staticmethod
    def get_explain_str(df):
        return df._sc._jvm.PythonSQLUtils.explainString(df._jdf.queryExecution(),"simple")

    #1. How many distinct protein annotations are found in the dataset? I.e. how many distinc InterPRO numbers are there?
    def query_one(self):
        Q1 = self.df.filter(self.df.InterPro_accession!="-").select("InterPro_accession").distinct()
        return ['1', Q1.count(), get_explain_str(Q1)]

    #2. How many annotations does a protein have on average?
    def query_two(self):
        Q2 = self.df.filter(self.df.InterPro_accession!="-").groupBy('Protein_accession')
        Q2 = Q2.agg(f.count('InterPro_accession')).agg(f.mean('count(InterPro_accession)'))
        return ['2', Q2.collect()[0][0], get_explain_str(Q2)]
    
    #3. What is the most common GO Term found?
    def query_three(self):
        Q3 = self.df.filter(self.df.GO_annotations!="-")
        Q3 = Q3.withColumn("go_a", f.explode(f.split(self.df.GO_annotations, "[|]")))
        Q3 = Q3.groupBy('go_a').count().orderBy('count', ascending=False)
        return ['3', Q3.collect()[0][0], get_explain_str(Q3)]
    
    #4. What is the average size of an InterPRO feature found in the dataset?
    def query_four(self):
        Q4 = self.df.withColumn('size', self.df['Stop'] - self.df['Start'])
        Q4 = Q4.groupBy().avg('size')
        return ['4', Q4.collect()[0][0], get_explain_str(Q4)]

    #5. What is the top 10 most common InterPRO features?
    def query_five(self):
        Q5 = self.df.filter(self.df.InterPro_accession != "-")
        Q5 = Q5.groupBy('InterPro_accession').count().orderBy('count', ascending=False)
        return ['5', [i[0] for i in Q5.take(10)], get_explain_str(Q5)]
    
    #6. If you select InterPRO features that are almost the same size (within 90-100%) as the protein itself, what is the top10 then?
    def query_six(self):
        Q6 = Q6.withColumn('size_ipf', Q6['Stop'] - Q6['Start'])
        Q6 = Q6.withColumn('size', Q6['size_ipf']/Q6['Seq_len'])
        Q6 = Q6.filter(Q6['size'] > 0.9).orderBy('size', ascending=False)
        return ['6', [i[11] for i in Q6.take(10)], get_explain_str(Q6)]
    
    #7. If you look at those features which also have textual annotation, what is the top 10 most common word found in that annotation?
    def query_seven(self):
        Q7 = self.df.filter(self.df.InterPro_description!="-")
        Q7 = Q7.withColumn("desc", f.explode(f.split(Q7.InterPro_description, " ")))
        Q7 = Q7.groupBy('desc').count().orderBy('count', ascending=False)
        return ['7', [i[0] for i in Q7.take(10)], get_explain_str(Q7)]
    
    #8. And the top 10 least common?
    def query_eight(self):
        Q8 = self.df.filter(self.df.InterPro_description!="-")
        Q8 = Q8.withColumn("desc", f.explode(f.split(Q8.InterPro_description, " ")))
        Q8 = Q8.groupBy('desc').count().orderBy('count', ascending=True)
        return ['8', [i[0] for i in Q8.take(10)], get_explain_str(Q8)]
    
    #9. Combining your answers for Q6 and Q7, what are the 10 most commons words found for the largest InterPRO features?
    def query_nine(self):
        Q9 = self.df.filter(self.df.InterPro_accession!="-")
        Q9 = Q9.withColumn('size_ipf', Q9['Stop'] - Q9['Start'])
        Q9 = Q9.withColumn('size', Q9['size_ipf']/Q9['Seq_len'])
        Q9 = Q9.filter(Q9['size'] > 0.9).orderBy('size', ascending=False)
        Q9 = Q9.withColumn("desc", f.explode(f.split(Q9.InterPro_description, " ")))
        Q9 = Q9.groupBy('desc').count().orderBy('count', ascending=False)
        return ['9', [i[0] for i in Q9.take(10)], get_explain_str(Q9)]
    
    #10 What is the coefficient of correlation (R**2) between the size of the protein and the number of features found?
    def query_ten(self):
        Q10 = self.df.filter(self.df.InterPro_accession!="-")
        Q10 = Q10.groupby(Q10.Protein_accession, 'Seq_len').count()
        return ['10', Q10.corr('Seq_len', 'count')**2, get_explain_str(Q10)]

    def all_queries(self):
        return [self.query_one(), self.query_two(),
                self.query_three(), self.query_four(),
                self.query_five(), self.query_six(),
                self.query_seven(), self.query_eight(),
                self.query_nine(), self.query_ten()]

    def save_answers(self):
        with open(self.save_directory + 'output.csv', 'w') as file:
            writer = csv.writer(file)
            queries = self.all_queries()
            for query in queries:
                writer.writerow(query)

if __name__ == "__main__":
    question_analysis = Questions()
    question_analysis.prepare_dataframe()
    question_analysis.save_answers()
