from pyspark.sql import SparkSession


def startSpark(name="spark-etl"):
    """Start Spark Sessions
    
    Args:
        param name: Spark job name
    Return:
        spark: SparkSession object
    """
    spark = (SparkSession
             .builder
             .appName(name)
             .getOrCreate())
    return spark


def extractCSV(filepath):
    """Opens CSV and return list of products
    
    Args:
        filepath: path to downloaded csv file
    Return:
        csv: list of products
    """
    with open(filepath, 'r') as csvfile:
        csvtext = csvfile.readlines()
    csv = []
    for i in csvtext:
        line = i.replace("\n", "")
        add = line.split(',')
        csv.extend(add)
    return csv


def transformRDD(spark, csv):
    """Transforms cvs list into Spark RDD,
    collects unique and get number of products
    
    Args:
        filepath: path to downloaded csv file
    Return:
        rddUnique: pyspark.rdd.RDD
    """
    rdd=spark.sparkContext.parallelize(csv)
    rddUnique = rdd.distinct()
    rddUniqueCount = rddUnique.count()
    rddUnique=spark.sparkContext.parallelize([rddUniqueCount])
    return rddUnique

    
def loadCSV(rdd, outputPath):
    """Load CSV into folder
    
    Args:
        filepath: path to downloaded csv file
    Return:
        None
    """
    rdd.coalesce(1).saveAsTextFile(outputPath)


def main():
    """Main ETL script definition
    
    """
    filePath = '../files/groceries.csv'
    outPutPath = '../out/out_1_2b.txt'
    spark = startSpark()
    csv = extractCSV(filePath)
    rdd = transformRDD(spark, csv)
    loadCSV(rdd, outPutPath)

    
if __name__ == '__main__':
    main()