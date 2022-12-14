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
        return: pyspark.rdd.RDD
    """
    rdd=spark.sparkContext.parallelize(csv)
    values = rdd.countByValue().items()
    rddValues=spark.sparkContext.parallelize(values)
    return rddValues

    
def loadCSV(rdd, outputPath):
    """Load CSV into folder
    
    Args:
        filepath: path to downloaded csv file
    Return:
        return: None
    """
    rdd.saveAsTextFile(outputPath)


def main():
    """Main ETL script definition
    
    """
    filePath = '../files/groceries.csv'
    outPutPath = '../test2_out/out_1_3.txt'
    spark = startSpark()
    csv = extractCSV(filePath)
    rdd = transformRDD(spark, csv)
    loadCSV(rdd, outPutPath)

    
if __name__ == '__main__':
    main()