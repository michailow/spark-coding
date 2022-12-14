from pyspark.sql import SparkSession
import pyspark.sql.functions as f


def startSpark(name="spark-etl"):
    """Start Spark Sessions
    
    Args:
        param name: Spark job name
    Return:
        return: SparkSession object
    """
    spark = (SparkSession
             .builder
             .appName(name)
             .getOrCreate())
    return spark


def extractDF(spark, filepath):
    """Opens parquet file
    
    Args:
        spark: pyspark.sql.session.SparkSession
        filepath: path to downloaded parquet file
    Return:
        df: list of products, pyspark.sql.dataframe.DataFrame
    """
    df = spark.read.parquet(filepath)
    return df


def transformDF(df):
    """Transforms df into stats
    
    Args:
        df: raw dataframe; pyspark.sql.dataframe.DataFrame
    Return:
        df: prepared dataframe; pyspark.sql.dataframe.DataFrame
    """
    dfFiltered = df.filter(df.price > 5000).filter(df.review_scores_value == 10)
    dfSelected = dfFiltered.select(f.mean('bathrooms'), f.mean('bedrooms'))
    dfSelected = dfSelected.withColumnRenamed("avg(bathrooms)",
                                                "avg_bathrooms").withColumnRenamed("avg(bedrooms)",
                                                                                   "avg_bedrooms")
    
    return dfSelected

    
def loadDF(df, outputPath):
    """Load DF into folder
    
    Args:
        df: pyspark.sql.dataframe.DataFrame
        outputPath: path to save files
    Return:
        None
    """
    df.write.csv(outputPath)


def main():
    """Main ETL script definition
    
    """
    filePath = '../files/part-00000.parquet'
    outPutPath = '../test2_out/out_2_3.txt'
    
    spark = startSpark()
    df = extractDF(spark, filePath)
    dfPrep = transformDF(df)
    loadDF(dfPrep, outPutPath)

    
if __name__ == '__main__':
    main()