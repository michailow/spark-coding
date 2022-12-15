from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import StringType
from pyspark.ml.classification import LogisticRegression, OneVsRest
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import VectorAssembler


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
    df = spark.read.csv(filepath)
    return df


def prepareModel(df):
    """Prepare model
    
    Args:
        df: pyspark.sql.dataframe.DataFrame
    Return:
        model: LinearRegression
    """
    
    mapping = {'Iris-virginica' : "1", "Iris-setosa" : "2", "Iris-versicolor" : "3"}
    dfIrisLabeled = df.withColumnRenamed('_c4', 'label')
    dfIrisLabeled = dfIrisLabeled.replace(to_replace=mapping, subset=['label'])
    dfIrisLabeled = dfIrisLabeled.withColumn("label", dfIrisLabeled.label.cast('int'))
    dfIrisInt = (dfIrisLabeled.withColumn("_c0", dfIrisLabeled._c0.cast('int'))
                     .withColumn("_c1", dfIrisLabeled._c1.cast('int'))
                     .withColumn("_c2", dfIrisLabeled._c2.cast('int'))
                     .withColumn("_c3", dfIrisLabeled._c3.cast('int')))
    assembler = VectorAssembler(inputCols = ['_c0', '_c1', '_c2', '_c3'], outputCol='features')
    output = assembler.transform(dfIrisInt)
    finalisedData = output.select('features', 'label')
    lr = LogisticRegression(maxIter=10, tol=1E-6, fitIntercept=True, labelCol='label', featuresCol='features')
    fitModel = lr.fit(finalisedData)
    return fitModel


def predictModel(spark, model):
    """Predict given results model
    
    Args:
        spark: pyspark.sql.session.SparkSession
        model: LinearRegression
    Return:
        df: dataframe with predictions, pyspark.sql.dataframe.DataFrame
    """
    predData = spark.createDataFrame(
        [(5.1, 3.5, 1.4, 0.2),
         (6.2, 3.4, 5.4, 2.3)],
        ["sepal_length", "sepal_width", "petal_length", "petal_width"])
    assembler = VectorAssembler(inputCols = ['sepal_length',
                                             'sepal_width',
                                             'petal_length',
                                             'petal_width'],
                                outputCol='features')
    predDataAcc = assembler.transform(predData)
    predFeatures = predDataAcc.select('features')
    predicts = model.transform(predFeatures)
    predictsPrepared = (predicts.withColumn("features", f.col('features').cast(StringType()))
                        .withColumn("rawPrediction", f.col('rawPrediction').cast(StringType()))
                        .withColumn("probability", f.col('probability').cast(StringType()))
                        .withColumn("prediction", f.col('prediction').cast(StringType())))
    return predictsPrepared


def loadDF(df, outPutPath):
    """Load DF into folder
    
    Args:
        df: pyspark.sql.dataframe.DataFrame
        outputPath: path to save files
    Return:
        None
    """
    df.coalesce(1).write.csv(outPutPath)
    

def main():
    """Main ETL script definition
    
    """
    filePath = '../files/iris.data'
    outPutPath = '../out/out_3_2.txt' 
    
    spark = startSpark()
    df = extractDF(spark, filePath)
    model = prepareModel(df)
    dfPred = predictModel(spark, model)
    loadDF(dfPred, outPutPath)
    

if __name__ == "__main__":
    main()