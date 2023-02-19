# import libraries
import os
import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession\
    .builder\
    .appName("FormulaOneAnalysis")\
    .master("local[8]")\
    .getOrCreate()

from depenencies.spark import start_spark

def main():
    
    spark, log, config = start_spark(
        app_name = "formula_one_analysis"
    )
    
    log.warn('etl is starting')
    
    


from pyspark.sql.functions import when, upper, to_date, datediff, floor, rank, desc
from pyspark.sql.window import Window

def startSparkSession():
    spark = 

def getFileNames(folderPath):
    return os.listdir(folderPath)

def createDF(directoryPath, file):
    return spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(directoryPath + file)

def avgTimeSpentAtPitStop(driverDf, pitStopDf):
    return driverDf.join(pitStopDf, driverDf.driverId == pitStopDf.driverId, 'inner').groupby('forename', 'surname').avg('milliseconds').withColumnRenamed("avg(milliseconds)", "avg_pit_stop_time")

def populateMissingDriverCodes(driverDf):
    return driverDf.withColumn('code', when(driverDf.driverRef.contains('_'), upper(driverDf.surname.substr(0, 3))).when(driverDf.code == '\\N', upper(driverDf.driverRef.substr(0, 3))).otherwise(driverDf.code))

def findAgePerSeason(drivers_df, results_df, races_df, seasonStartDate, seasonEndDate, seasonYear):
    racesForDrivers = drivers_df.join(results_df, drivers_df.driverId == results_df.driverId, 'inner').select('forename', 'surname', 'dob', 'raceId')
    driversForYear = racesForDrivers.join(races_df, racesForDrivers.raceId == races_df.raceId, 'inner').filter(races_df.year == seasonYear).select('forename', 'surname', 'dob', 'year', 'date')
    ageForDrivers = driversForYear.withColumn('age', floor(datediff(to_date('date', 'yyyy-M-d'), to_date('dob', 'yyyy-M-d')) / 365.25))
    
    startYoungest = ageForDrivers.filter(ageForDrivers.date == seasonStartDate).orderBy('age').limit(1)
    startOldest = ageForDrivers.filter(ageForDrivers.date == seasonStartDate).orderBy('age', ascending = False).limit(1)
    endYoungest = ageForDrivers.filter(ageForDrivers.date == seasonEndDate).orderBy('age').limit(1)
    endOldest = ageForDrivers.filter(ageForDrivers.date == seasonEndDate).orderBy('age', ascending = False).limit(1)
    
    return startYoungest.union(startOldest).union(endYoungest).union(endOldest)
    
def mostWinByGrandPrix(races_df, results_df, drivers_df):
    driversByRace = races_df.join(results_df.filter(results_df.position == 1), races_df.raceId == results_df.raceId, 'inner').select('driverId', 'name')
    countsByGrandPrix = driversByRace.join(drivers_df, drivers_df.driverId == driversByRace.driverId, 'inner').groupBy('name', 'forename', 'surname').count()
    
    windowSpec = Window.partitionBy("name").orderBy(desc("count"))
    winsByDriver = countsByGrandPrix.withColumn("rankByWins", rank().over(windowSpec))
    
    return winsByDriver.filter(winsByDriver.rankByWins == 1)

def mostLossByGrandPrix(races_df, results_df, drivers_df):
    driversByRace = races_df.join(results_df.filter(results_df.position > 1), races_df.raceId == results_df.raceId, 'inner').select('driverId', 'name')
    countsByGrandPrix = driversByRace.join(drivers_df, drivers_df.driverId == driversByRace.driverId, 'inner').groupBy('name', 'forename', 'surname').count()
    
    windowSpec = Window.partitionBy("name").orderBy(desc("count"))
    lossByDriver = countsByGrandPrix.withColumn("rankByWins", rank().over(windowSpec))
    
    return lossByDriver.filter(lossByDriver.rankByWins == 1)