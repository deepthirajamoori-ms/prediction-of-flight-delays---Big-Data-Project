from amadeus import Client, ResponseError
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from functools import reduce  # For Python 3.x
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

spark = SparkSession \
    .builder \
    .appName("Predict Airlines Project") \
    .config("", "") \
    .getOrCreate()

sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)

amadeus = Client(
    client_id='0AIexWdDZGUqXinZ8A9znjIStvCk98B0',
    client_secret='r7hgfYqWeGb8mx8A'
)

originLocation = input("Please enter the origin location : ")
destinationLocation = input("Please enter the destination location : ")
response = amadeus.shopping.flight_offers_search.get(originLocationCode = originLocation , \
                                                     destinationLocationCode = destinationLocation, \
                                                     departureDate='2020-07-01', adults=1)

actual_iatacode_origin = []
actual_terminal_origin = []
actual_at_origin = []
actual_iatacode_destination = []
actual_terminal_destination = []
actual_at_destination = []
actual_carrierCode = []
actual_flight_number = []
actual_duration = []

for loop1 in response.data:
    for loop2 in loop1['itineraries']:
        duration = loop2['duration']
        for segment in loop2['segments']:
            for loop3,v in segment['departure'].items():
                if loop3 == 'iataCode':
                  actual_iatacode_origin.append(v)
                elif loop3 == 'terminal':
                  actual_terminal_origin.append(v)
                elif loop3 == 'at':
                  actual_at_origin.append(v)
            for loop4,v1 in segment['arrival'].items():
                if loop4 == 'iataCode':
                  actual_iatacode_destination.append(v1)
                elif loop4 == 'terminal':
                  actual_terminal_destination.append(v1)
                elif loop4 == 'at':
                  actual_at_destination.append(v1)
            actual_carrierCode.append(segment['carrierCode'])
            actual_flight_number.append(segment['number'])

df = sqlContext.createDataFrame(zip(actual_iatacode_origin,
                               actual_at_origin,
                               actual_iatacode_destination,
                               actual_at_destination,
                               actual_carrierCode,
                               actual_flight_number), schema=['actual_iatacode_origin',
                                                              'actual_at_origin',
                                                              'actual_iatacode_destination',
                                                              'actual_at_destination',
                                                              'actual_carrierCode',
                                                              'actual_flight_number'])
df_filter = df.where(df.actual_iatacode_origin == originLocation)

data_jan = spark.read.csv(r"C:\Users\dinesh.karem\Desktop\Big Data\Project-Predicting_the_flight_delays\Data_for_project\547910423_T_ONTIME_REPORTING_JAN.csv",header=True,sep=",");
data_feb = spark.read.csv(r"C:\Users\dinesh.karem\Desktop\Big Data\Project-Predicting_the_flight_delays\Data_for_project\547910423_T_ONTIME_REPORTING_FEB.csv",header=True,sep=",");
data_mar = spark.read.csv(r"C:\Users\dinesh.karem\Desktop\Big Data\Project-Predicting_the_flight_delays\Data_for_project\547910423_T_ONTIME_REPORTING_MAR.csv",header=True,sep=",");
data_apr = spark.read.csv(r"C:\Users\dinesh.karem\Desktop\Big Data\Project-Predicting_the_flight_delays\Data_for_project\547910423_T_ONTIME_REPORTING_APR.csv",header=True,sep=",");
data_may = spark.read.csv(r"C:\Users\dinesh.karem\Desktop\Big Data\Project-Predicting_the_flight_delays\Data_for_project\547910423_T_ONTIME_REPORTING_MAY.csv",header=True,sep=",");
data_june = spark.read.csv(r"C:\Users\dinesh.karem\Desktop\Big Data\Project-Predicting_the_flight_delays\Data_for_project\547910423_T_ONTIME_REPORTING_JUNE.csv",header=True,sep=",");
data_july = spark.read.csv(r"C:\Users\dinesh.karem\Desktop\Big Data\Project-Predicting_the_flight_delays\Data_for_project\547910423_T_ONTIME_REPORTING_JULY.csv",header=True,sep=",");
data_aug = spark.read.csv(r"C:\Users\dinesh.karem\Desktop\Big Data\Project-Predicting_the_flight_delays\Data_for_project\547910423_T_ONTIME_REPORTING_AUG.csv",header=True,sep=",");
data_sep = spark.read.csv(r"C:\Users\dinesh.karem\Desktop\Big Data\Project-Predicting_the_flight_delays\Data_for_project\547910423_T_ONTIME_REPORTING_SEP.csv",header=True,sep=",");
data_oct = spark.read.csv(r"C:\Users\dinesh.karem\Desktop\Big Data\Project-Predicting_the_flight_delays\Data_for_project\547910423_T_ONTIME_REPORTING_OCT.csv",header=True,sep=",");
data_nov = spark.read.csv(r"C:\Users\dinesh.karem\Desktop\Big Data\Project-Predicting_the_flight_delays\Data_for_project\547910423_T_ONTIME_REPORTING_NOV.csv",header=True,sep=",");
data_dec = spark.read.csv(r"C:\Users\dinesh.karem\Desktop\Big Data\Project-Predicting_the_flight_delays\Data_for_project\547910423_T_ONTIME_REPORTING_DEC.csv",header=True,sep=",");

data_jan = data_jan.select(data_jan.FL_DATE, data_jan.OP_UNIQUE_CARRIER, data_jan.OP_CARRIER_FL_NUM, data_jan.DEP_DELAY)
data_feb = data_feb.select(data_feb.FL_DATE, data_feb.OP_UNIQUE_CARRIER, data_feb.OP_CARRIER_FL_NUM, data_feb.DEP_DELAY)
data_mar = data_mar.select(data_mar.FL_DATE, data_mar.OP_UNIQUE_CARRIER, data_mar.OP_CARRIER_FL_NUM, data_mar.DEP_DELAY)
data_apr = data_apr.select(data_apr.FL_DATE, data_apr.OP_UNIQUE_CARRIER, data_apr.OP_CARRIER_FL_NUM, data_apr.DEP_DELAY)
data_may = data_may.select(data_may.FL_DATE, data_may.OP_UNIQUE_CARRIER, data_may.OP_CARRIER_FL_NUM, data_may.DEP_DELAY)
data_june = data_june.select(data_june.FL_DATE, data_june.OP_UNIQUE_CARRIER, data_june.OP_CARRIER_FL_NUM, data_june.DEP_DELAY)
data_july = data_july.select(data_july.FL_DATE, data_july.OP_UNIQUE_CARRIER, data_july.OP_CARRIER_FL_NUM, data_july.DEP_DELAY)
data_aug = data_aug.select(data_aug.FL_DATE, data_aug.OP_UNIQUE_CARRIER, data_aug.OP_CARRIER_FL_NUM, data_aug.DEP_DELAY)
data_sep = data_sep.select(data_sep.FL_DATE, data_sep.OP_UNIQUE_CARRIER, data_sep.OP_CARRIER_FL_NUM, data_sep.DEP_DELAY)
data_oct = data_oct.select(data_oct.FL_DATE, data_oct.OP_UNIQUE_CARRIER, data_oct.OP_CARRIER_FL_NUM, data_oct.DEP_DELAY)
data_nov = data_nov.select(data_nov.FL_DATE, data_nov.OP_UNIQUE_CARRIER, data_nov.OP_CARRIER_FL_NUM, data_nov.DEP_DELAY)
data_dec = data_dec.select(data_dec.FL_DATE, data_dec.OP_UNIQUE_CARRIER, data_dec.OP_CARRIER_FL_NUM, data_dec.DEP_DELAY)

def unionAll(*dfs):
    return reduce(DataFrame.unionAll, dfs)

final_csv_file = unionAll(*[data_jan, data_feb, data_mar,data_apr,data_may,data_june,data_july,data_aug,data_sep,data_oct,data_nov,data_dec])

overall_airlines_perf = final_csv_file.groupBy([final_csv_file.OP_UNIQUE_CARRIER.alias('CARRIER')]).agg(F.round(F.avg('DEP_DELAY'), 0).alias('AVERAGE_dELAY'))
print("Overall flight operators performance, good performers being on top: \n")
overall_airlines_perf.sort('Average_delay', ascending=True).show()

flight_perf_per_airlines = final_csv_file.groupBy([final_csv_file.OP_UNIQUE_CARRIER, final_csv_file.OP_CARRIER_FL_NUM]).agg(F.round(F.avg('DEP_DELAY'), 0).alias('Expected_delay'), F.min('DEP_DELAY'), F.max('DEP_DELAY'))
condition_on_join = [df_filter.actual_carrierCode == flight_perf_per_airlines.OP_UNIQUE_CARRIER, df_filter.actual_flight_number == flight_perf_per_airlines.OP_CARRIER_FL_NUM]

final_df = df_filter.join(flight_perf_per_airlines, condition_on_join, 'inner' ).select(df_filter.actual_iatacode_origin.alias('ORIGIN'), \
                                                                          df_filter.actual_at_origin.alias('DEP_TIME'), \
                                                                          df_filter.actual_iatacode_destination.alias('DESTINATION'), \
                                                                          df_filter.actual_at_destination.alias('ARR_TIME'), \
                                                                          df_filter.actual_carrierCode.alias('CARRIER'), \
                                                                          df_filter.actual_flight_number.alias('FL_NUM'), \
                                                                          flight_perf_per_airlines.Expected_delay.alias('EXPECTED_DELAY'))
print("Below are the flights info from your chosen origin, Expected delay in minutes: \n")
final_df.show()
