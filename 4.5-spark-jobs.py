from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def main():
	spark = SparkSession.builder\
		.master('yarn')\
		.appName('4.5 Spark Test Job')\
		.getOrCreate()

	df = spark.read.option('header', True).option('inferSchema', True).csv('s3a://.../transaction_data.csv')
	filtered_df = df.where((col("TRANS_TIME") > 1000) & (col("TRANS_TIME") < 2200))\
		.where((col("STORE_ID") > 200) & (col("STORE_ID") < 500))
	
	filtered_df.write.mode('overwrite').format('delta').saveAsTable('module_4.transactional_data_1')

if __name__ == "__main__":
    main() 
