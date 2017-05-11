all: basic.py test_data.txt
	PYSPARK_DRIVER_PYTHON=python spark-submit \
		--master local[*] \
		--packages com.databricks:spark-csv_2.11:1.4.0,mysql:mysql-connector-java:5.1.39 \
		--driver-memory 8g \
		basic.py
