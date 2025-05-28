FROM bde2020/spark-master:3.3.0-hadoop3.3


# Set working directory
WORKDIR /app

# # Copy the local application code to the container
# COPY spark-stream.py /app/spark-stream.py
# COPY get_all_spark_stream.py /app/get_all_spark_stream.py

# Install Python packages (pip and pyspark)
RUN pip install pyspark kafka-python

# Command to run both spark jobs sequentially
CMD /spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 /app/kafka_to_hive.py
#  && \
#     /spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 /app/get_all_spark_steam.py