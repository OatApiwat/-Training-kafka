from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings,StreamTableEnvironment
import dotenv
import os
from pyflink.table.expressions import col

dotenv.load_dotenv()

def streaming():
    # create streaming envionment
    stream_env = StreamExecutionEnvironment.get_execution_environment()
    stream_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    # create table envionment
    tb_env = StreamTableEnvironment.create(stream_execution_environment=stream_env,
                                           environment_settings=stream_settings)

    # flink sql-connector-kafka jar path
    jar_path = "/home/mic/mic_kafka_training/pyflink/flink-sql-connector-kafka-3.4.0-1.20.jar"
    tb_env.get_config().get_configuration().set_string("pipeline.jars", "file:///" + jar_path)
    
    # create kafka source table
    source_kafka_1= f"""
    CREATE TABLE source_table_1 (
        topic VARCHAR,
        data1 INT,
        data2 INT,
        data3 INT,
        data4 INT,
        data5 INT,
        data6 INT,
        data7 INT,
        data8 INT,
        data9 INT,
        data10 INT,
        ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' 
    ) WITH (
        'connector' = 'kafka',
        'topic' = '{os.environ["KAFKA_TOPIC_1"]}',
        'properties.bootstrap.servers' = '{os.environ["KAFKA_SERVER"]}',
        'properties.group.id' = 'test_1',
        'scan.startup.mode' = 'latest-offset',
        'format' = 'json'
    )
    """
    # execute
    tb_env.execute_sql(source_kafka_1)
    source_kafka_1 = tb_env.from_path('source_table_1')  # table source name

    # show schema
    print("+++++++++++ schema +++++++++++")
    source_kafka_1.print_schema()

    # seelct all
    # final_tb = source_kafka_1.select(col("*"))
    
    # where
    # final_tb = source_kafka_1.select(col("*")).where(col("data1")<100)
    # where
    final_tb = source_kafka_1.select(col("data1"),col("data2")).where(col("data1")<100)
    
    # filter
    # final_tb = source_kafka_1.select(col("*")).filter(col("data1")<100)
    
    # rename
    # final_tb = source_kafka_1.select(col("*")).rename_columns(col("data1").alias('data_1'))

    # https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/tableapi/


    # print result
    final_tb.execute().print()

def main():
    streaming()

if __name__ == '__main__':
    main()