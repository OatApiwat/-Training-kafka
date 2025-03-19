from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings,StreamTableEnvironment
import dotenv
import os

dotenv.load_dotenv()

def streaming():
    # create streaming envionment
    stream_env = StreamExecutionEnvironment.get_execution_environment()
    stream_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    # create table envionment
    tb_env = StreamTableEnvironment.create(stream_execution_environment=stream_env,
                                           environment_settings=stream_settings)

    # flink jar path
    jar_path = "/home/mic/mic_kafka_training/pyflink/flink-sql-connector-kafka-3.4.0-1.20.jar"
    tb_env.get_config().get_configuration().set_string("pipeline.jars", "file:///" + jar_path)

    # create kafka source table
    source_kafka_1 = f"""
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
        'properties.group.id' = 'test_2',
        'scan.startup.mode' = 'latest-offset',
        'format' = 'json'
    )
    """
    # ต่างกันตรง topic
    # create kafka source table
    source_kafka_2 = f"""
    CREATE TABLE source_table_2 (
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
        'topic' = '{os.environ["KAFKA_TOPIC_2"]}',
        'properties.bootstrap.servers' = '{os.environ["KAFKA_SERVER"]}',
        'properties.group.id' = 'test_2',
        'scan.startup.mode' = 'latest-offset',
        'format' = 'json'
    )
    """
    # execute
    tb_env.execute_sql(source_kafka_1)
    source_kafka_1 = tb_env.from_path('source_table_1')

    tb_env.execute_sql(source_kafka_2)
    source_kafka_2 = tb_env.from_path('source_table_2')

    # show schema
    print("+++++++++++ schema1 +++++++++++")
    source_kafka_1.print_schema()
    print("+++++++++++ schema2 +++++++++++")
    source_kafka_2.print_schema()

    # query  union data
    sql_query = """
    SELECT * FROM source_table_1
    UNION ALL
    SELECT * FROM source_table_2
    """
    # excute query
    result_table = tb_env.sql_query(sql_query)

    # print result
    result_table.execute().print()

def main():
    streaming()

if __name__ == '__main__':
    main()