from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings,StreamTableEnvironment
from pyflink.table.expressions import lit,col
from pyflink.table.window import Tumble
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
    # flink sql-connector-kafka jar path
    jar_path = "D:\\docker\\mic_kafka\\pyflink\\flink-sql-connector-kafka-3.4.0-1.20.jar"
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
        ts AS PROCTIME()
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
        # Read the table
    source_kafka_1 = tb_env.from_path('source_table_1')  # table source name

    # show schema
    print("+++++++++++ schema +++++++++++")
    source_kafka_1.print_schema()

    # Define Tumbling Window Aggregate for every 5 second
    tumbling_window = source_kafka_1.window(Tumble.over(lit(5).seconds)
                                            .on(source_kafka_1.ts)
                                            .alias('w'))\
                                            .group_by(col('w'),source_kafka_1.topic)\
                                            .select(source_kafka_1.topic,
                                                    col('w').start.alias('window_start'),
                                                    col('w').end.alias('window_end'),
                                                    (source_kafka_1.data1).max.alias('g_data1'),
                                                    (source_kafka_1.data2).max.alias('g_data2')
                                                    )

    # sink kafka
    sink_table = f"""
        CREATE TABLE kafka_tb (
            topic VARCHAR, 
            window_start TIMESTAMP_LTZ(3),
            window_end TIMESTAMP_LTZ(3),
            g_data1 BIGINT,
            g_data2 BIGINT
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'kafka_data',
            'properties.bootstrap.servers' = '{os.environ["KAFKA_SERVER"]}',
            'format' = 'json'
        )
        """
    
    tb_env.execute_sql(sink_table)
    tumbling_window.execute_insert("kafka_tb").wait()

def main():
    streaming()

if __name__ == '__main__':
    main()