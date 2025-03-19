from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings,StreamTableEnvironment
from pyflink.table.expressions import lit,col
from pyflink.table.window import Session
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
    jar_path = "E:\\mic_kafka_training\\pyflink\\flink-sql-connector-kafka-3.4.0-1.20.jar"
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
    tumbling_window = source_kafka_1.window(Session.with_gap(lit(1).seconds)
                                            .on(source_kafka_1.ts)
                                            .alias('w'))\
                                            .group_by(col('w'),source_kafka_1.topic)\
                                            .select(source_kafka_1.topic,
                                                    col('w').start.alias('window_start'),
                                                    col('w').end.alias('window_end'),
                                                    (source_kafka_1.data1).max.alias('g_data1'),
                                                    (source_kafka_1.data2).max.alias('g_data2'),
                                                    (source_kafka_1.data1).sum.alias('acc_data1'),
                                                    (source_kafka_1.data2).sum.alias('acc_data2'),
                                                    (source_kafka_1.data1*source_kafka_1.data2).max.alias('data1xdata2')
                                                    )

    # sink print out
    tumbling_window.execute().print()
    tb_env.execute('tb-api-tumbling-windows')

def main():
    streaming()

if __name__ == '__main__':
    main()