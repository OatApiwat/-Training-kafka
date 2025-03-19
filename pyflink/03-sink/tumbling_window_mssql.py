from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment
from pyflink.table.expressions import lit, col
from pyflink.table.window import Tumble
import dotenv
import os
import pymssql
from datetime import datetime

dotenv.load_dotenv()


def insert_to_mssql(data):
    """Insert processed data into MSSQL"""
    try:
        # Establish connection to MSSQL
        server="192.168.0.203"
        user="sa"
        password="sa$admin123"
        database="kafka_db"

        # Connect to the SQL Server
        conn = pymssql.connect(server=server, user=user, password=password, database=database)
        cursor = conn.cursor()

        # SQL insert query
        insert_query = """
        INSERT INTO kafka_tb2 (topic, window_start, window_end, g_data1, g_data2, acc_data1, acc_data2, data1xdata2)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, tuple(data)) 

        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error while inserting into MSSQL: {e}")

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

    tumbling_window = source_kafka_1.window(Tumble.over(lit(5).seconds)
                                            .on(source_kafka_1.ts)
                                            .alias('w'))\
                                            .group_by(col('w'), source_kafka_1.topic)\
                                            .select(source_kafka_1.topic,
                                                    col('w').start.alias('window_start'),
                                                    col('w').end.alias('window_end'),
                                                    (source_kafka_1.data1).max.alias('g_data1'),
                                                    (source_kafka_1.data2).max.alias('g_data2'),
                                                    (source_kafka_1.data1).sum.alias('acc_data1'),
                                                    (source_kafka_1.data2).sum.alias('acc_data2'),
                                                    (source_kafka_1.data1 * source_kafka_1.data2).max.alias('data1xdata2')
                                                    )

    result = tumbling_window.execute().collect()

    data_to_insert = []
    for row in result:
        print("row: ",row)
        print(type(row))
        topic = row[0]
        window_start = row[1].strftime('%Y-%m-%d %H:%M:%S')
        window_end = row[2].strftime('%Y-%m-%d %H:%M:%S')
        g_data1 = row[3]
        g_data2 = row[4]
        acc_data1 = row[5]
        acc_data2 = row[6]
        data1xdata2 = row[7]
        data_to_insert = [topic,window_start,window_end,g_data1,g_data2,acc_data1,acc_data2,data1xdata2]
        print(data_to_insert)

        insert_to_mssql(data_to_insert)

def main():
    streaming()

if __name__ == '__main__':
    main()