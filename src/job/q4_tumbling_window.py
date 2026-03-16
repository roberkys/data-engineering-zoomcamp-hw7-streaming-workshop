from pyflink.table import EnvironmentSettings, TableEnvironment

def run_q4():
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = TableEnvironment.create(settings)
    
    t_env.get_config().set("parallelism.default", "1")

    # Source DDL
    source_ddl = """
        CREATE TABLE green_trips (
            lpep_pickup_datetime STRING,
            lpep_dropoff_datetime STRING,
            PULocationID INT,
            DOLocationID INT,
            passenger_count INT,
            trip_distance DOUBLE,
            tip_amount DOUBLE,
            total_amount DOUBLE,
            event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'green-trips',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'properties.group.id' = 'flink-q4-group',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        )
    """
    t_env.execute_sql(source_ddl)

    # Sink DDL
    sink_ddl = """
        CREATE TABLE q4_sink (
            window_start TIMESTAMP(3),
            PULocationID INT,
            num_trips BIGINT
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = 'q4_trips_per_window',
            'username' = 'postgres',
            'password' = 'postgres'
        )
    """
    t_env.execute_sql(sink_ddl)

    # Query
    t_env.execute_sql("""
        INSERT INTO q4_sink
        SELECT
            TUMBLE_START(event_timestamp, INTERVAL '5' MINUTE) AS window_start,
            PULocationID,
            COUNT(*) AS num_trips
        FROM green_trips
        GROUP BY
            PULocationID,
            TUMBLE(event_timestamp, INTERVAL '5' MINUTE)
    """)

if __name__ == '__main__':
    run_q4()
