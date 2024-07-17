# Imports
import duckdb
import time

# Functions
def create_duckdb():
    '''
    Select the station, its min, mean and max temperatures and group by the stations
    '''
    result = duckdb.sql("""
        SELECT station,
            MIN(temperature) AS min_temperature,
            CAST(AVG(temperature) AS DECIMAL(3, 1)) AS mean_temperature,
            MAX(temperature) AS max_temperature
        FROM read_csv("data/measurements.txt", AUTO_DETECT=FALSE, sep=';', columns={'station':VARCHAR, 'temperature': 'DECIMAL(3, 1)'})
        GROUP BY station
        ORDER BY station
    """)

    # Showing the result
    result.show()

    # Saving the result in a .parquet file for future using
    result.write_parquet('data\measurements.parque')

# Main function
if __name__ == "__main__":
    import time
    start_time = time.time()
    create_duckdb()
    took = time.time() - start_time
    print(f"Duckdb Took: {took:.2f} sec")
