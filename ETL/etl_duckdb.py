# Imports
import duckdb
import time

# Functions
def create_duckdb():
    '''
    Select the station, its min, mean and max temperatures and group by the stations
    '''
    duckdb.sql("""
        SELECT station,
            MIN(temperature) AS min_temperature,
            CAST(AVG(temperature) AS DECIMAL(3, 1)) AS mean_temperature,
            MAX(temperature) AS max_temperature
        FROM read_csv("data/measurements.txt", AUTO_DETECT=FALSE, sep=';', columns={'station':VARCHAR, 'temperature': 'DECIMAL(3, 1)'})
        GROUP BY station
        ORDER BY station
    """).show()

# Main function
if __name__ == "__main__":
    import time
    start_time = time.time()
    create_duckdb()
    took = time.time() - start_time
    print(f"Duckdb Took: {took:.2f} sec")
