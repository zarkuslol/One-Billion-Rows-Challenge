# Imports
import pandas as pd
from multiprocessing import Pool, cpu_count
from tqdm import tqdm  # Progress Bar

CONCURRENCY = cpu_count()

# Global variables
total_rows = 1_000_000_000  # Total de linhas conhecido
chunksize = 100_000_000  # Define o tamanho do chunk
filename = "data/measurements.txt"  # Certifique-se de que este é o caminho correto para o arquivo

# Functions
def process_chunk(chunk):
    '''
    Aggregate data inside the chunk using Pandas
    '''
    aggregated = chunk.groupby('station')['measure'].agg(['min', 'max', 'mean']).reset_index()
    return aggregated

def create_df_with_pandas(filename, total_linhas, chunksize=chunksize):
    '''
    Create final dataframe using Pandas
    '''
    total_chunks = total_linhas // chunksize + (1 if total_linhas % chunksize else 0)
    results = []

    with pd.read_csv(filename, sep=';', header=None, names=['station', 'measure'], chunksize=chunksize) as reader:
        # Envolvendo o iterador com tqdm para visualizar o progresso
        with Pool(CONCURRENCY) as pool:
            for chunk in tqdm(reader, total=total_chunks, desc="Processing"):
                # Processa cada chunk em paralelo
                result = pool.apply_async(process_chunk, (chunk,))
                results.append(result)

            results = [result.get() for result in results]

    final_df = pd.concat(results, ignore_index=True)

    final_aggregated_df = final_df.groupby('station').agg({
        'min': 'min',
        'max': 'max',
        'mean': 'mean'
    }).reset_index().sort_values('station')

    return final_aggregated_df

# Main function
if __name__ == "__main__":
    import time

    print("Starting file processing!")
    start_time = time.time()
    df = create_df_with_pandas(filename, total_rows, chunksize)
    took = time.time() - start_time

    print(df.head())
    print(f"Processing took: {took:.2f} sec")
