import pandas as pd
    
def get_best(file):
    # Read data and set column names
    data = pd.read_csv(file, sep=':', header=None)
    data.columns = ['name', 'value']

    # Extract N50 and Kmer_size values
    n50_value = data.loc[data['name'].str.contains('N50'), 'value'].max()
    kmer_value = data.loc[data['name'].str.contains('Kmer_size'), 'value'].max()

    # Create a new dataframe with the extracted values
    result = pd.DataFrame({'n50': [n50_value], 'kmer': [kmer_value]})

    # Save the result to a CSV file
    result.to_csv('output/kmer_result.csv', index=False)

    # Print the result
    print(f"best kmer is: {kmer_value} and best n50 is: {n50_value}")


if __name__ == "__main__":
    get_best('output/output.csv')
