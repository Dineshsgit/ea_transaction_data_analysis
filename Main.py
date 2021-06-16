import pandas as pd
import json
import os

def main():
    # Initialize the config and read parameter file ('/parameters/config.json')
    config = CONFIG

    # read the data from source as a pandas dataframe
    source_data = read_input_file(config['input_directory'])

    # Generate the hash key and remove invalid columns
    augmented_data = generate_hash(source_data)

    # Split the dataframe into multiple chunks and write the output to CSV
    split_dataframe(augmented_data, config['json_output_directory'])

    # find the top suburbs based on the instalation time
    find_top_suburbs(augmented_data, config['top_suburbs_output'])

    # find the top agents in each suburb based on the amount
    find_top_agents_by_suburb(augmented_data, config['top_agents_output'])


# read input file from the loaction
def read_input_file(file_path):
    path = os.path.dirname(os.path.realpath(__file__))
    data = pd.read_csv("{}/{}".format(path, file_path))
    return data


# generate hash key and drop invalid data
def generate_hash(data):
    data['hash_key'] = pd.util.hash_pandas_object(data)
    data['hash_key'] = data['hash_key'].astype(str)
    del data['Unnamed: 17']
    del data['Unnamed: 18']
    return data


# break the pandas dataframe into chunks and write output file each chunk of size 1000 records
def split_dataframe(df, output_directory, chunk_size = 1000):
    path = os.path.dirname(os.path.realpath(__file__)) + '/' + output_directory
    chunks = list()
    num_chunks = len(df) // chunk_size + 1
    for i in range(num_chunks):
        chunks.append(df[i*chunk_size:(i+1)*chunk_size])
    for j, df in enumerate(chunks):
    	df.to_json(orient = "records", force_ascii = True, default_handler = None, path_or_buf='{}/{}.json'.format(path,j))
    return True


def find_top_suburbs(df, output_directory):
    path = os.path.dirname(os.path.realpath(__file__)) + '/' + output_directory
    df['days_to_install'] = pd.to_datetime(df['Implemented Date '], format="%d/%m/%Y %H:%M") - pd.to_datetime(
        df['Request Date '], format="%d/%m/%Y %H:%M")
    df['days_to_install'] = df['days_to_install'] / pd.Timedelta(1, unit='d')
    data_by_pc = df[['days_to_install', 'Post Code ']]
    output_df = data_by_pc.groupby(['Post Code ']).mean().reset_index().sort_values('days_to_install', ascending=True)
    output_df.to_csv(path + '/' + 'top_suburbs.csv', index = False, header=True)


def find_top_agents_by_suburb(df, output_directory):
    path = os.path.dirname(os.path.realpath(__file__)) + '/' + output_directory
    output_df = df[['Post Code ', 'Agent ID ', '$ Amount ']].groupby(['Agent ID ', 'Post Code ']).sum().reset_index().sort_values(
        '$ Amount ', ascending=False)
    output_df.to_csv(path + '/' + 'top_agents.csv', index=False, header=True)


# This method is to read the config file and handle any run time exceptions
def get_config():
    config_file= None
    path = os.path.dirname(os.path.realpath(__file__))
    try:
        config_file = open(file="{}/parameters/config.json".format(path), mode="r")
        config_str = config_file.read()
        config_json = json.loads(config_str)
        return config_json
    finally:
        if (config_file!=None):
            config_file.close()

CONFIG = get_config()

if __name__ == "__main__":
    main()