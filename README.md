# ea_transaction_data_analysis

This project is done using Pandas Dataframes and Pandas API.

This is broken down into five steps:

**Step1:** Extract the data from CSV using pandas read(method: read_input_file)

**Step2:** Generate the Hash Key to uniquely determine each row in the dataset(method: generate_hash)

**Step3:** Generate the JSON file for every 1000 rows in dataframe. Indorder to achieve this I have broken down the Dataframe into chunks for each chunk of size 1000 records. After each chunk has been generated I write the JSON file based on the index of the chunk on filename(method_name: split_dataframe).

**Step4:** As the Pandas dataframe doesnt automatically cast the date columns (request date and Implementation date) to timestamp. Using pandas.to_datetime I am exclusively casting these values and  find the difference between days in order to determine the turnaround time for each suburb(method_name: find_top_suburbs).

**Step5:** Group by AgentId and Postcode and sum by Amount to find the top agent for suburb(method_name : find_top_agents_by_suburb)

**Input Location:** /input/transaction.csv

**Output Location:**    
    /output/json_output - for writing output JSON files         
    /output/top_agents - for writing top agents         
    /output/top_suburns - for writing top suburbs
    
    
    



       |-- parameters/
       |      |-- config.json
       |-- input
       |      |-- transaction.cv
       |-- output
       |      |-- json_output/
       |      |-- top_agents/
       |      |-- top_suburbs/
       |-- test_output
       |-- Main.py
       |-- Tests.py
       |-- ReadMe.Md
