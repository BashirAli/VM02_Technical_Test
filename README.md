# VM02_Technical_Test
This repo is designed to host my results of the technical test assigned by Virgin Media O2, due to an application of their vacant Junior-Mid Data Engineer job role. Developed in PyCharm IDE 2021.2

This repo uses Apache Beam (https://beam.apache.org/) to do the following:
1. Read file from Google Cloud Storage
2. Exclude transactions lower than 20 
3. Exclude transactions earlier than 2010
4. Sum transactions by date
5. Save output to the provided output path 

All relevant code can be found under the ***/src*** folder, the entry file being called main.py respectively.
The repo also includes 1 unit test found under ***/test*** which aims to test all previous transformation steps mentioned (via Composite Transform).

# Run File
To run the source code: 
1. Open CMD (Command Prompt)
2. navigate to the code-env folder of this repo. See here for Windows 10 CMD example: **https://www.howtogeek.com/659411/how-to-change-directories-in-command-prompt-on-windows-10/#:~:text=If%20the%20folder%20you%20want,window%2C%20and%20then%20press%20Enter.** 
3. Run the following command: 

** pip install -r requirements.txt **

4. On completion, navigate to the src folder of this repo. Run the following command:

**python -m main --input-file gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv --output-path ../output/results**

This should run the repo and produce an output with a ***jsonl.gz*** file extension respectively


# Run Unit Test
To run the test code: 
1. Open CMD (Command Prompt)
2. navigate to the project folder of this repo. 
3. Run the following command into CMD

** python -m unittest test/composite_transform_test.py ** 

This should run the unit tests