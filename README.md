# scytale-assignment
This is a data engineering task, that fetches all the PRs in an organization's GitHub repository. 
PRs from each repo are stored in independent JSON files. 
These files are transformed, using Spark and the given business rules. 
The output of the process is stored as a parquet file.
