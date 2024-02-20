# scytale-assignment
This is a data engineering task, that fetches all the PRs in an organization's GitHub repository. 
PRs from each repo are stored in independent JSON files. 
These files are transformed, using Spark and the given business rules. 
The output of the process is stored as a parquet file.

There are 5 files added to the repository namely
- config.py : holds all config settings for the project
- quick_solution.ipynb : jupyter file that shows a primitive approach to solving the problem.
- github_data_transformer.py : a python class that holds the implementation of the entire process, it calls the class above
- main_file.ipynb : this is the jupyter file that serves as the entry point to the program. The class mentioned above is initialized and used here.
