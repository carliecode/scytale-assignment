# Scytale-Assignment
This is a data engineering task, that fetches all the PRs in an organization's GitHub repository. 
PRs from each repo are stored in independent JSON files. 
These files are transformed, using Spark and the given business rules. 
The output of the process is stored as a parquet file.

There are 5 files added to the repository namely -

- **config.py**: holds all config settings for the project
- **quick_solution.ipynb**: Jupyter file that shows a primitive approach to solving the problem.
- **github_data_transformer.py**: a Python class that holds the implementation of the entire process, it calls the class above
- **main_file.ipynb**: this is the Jupyter file that serves as the entry point to the program. The class mentioned above is initialized and used here.

# How to run the solution
Using spark on Docker, browse to the location of the project folder using Jupyter on docker, open **main_file.ipynb**,
and run the codes. If you successfully run this, you you see the following : 

![image](https://github.com/carliecode/scytale-assignment/assets/15030941/09ef939f-0fdc-4795-a4f8-7646f9e42ce6)
