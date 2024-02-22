from configs import *
from pyspark.sql import SparkSession,functions as F
from pyspark.sql.types import IntegerType, StringType, StructType, StructField, TimestampType
from pyspark.sql.functions import col, concat_ws, split
from github import Github
import json,os, datetime


class Github_Data_Transformer :
    
     def __init__(self):
        # read configuration from config file
        print('')
        print('Initializing the Github - Data Extracator ...') 
        configs = project_configs
        self.source_files_path = os.getcwd() +  r'/extracted_json_files/'       
        self.github_token = configs["token"]
        self.org_name = configs["org_name"]      
         
        #spark_master = configs["spark_master"]
        self.spark = SparkSession.builder.appName("Github - Data Transformer").getOrCreate()  
        self.columnsMetaData = StructType(
            [StructField("organization_name", StringType(), True),
            StructField("repository_id", StringType(), True),
            StructField("repository_name", StringType(), True),
            StructField("repository_owner", StringType(), True),
            #StructField("title", StringType(), True),
            StructField("merged_at", TimestampType(), True),
            StructField("state", StringType(), True)]
        )        
        # Create an empty RDD with empty schema
        emp_RDD = self.spark.sparkContext.emptyRDD()   
        self.df = self.spark.createDataFrame(emp_RDD, self.columnsMetaData)
        print('Initialization completed successfuly !') 
        print('') 

     def extract_github_data(self):
        print('Contacting Github for repos and PRs information ...')
        gh = Github(self.github_token)
        org = gh.get_organization(self.org_name)

        if not os.path.exists(self.source_files_path): 
            os.makedirs(self.source_files_path)
         
        for repo in org.get_repos():
            pr_filename = os.path.join(self.source_files_path, f"{repo.name}.json")
            list_prs = []

            for pr in repo.get_pulls(state='all'):
                pr_data = {
                    'organization_name': repo.full_name,
                    'repository_id': repo.id,
                    'repository_name': repo.name,
                    'repository_owner': repo.owner.login,
                    'merged_at': str(pr.merged_at),
                    'state': pr.state
                }
                list_prs.append(pr_data)

            with open(pr_filename, 'w') as f:
                json.dump(list_prs, f)

        print('Github PRs have been extracted successfully!')
        print(f'Files have been downloaded to {self.source_files_path}')


     def clean_transform_data(self):
        print('Transformation and cleaning of PR data has started ...')
        #load files into sparksession
        df = self.spark.read.json(self.source_files_path, self.columnsMetaData)
        
        #cleaning the data
        df = self.df.withColumn('organization_name',split(col('organization_name'),'/')[0])

        #applying all transformation rules
        df_num_prs = df.groupBy('organization_name','repository_id','repository_owner').count().withColumnRenamed('count', 'num_prs') 
        
        df_num_merged_prs = df.filter(col('merged_at').isNotNull())
        df_num_merged_prs = df_num_merged_prs.groupBy('repository_id').count().withColumnRenamed('count', "num_prs_merged")
        
        df_merged_at = df.groupBy('repository_id').agg(F.max('merged_at').alias('merged_at'))

        #Dear reviewer, please feel free to uncomment the codes below to see the tables used in the join
        #df.show()
        #df_num_prs.show()
        #df_num_merged_prs.show()
        #df_merged_at.show()
        
        df = df_num_prs.join(df_num_merged_prs, ['repository_id'], how='left') \
             .join(df_merged_at, df_num_prs['repository_id'] == df_merged_at['repository_id'], how='left') \
             .select('organization_name',df_num_prs['repository_id'],'repository_owner','num_prs','num_prs_merged', 'merged_at')
        
        df = df.withColumn(
            'is_compliant',
            F.when((F.col("num_prs") == F.col("num_prs_merged")) & F.col("repository_owner").like('%scytale%') , 1)\
            .otherwise(0)
        )

        print('Transformation and data cleaning has completed !')
        print('') 
        #return df
        

     def save_as_parquet(self) :
        self.df.write.mode('overwrite').format('parquet').save(self.source_files_path + 'ouput_file')
        print('Output file location is : ' + self.source_files_path + 'ouput_file') 
        print('') 
        print('The application has exited successfully') 
        
