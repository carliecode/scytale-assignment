from github_data_transformer import Github_Data_Transformer

handler = Github_Data_Transformer()

handler.extract_github_data()
handler.clean_transform_data()
handler.save_as_parquet()