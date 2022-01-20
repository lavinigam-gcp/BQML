#Python Helper Functions

from google.cloud import bigquery

def debugLogSQL(sql):
    # Make sure to use "Query Formatter" in "More" option in CBQ Console
    return(' '.join([line.strip() for line in sql.splitlines()]).strip())
    
def get_df_from_query(query):
    # return df from the query given. 
    #Dont pass on big data queries. Usefule for small datasets  
    client = bigquery.Client()
    query_job = client.query(query)
    result_df = query_job.to_dataframe()
    return result_df

def get_table_detail_dict(project_id,dataset_id,table_name=None):
    table_details_dict = {}
    table_details_dict['project_id'] = project_id 
    table_details_dict['dataset_id'] = dataset_id
    table_details_dict['table_name'] = table_name
    return table_details_dict

def get_data_shape(table_details_dict):
    query ="""
    SELECT  
    count(distinct column_name),
    (select  count(*) from  `{project_id}.{dataset_id}.events_*`)
    FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
    """.format(project_id = table_details_dict['project_id'],
               dataset_id = table_details_dict['dataset_id'])
    return [get_df_from_query(query).rename(columns = {"f0_":"Total Columns",
                                                       "f1_":"Total Rows"})
            ,debugLogSQL(query)]


def get_datatypes_of_column(table_details_dict,specific_type=None):
    if not specific_type:
        query = """SELECT 
        column_name,ordinal_position,
        is_nullable,data_type
        FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = '{table_name}'
        """.format(project_id = table_details_dict['project_id'],
                   dataset_id = table_details_dict['dataset_id'],
                   table_name = table_details_dict['table_name'])
        return [get_df_from_query(query),debugLogSQL(query)]
    elif specific_type in ["STRING","INT64","FLOAT64"] :
        query = """
        SELECT 
        column_name,ordinal_position,
        is_nullable,data_type
        FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = '{table_name}'
        AND data_type like '{dtype}'
        """.format(project_id = table_details_dict['project_id'],
                   dataset_id = table_details_dict['dataset_id'],
                   table_name = table_details_dict['table_name'],
                  dtype = specific_type)
        return [get_df_from_query(query),debugLogSQL(query)]
    elif specific_type == 'ARRAY':
        query = """
        SELECT 
        column_name,ordinal_position,
        is_nullable,data_type
        FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = '{table_name}'
        AND data_type like '{dtype}%'
        """.format(project_id = table_details_dict['project_id'],
                   dataset_id = table_details_dict['dataset_id'],
                   table_name = table_details_dict['table_name'],
                  dtype = specific_type)
        return [get_df_from_query(query),debugLogSQL(query)]
    elif specific_type == 'STRUCT':
        query = """
        SELECT 
        column_name,ordinal_position,
        is_nullable,data_type
        FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = '{table_name}'
        AND data_type like '{dtype}%'
        """.format(project_id = table_details_dict['project_id'],
                   dataset_id = table_details_dict['dataset_id'],
                   table_name = table_details_dict['table_name'],
                  dtype = specific_type)
        return [get_df_from_query(query),debugLogSQL(query)]
    else:
        return ["ERROR", """
        Not correct dataype supplied or no columns with that type. Try with - 
        STRING, INT64,FLOAT64,ARRAY, STRUCT
        """]
                
        

def get_all_tables_in_dataset(table_details_dict):
    query = """
    SELECT TABLE_NAME from `{project_id}.{dataset_id}.INFORMATION_SCHEMA.TABLES`
    """.format(project_id = table_details_dict['project_id'],
               dataset_id = table_details_dict['dataset_id'])
    return [get_df_from_query(query),debugLogSQL(query)]
        
def get_topN_data_from_table(N,table_details_dict, all_tables=True):
    if all_tables:
        query = """
        SELECT *
        FROM 
            `{project_id}.{dataset_id}.events_*`
        LIMIT {N}
        """.format(project_id = table_details_dict['project_id'],
                   dataset_id = table_details_dict['dataset_id'],
                   N = N)
        return [get_df_from_query(query),debugLogSQL(query)]
    
def get_table_metadata(table_details_dict):
    query = """
    SELECT * 
    FROM `{project_id}.{dataset_id}.__TABLES__`
    """.format(project_id = table_details_dict['project_id'],
                   dataset_id = table_details_dict['dataset_id'])
    return [get_df_from_query(query),debugLogSQL(query)]

def get_specific_datatype_table(table_details_dict,dtype,limit=10):
    query = """
            DECLARE query STRING;
            DECLARE columns ARRAY<STRING>;
            # get all the specific columns in an array 
            SET columns = (
              WITH all_columns AS (
                SELECT column_name
                FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
                WHERE table_name = 'events_20210128'
                and  data_type IN {dtype}
              )
              SELECT ARRAY_AGG((column_name) ) AS columns
              FROM all_columns
            );

            SET query = (select STRING_AGG(x) from unnest(columns) as x);

            EXECUTE IMMEDIATE 
            "select " || query || " from `{project_id}.{dataset_id}.{table_name}` LIMIT {LIMIT}";
            """.format(project_id = table_details_dict['project_id'],
                   dataset_id = table_details_dict['dataset_id'],
                   table_name = table_details_dict['table_name'],
                  dtype = dtype,
                      LIMIT=limit)
    return [get_df_from_query(query),debugLogSQL(query)]

def get_count_percentage_fromtable(table_details_dict,dtype,limit=10):
    query = """
            DECLARE query STRING;
            DECLARE columns ARRAY<STRING>;
            DECLARE count INT64;
          
            SET columns = (
              WITH all_columns AS (
                SELECT column_name
                FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
                WHERE table_name = 'events_20210128'
                and  data_type IN {dtype}
              )
              SELECT ARRAY_AGG((column_name) ) AS columns
              FROM all_columns
            );
            set count = (SELECT
            COUNT(*)
            FROM {project_id}.{dataset_id}.{table_name});
            set query = (select STRING_AGG('ROUND(COUNT(DISTINCT ' ||x||")/"||count||",2)*100  as "||x||'_prcntg') 
            from unnest(columns) as x);

            EXECUTE IMMEDIATE 
            "select " || query || " from `{project_id}.{dataset_id}.{table_name}` LIMIT {LIMIT}";
            """.format(project_id = table_details_dict['project_id'],
                   dataset_id = table_details_dict['dataset_id'],
                   table_name = table_details_dict['table_name'],
                  dtype = dtype,
                      LIMIT=limit)
    return [get_df_from_query(query),debugLogSQL(query)]    
    
    
# def get_data_count_profile(table_details_dict):
#     data_count_dict = {}
#     query = """
#     SELECT 
#     COUNT(*) 
#     FROM `{project_id}.{dataset_id}.events_*`
#     """.format(project_id = table_details_dict['project_id'],
#               dataset_id = table_details_dict['dataset_id'])
    
#     total_data_count = bq_helper_functions.get_df_from_query(query).iloc[0][0]
#     data_count_dict['total_data_count'] = total_data_count
#     for each_table in bq_helper_functions.get_all_tables_in_dataset(table_params)[0]['TABLE_NAME']:
#         query = """
#         SELECT 
#         COUNT(*) 
#         FROM `{project_id}.{dataset_id}.{table_name}`
#         """.format(project_id = table_details_dict['project_id'],
#                   dataset_id = table_details_dict['dataset_id'],
#                  table_name =each_table )
#         ind_table_count = bq_helper_functions.get_df_from_query(query).iloc[0][0]
#         data_count_dict[each_table] = ind_table_count
#         count_df = pd.DataFrame(data_count_dict,index=[0]).T.reset_index().rename(columns={'index':"key",0:'data_count'})
#     return count_df