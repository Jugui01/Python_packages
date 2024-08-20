import yaml
import pyodbc
from sqlalchemy import create_engine, URL
import pandas as pd
from azure.identity import DefaultAzureCredential

##Yamls##
config_Yaml = {'Database': ['brandtrends'], 
          'YAML_link': ['D:/Travail pro/Kidz Global/Azure_test/YAML/config_brandtrends.yaml']
          }
config_Yaml = pd.DataFrame(config_Yaml)
##########



def tables_lists(database):                     #List the available tables included in the database
    #Find Yaml path according to database value
    try:
        Path= list(config_Yaml[config_Yaml['Database'] == database]['YAML_link'])
    except pyodbc.Error as ex:
        return(f"Error: no existing yaml for database {database}", ex)

    if len(Path) == 0:
        return (f"Error: no existing yaml for database {database}")
    else:
        Path_yaml = Path[0]
    
    #Connect to Azure database
    with open(Path_yaml, 'r') as stream:
        config = yaml.safe_load(stream)

    # Connection information
    username = config['database_credentials']['username']
    password = config['database_credentials']['password']
    host = config['database_credentials']['host']
    database = config['database_credentials']['database']
    port = config['database_credentials']['port']

    # Create connection string

    try:
        # Create connection
        conn_str = f"DRIVER=ODBC Driver 18 for SQL Server;SERVER={host};DATABASE={database};UID={username};PWD={password}"

        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # List available tables
        list_tables_sql = '''
        SELECT TABLE_NAME 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_TYPE = 'BASE TABLE'
        '''
        cursor.execute(list_tables_sql)

        # Récupérer et afficher les noms des tables
        tables = cursor.fetchall()
        result = []
        for table in tables:
            result.append(table.TABLE_NAME)

        # Fermer la connexion
        cursor.close()
        conn.close()
        return result
    except pyodbc.Error as ex:
        return("Error for connecting to database :", ex)


def select_data(database, query):                       #Extract data from tables of database according to a query
    #Find Yaml path according to database value
    try:
        Path= list(config_Yaml[config_Yaml['Database'] == database]['YAML_link'])
    except pyodbc.Error as ex:
        return(f"Error: no existing yaml for database {database}", ex)

    if len(Path) == 0:
        return (f"Error: no existing yaml for database {database}")
    else:
        Path_yaml = Path[0]
    
    #Connect to Azure database
    with open(Path_yaml, 'r') as stream:
        config = yaml.safe_load(stream)

    # Connection information
    username = config['database_credentials']['username']
    password = config['database_credentials']['password']
    host = config['database_credentials']['host']
    database = config['database_credentials']['database']
    port = config['database_credentials']['port']

    # Create connection string

    try:
        # Create connection
        conn_str = f"DRIVER=ODBC Driver 18 for SQL Server;SERVER={host};DATABASE={database};UID={username};PWD={password}"

        conn = pyodbc.connect(conn_str)

        try:
            df = pd.read_sql_query(query, conn)
            df = pd.DataFrame(df)
        
        except pyodbc.Error as e:
            # Check is table is existing or not
            if 'Invalid object name' in str(e):
                raise ValueError(f"table is not existing in {database}") from e
            else:
                raise Exception(f"Error when executing query : {e}") from e
        finally:
            # Fermer la connexion
            conn.close()
    
        return df
      
    except pyodbc.Error as ex:
        return("Error for connecting to database :", ex)
    


def column_names(database, table):                      #List column names of a table from database
    #Find Yaml path according to database value
    try:
        Path= list(config_Yaml[config_Yaml['Database'] == database]['YAML_link'])
    except pyodbc.Error as ex:
        return(f"Error: no existing yaml for database {database}", ex)

    if len(Path) == 0:
        return (f"Error: no existing yaml for database {database}")
    else:
        Path_yaml = Path[0]
    
    #Connect to Azure database
    with open(Path_yaml, 'r') as stream:
        config = yaml.safe_load(stream)

    # Connection information
    username = config['database_credentials']['username']
    password = config['database_credentials']['password']
    host = config['database_credentials']['host']
    database = config['database_credentials']['database']
    port = config['database_credentials']['port']

    # Create connection string

    try:
        # Create connection
        conn_str = f"DRIVER=ODBC Driver 18 for SQL Server;SERVER={host};DATABASE={database};UID={username};PWD={password}"

        conn = pyodbc.connect(conn_str)

        cursor = conn.cursor()

        # Exécuter la requête pour récupérer les noms des colonnes
        query = f'''
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{table}'
        '''
        cursor.execute(query)
        
        # Récupérer les résultats
        columns = [row.COLUMN_NAME for row in cursor.fetchall()]
      
    except pyodbc.Error as ex:
        return("Error for connecting to database :", ex)
    finally:
        # Fermer la connexion
        conn.close()

    return columns


def Execute_query(database,query):           #Create new table in a database, add data, drop table etc..

        
    #Find Yaml path according to database value
    try:
        Path= list(config_Yaml[config_Yaml['Database'] == database]['YAML_link'])
    except pyodbc.Error as ex:
        return(f"Error: no existing yaml for database {database}", ex)

    if len(Path) == 0:
        return (f"Error: no existing yaml for database {database}")
    else:
        Path_yaml = Path[0]
    
    #Connect to Azure database
    with open(Path_yaml, 'r') as stream:
        config = yaml.safe_load(stream)

    # Connection information
    username = config['database_credentials']['username']
    password = config['database_credentials']['password']
    host = config['database_credentials']['host']
    database = config['database_credentials']['database']
    port = config['database_credentials']['port']

    # Create connection string

    try:
        # Create connection
        conn_str = f"DRIVER=ODBC Driver 18 for SQL Server;SERVER={host};DATABASE={database};UID={username};PWD={password}"

        conn = pyodbc.connect(conn_str)

        cursor = conn.cursor()
        cursor.execute(query)

        conn.commit()

    except pyodbc.Error as ex:
        raise Exception(f"An error occurred while executing the SQL query: {ex}") from ex
    finally:
        # Fermer la connexion
        conn.close()


def Add_data(df,database,table):            #Add data from df in table from database (if same structure)
    #Find Yaml path according to database value
    try:
        Path= list(config_Yaml[config_Yaml['Database'] == database]['YAML_link'])
    except pyodbc.Error as ex:
        return(f"Error: no existing yaml for database {database}", ex)

    if len(Path) == 0:
        return (f"Error: no existing yaml for database {database}")
    else:
        Path_yaml = Path[0]
    
    #Connect to Azure database
    with open(Path_yaml, 'r') as stream:
        config = yaml.safe_load(stream)

    # Connection information
    username = config['database_credentials']['username']
    password = config['database_credentials']['password']
    host = config['database_credentials']['host']
    database = config['database_credentials']['database']
    port = config['database_credentials']['port']

    # Create connection string

    try:
        # Create connection
        conn_str = f"DRIVER=ODBC Driver 18 for SQL Server;SERVER={host};DATABASE={database};UID={username};PWD={password}"

        conn = pyodbc.connect(conn_str)

        cursor = conn.cursor()

        insert_query = f"INSERT INTO {table} ([{'], ['.join(df.columns)}]) VALUES ({', '.join(['?' for _ in df.columns])})"
        
        # Insert data row by row
        rows_inserted = 0
        total_rows = len(df)
        for index, row in df.iterrows():
            cursor.execute(insert_query, tuple(row))
            rows_inserted += 1
            percent = round((rows_inserted/total_rows)*100,1)
            print(f"Inserted {rows_inserted}/{total_rows} rows into {table} ({percent}%)")

        #Commit change
        conn.commit()
        print("Data insertion completed.")

    except pyodbc.Error as ex:
        raise Exception(f"An error occurred while executing the SQL query: {ex}") from ex
    finally:
        # Fermer la connexion
        conn.close()