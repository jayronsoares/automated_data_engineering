import os
import pandas as pd
from pandas.io import sql
from sqlalchemy import create_engine
from sqlalchemy.pool import Pool
import mysql.connector
import json

# Changing the DIR
os.chdir("C:\\PYTHON")

try:
   
    
    ################################################################
    # LOADING THE DATA
    ################################################################


    print('###################################### \n')
    print("STEP 1: EXTRACT DATA")
    
    # Load the log file equipment_failure_sensors.log
    df_failure_sensors = pd.read_csv("equipment_failure_sensors.log",
                                     sep="\t",
                                     engine='python',
                                     names = ["log_date","ERROR", "sensor_id","to_drop_column","temperature","vibration"],
                                     usecols=[0,1,2,3,4,5],
                                     header=None,
                                     index_col=False)

    # Load the log file equipment_sensors.csv
    df_equipment_sensors = pd.read_csv("equipment_sensors.csv",
                                       sep=";",
                                       usecols=['equipment_id', 'sensor_id'],
                                       encoding='utf8')

    # Load the log file equipment.json
    arq=open('equipment.json').read()
    json_object = json.loads(arq)

    ################################################################
    # CLEANING THE DATA
    ################################################################

    print('###################################### \n')
    print("STEP 2: DATA WRANGLING \n")
    
    # Remove special characters
    df_failure_sensors = df_failure_sensors.replace(to_replace=r'[^0-9+-]',value='',regex=True)
    
    # Remove unwanted columns
    df_failure_sensors.drop(columns=['ERROR', 'to_drop_column'], inplace=True, axis=1)

    # A simple adjust at LOG_DATE column
    df_failure_sensors['log_date'] = df_failure_sensors['log_date'].str.replace(r'\D+','')
    df_failure_sensors['log_date'] = pd.to_datetime(df_failure_sensors['log_date'], errors='coerce')

    def chek_for_missing_zero_values(df):
        zero_val = (df == 0.00).astype(int).sum(axis=0)
        mis_val = df.isnull().sum()
        mis_val_percent = 100 * df.isnull().sum() / len(df)
        mz_table = pd.concat([zero_val, mis_val, mis_val_percent], axis=1)
        mz_table = mz_table.rename(
        columns = {0 : 'Zero Values', 1 : 'Missing Values', 2 : '% of Total Values'})
        mz_table['Total Zero Missing Values'] = mz_table['Zero Values'] + mz_table['Missing Values']
        mz_table['% Total Zero Missing Values'] = 100 * mz_table['Total Zero Missing Values'] / len(df)
        mz_table['Data Type'] = df.dtypes
        mz_table = mz_table[mz_table.iloc[:,1] != 0].sort_values('% of Total Values', ascending=False).round(1)
        print ("The dataframe has " + str(df.shape[1]) + " columns and " + str(df.shape[0]) + " Rows.\n"
                       "There are " + str(mz_table.shape[0]) +" columns that have missing values.")
        return mz_table

    print("## CHECK FOR MISSING OR ZERO VALUES\n")
    chek_for_missing_zero_values(df_failure_sensors)
    print()
    
    chek_for_missing_zero_values(df_equipment_sensors)
    print()
    

    # Remove empy blank space
    df_failure_sensors.columns = [x.strip().replace(' ', "") for x in df_failure_sensors.columns]
    df_equipment_sensors.columns = [x.strip().replace(' ', "") for x in df_equipment_sensors.columns]
    

    ################################################################
    # CREATING TABLES
    ################################################################
    
     # Create a database connection -- DATABASE TARGET
    db_connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="password",
        database="db_urano", # schema used to be load
        auth_plugin='mysql_native_password'
        )
    # Create a MySQL cursor to process the steps
    db_cursor = db_connection.cursor()
    

    print('###################################### \n')
    print("\nSTEP 3: CREATING TABLES ")
    
    # Create tables
    query=["CREATE TABLE equipment_failure_sensors (log_date VARCHAR(200), sensor_id VARCHAR(100), temperature VARCHAR(100), vibration VARCHAR(100))",
           "CREATE TABLE equipment (equipment_id VARCHAR(100), code VARCHAR(100), group_name VARCHAR(100))",
           "CREATE TABLE equipment_sensors (equipment_id VARCHAR(100), sensor_id VARCHAR(100))"]
    for i in query:
        db_cursor.execute(i)
        db_connection.commit()


    ################################################################
    # INSERTING RECORDS
    ################################################################

    print('###################################### \n')
    print("STEP 4: INSERTING RECORDS INTO THE TABLES")
    
    try:
        # Insert records from LOG file.
        cols = "`,`".join([str(i) for i in df_failure_sensors.columns.tolist()])
        for i,row in df_failure_sensors.iterrows():
            sql = "INSERT INTO `equipment_failure_sensors` (`" +cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
            db_cursor.execute(sql, tuple(row))

            
        # Insert records from JSON file.
        for item in json_object:
            equipment_id = item["equipment_id"]
            code = item["code"]
            group_name = item["group_name"]
            db_cursor.execute("INSERT INTO `equipment` (`equipment_id`, `code`,`group_name`) VALUES (%s, %s, %s)",(equipment_id, code,group_name))


        # Insert records from CSV file.
        engine = create_engine('mysql+pymysql://root:password@localhost:3306/db_urano', echo_pool=True, pool_size=10, max_overflow=20 )
        with engine.connect() as conn, conn.begin():
            df_equipment_sensors.to_sql('equipment_sensors', conn, index=False, if_exists='append')


        print("ALL RECORDS SUCCESSFULLY LOADED!\n")
        
        ################################################################
        # RETRIEVING THE RECORDS
        ################################################################
        
        # Total equipment failures that happened?
        total = """SELECT SUM(TotalEquipmentFailures) AS TOTAL_OF_FAILURES
                        FROM(
                            SELECT COUNT(*) AS TotalEquipmentFailures
                            FROM equipment_failure_sensors as ef
                            LEFT JOIN equipment_sensors as es
                                ON ef.sensor_id = es.sensor_id
                            LEFT JOIN equipment as eq
                                ON eq.equipment_id=es.equipment_id
                            WHERE EXTRACT(YEAR_MONTH FROM log_date) = '202001'
                            GROUP BY eq.equipment_id
                        ) as Failures;"""
        
        db_cursor.execute(total)
        records = db_cursor.fetchall()
        
        print("All the answers are related to January 2020\n")
        print("\nTotal equipment failures that happened?\n")
        for row in records:
            print("TOTAL OF FAILURES = ", row[0])

        # Which equipment code had most failures?
        code = """ SELECT CODE
                     FROM(
                             SELECT eq.CODE, COUNT(*) AS TotalEquipmentFailures
                             FROM equipment_failure_sensors as ef
                             LEFT JOIN equipment_sensors as es	
                                    ON ef.sensor_id = es.sensor_id
                             INNER JOIN equipment as eq
                                   ON eq.equipment_id=es.equipment_id
                             WHERE EXTRACT(YEAR_MONTH FROM log_date) = '202001'
                             GROUP BY eq.CODE, eq.equipment_id
                                ) as Failures
                    ORDER BY TotalEquipmentFailures DESC
                    LIMIT 1;"""
        
        db_cursor.execute(code)
        records_failures = db_cursor.fetchall()
        
        print("\nWhich equipment code had most failures?\n")
        for row in records_failures:
            print("EQUIPMENT CODE = ", row[0])

        # Average amount of failures across equipment group, ordering by the amount of failures in ascending order?
        average = """
                    SELECT group_name as EQUIPMENT_GROUP, 
                           round(AVG(TotalEquipmentFailures),2) as AVG_AMOUNT_FAILURES
                    FROM(
                            SELECT eq.group_name, ef.sensor_id, COUNT(*) AS TotalEquipmentFailures
                            FROM equipment_failure_sensors as ef
                            LEFT JOIN equipment_sensors as es	
                                    ON ef.sensor_id = es.sensor_id
                            INNER JOIN equipment as eq
                                    ON eq.equipment_id=es.equipment_id
                            WHERE EXTRACT(YEAR_MONTH FROM log_date) = '202001'
                            GROUP BY es.equipment_id
                            ) as Failures
                    group by group_name
                    order by count(sensor_id) ASC; """
        
        db_cursor.execute(average)
        average_failures = db_cursor.fetchall()
        
        print("\nAverage amount of failures across equipment group, ordering by the amount of failures in ascending order?\n")
        for row in average_failures:
            print("EQUIPMENT GROUP = ", row[0],
                  "AVERAGE OF FAILURES = ", row[1])
        
            
    except Exception as error:
        print("Exception occurred: {}",format(error))

    finally:
        db_connection.commit()


except Exception as e:
    print("Exception occurred: {}",format(e))
    pass
    
finally:
    db_connection.close()

if __name__ == "__main__":
    print("\nPipeline ETL processed sucessfully\n")
else:
    print("Error ocurred during the execution")
