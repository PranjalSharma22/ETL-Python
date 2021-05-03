import mysql.connector as msql
from mysql.connector import Error
import pandas as pd
from sqlalchemy import create_engine


#Connecting to MYSQL to create a database "Incubyte" and the intermediate table "Customer"
try:
    CONN = msql.connect(host='localhost', user='root', password='')
    if CONN.is_connected():
        cursor = CONN.cursor()
        cursor.execute("Drop database if exists Incubyte")
        cursor.execute("CREATE DATABASE Incubyte")
        print("Incubyte database is created")
        cursor.execute("use Incubyte")
        print("Creating Table...")
        cursor.execute("Drop table if exists Incubyte.Customer")
        cursor.execute("CREATE TABLE Incubyte.Customer(Name varchar(255) primary key,Cust_I varchar(18) not null,Open_Dt date not null,Consul_Dt date,VAC_ID char(5),DR_Name char(255),State char(5),Country char(5),DOB date,FLAG char(1))")
        print("Table is created")
        CONN.commit()
        
except Error as e:
    print("Error while connecting to MySQL", e)

    


#Reading CSV data file into pandas dataframe
DF = pd.read_csv("/home/pranjal/Documents/Incubyte/SourceDataFile.csv", delimiter='|')

#Dropping not required columns
DF = DF.drop(["Unnamed: 0", "H"], axis=1)

#Creating a dictionary to get the original names of columns
NAME_DICT = {'Customer_Name':'Name', 'Customer_Id':'Cust_I', 'Open_Date':'Open_Dt', 'Last_Consulted_Date':'Consul_Dt',
             'Vaccination_Id':'VAC_ID', 'Dr_Name':'DR_Name', 'State':'State', 'Country':'Country', 'DOB':'DOB', 'Is_Active':'FLAG'}

#Renaming dataframe column names
DF.rename(columns=NAME_DICT, inplace=True)

#Defining the data types of columns of dataframe
DF['Open_Dt'] = pd.to_datetime(DF['Open_Dt'], format='%Y%m%d')
DF['Consul_Dt'] = pd.to_datetime(DF['Consul_Dt'], format='%Y%m%d')
DF['DOB'] = pd.to_datetime(DF['DOB'], format='%d%m%Y')

#Loading dataframe into the intermediate Customer table
ENGINE = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}".format(user="root", pw="", db="Incubyte"))
DF.to_sql('Customer', con=ENGINE, if_exists='append', chunksize=1000, index=False)

print("Data loaded successfully into intermediate table")





#Creating Final Country wise tables and inserting data into that.

try:
    CONN = msql.connect(host='localhost', user='root', password='')

    if CONN.is_connected():
        cursor = CONN.cursor()
        cursor.execute("use Incubyte")
        cursor.execute("select distinct country from Incubyte.Customer") 
        COUNTRY_NAMES = cursor.fetchall()
        for country in COUNTRY_NAMES:
            cursor.execute("Drop table if exists table_{}".format(country[0]))
            query1 = "CREATE TABLE table_{} LIKE Customer".format(country[0])
            query2 = "INSERT INTO table_{} SELECT * FROM Customer where country='{}'".format(country[0], country[0])
            cursor.execute(query1)
            cursor.execute(query2)
        CONN.commit()
        print("Data Loaded Successfully into final respective tables")
        
except Error as e:
    print("Error while connecting to MySQL", e)


