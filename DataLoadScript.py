import mysql.connector as msql
from mysql.connector import Error
import pandas as pd
from sqlalchemy import create_engine


#Connecting to MYSQL to create a database "Incubyte" and the intermediate table "Customer"
try:
    conn = msql.connect(host='localhost', user='root', password='')
    if conn.is_connected():
    	cursor = conn.cursor()
    	cursor.execute("Drop database if exists Incubyte")
    	cursor.execute("CREATE DATABASE Incubyte")
    	print("Incubyte database is created")
    	cursor.execute("use Incubyte")
    	print("Creating Table...")
    	cursor.execute("Drop table if exists Incubyte.Customer")
    	cursor.execute("CREATE TABLE Incubyte.Customer(Name varchar(255) primary key,Cust_I varchar(18) not null,Open_Dt date not null,Consul_Dt date,VAC_ID char(5),DR_Name char(255),State char(5),Country char(5),DOB date,FLAG char(1))")
    	print("Table is created")
    	conn.commit()
        
except Error as e:
    print("Error while connecting to MySQL", e)

    


#Reading CSV data file into pandas dataframe
df = pd.read_csv("/home/pranjal/Documents/Incubyte/SourceDataFile.csv", delimiter='|')

#Dropping not required columns
df = df.drop(["Unnamed: 0", "H"], axis=1)

#Creating a dictionary to get the original names of columns
NameDict = {'Customer_Name':'Name', 'Customer_Id':'Cust_I', 'Open_Date':'Open_Dt', 'Last_Consulted_Date':'Consul_Dt', 'Vaccination_Id':'VAC_ID', 'Dr_Name':'DR_Name', 'State':'State', 'Country':'Country', 'DOB':'DOB', 'Is_Active':'FLAG'}

#Renaming dataframe column names
df.rename(columns=NameDict, inplace=True)

#Defining the data types of columns of dataframe
df['Open_Dt'] = pd.to_datetime(df['Open_Dt'], format='%Y%m%d')
df['Consul_Dt'] = pd.to_datetime(df['Consul_Dt'], format='%Y%m%d')
df['DOB'] = pd.to_datetime(df['DOB'], format='%d%m%Y')

#Loading dataframe into the intermediate Customer table  
engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}".format(user="root", pw="", db="Incubyte"))
df.to_sql('Customer', con=engine, if_exists='append', chunksize=1000, index=False)

print("Data loaded successfully into intermediate table")





#Creating Final Country wise tables and inserting data into that.

try:
    conn = msql.connect(host='localhost', user='root',  
                        password='')

    if conn.is_connected():
    	cursor = conn.cursor()
    	cursor.execute("use Incubyte")
    	cursor.execute("select distinct country from Incubyte.Customer") 
    	country_names = cursor.fetchall()
    	for country in country_names:
    		cursor.execute("Drop table if exists table_{}".format(country[0]))
    		query1 = "CREATE TABLE table_{} LIKE Customer".format(country[0])
    		query2 = "INSERT INTO table_{} SELECT * FROM Customer where country='{}'".format(country[0], country[0])
    		cursor.execute(query1)
    		cursor.execute(query2)
    	conn.commit()
    	print("Data Loaded Successfully into final respective tables")
        
except Error as e:
    print("Error while connecting to MySQL", e)


