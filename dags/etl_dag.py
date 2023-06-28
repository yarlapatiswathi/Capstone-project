from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
#from airflow.hooks.mysql_hook import MySqlHook
from airflow.providers.mysql.hooks.mysql import MySqlHook

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pandas as pd

default_args={
    'owner':'Swathi',
    'retries':0,
    'retry_delay':timedelta(minutes=2)
}



#Write to MySQL Table
def write_to_db(df,table_name):
    try:
        # Fetch the MySQL connection details using Airflow's MySqlHook
        mysql_conn_id = 'mysql_con'  # Airflow connection ID for MySQL
        mysql_hook = MySqlHook(mysql_conn_id)
        connection = mysql_hook.get_connection(mysql_conn_id)
        host = connection.host
        port = connection.port
        schema = connection.schema
        login = connection.login
        password = connection.password

        df.write.format("jdbc")\
        .option("driver","com.mysql.cj.jdbc.Driver")\
        .option("url", f'jdbc:mysql://{host}:{port}/{schema}')\
        .option("dbtable", table_name)\
        .option("user", f'{login}')\
        .option("password", f'{password}')\
        .save()
        print(f'{table_name} table is created')
    except Exception as e:
        print('Error while connecting to MySQL',e)


#checking for nulls
def check_nulls(df):
    navalues=df.select([
    (
        count(when((isnan(c) | col(c).isNull()), c)) if t not in ("timestamp", "date")
        else count(when(col(c).isNull(), c))
    ).alias(c)
    for c, t in df.dtypes if c in df.columns])
    return navalues
def cust_transformation(df_cust):
    print('checking for Nulls and duplicates...')
    df_cust.groupBy(df_cust.columns).count().where('count>1').show()
    #df_cust.distinct().count())==(df_cust.count():
    check_nulls(df_cust).show()
    df_cust=df_cust.withColumn('SSN',col('SSN').cast('int'))
    df_cust=df_cust.withColumn('CUST_ZIP',col('CUST_ZIP').cast('int'))
    df_cust=df_cust.withColumn('LAST_UPDATED',to_timestamp('LAST_UPDATED'))
    df_cust=df_cust.withColumn('FIRST_NAME',initcap(col('FIRST_NAME')))
    df_cust=df_cust.withColumn('MIDDLE_NAME',lower('MIDDLE_NAME'))
    df_cust=df_cust.withColumn('LAST_NAME',initcap(col('LAST_NAME')))
    df_cust=df_cust.withColumn('FULL_STREET_ADDRESS',concat_ws(',',df_cust.APT_NO,df_cust.STREET_NAME))
    df_cust=df_cust.withColumn('CUST_PHONE',regexp_replace(rpad(col('CUST_PHONE'),10,'0'),r'^(\d{3})(\d{3})(\d{4})$','($1)$2-$3'))
    df_cust=df_cust.drop(*['APT_NO','STREET_NAME'])
    col_order=['SSN','FIRST_NAME','MIDDLE_NAME','LAST_NAME','FULL_STREET_ADDRESS','CUST_CITY','CUST_STATE',
     'CUST_COUNTRY','CUST_ZIP','CUST_PHONE','CUST_EMAIL','LAST_UPDATED']
    print('Customer details -- Transformation done and Writing to Database')
    df_cust=df_cust.select(*col_order)
    df_cust.show(3)
    write_to_db(df_cust,'CDW_SAPP_CUSTOMER')

def branch_transformation(df_branch):
    print('Checking for Null values and Duplicates in branch...')
    check_nulls(df_branch).show()
    df_branch.groupBy(df_branch.columns).count().where('count>1').show()
    # (df_branch.distinct().count())==(df_branch.count())
    cols=['BRANCH_CODE','BRANCH_ZIP']
    for col_name in cols:
        df_branch=df_branch.withColumn(col_name,col(col_name).cast('int'))
    df_branch=df_branch.withColumn('BRANCH_ZIP',coalesce(col('BRANCH_ZIP'),lit(999999)))
    df_branch=df_branch.withColumn('BRANCH_PHONE',regexp_replace(col('BRANCH_PHONE'),'^(\d{3})(\d{3})(\d{4})$','($1)$2-$3'))
    df_branch=df_branch.withColumn('LAST_UPDATED',to_timestamp('LAST_UPDATED'))
    col_order=['BRANCH_CODE','BRANCH_NAME','BRANCH_STREET','BRANCH_CITY','BRANCH_STATE','BRANCH_ZIP','BRANCH_PHONE','LAST_UPDATED']
    df_branch=df_branch.select(*col_order)
    print('Branch details -- Transformation done and Writing to Database')
    df_branch.show(3)
    write_to_db(df_branch,'CDW_SAPP_BRANCH')

def credit_transformation(df_credit):
    print('Checking for Null values and Duplicates in Credit_Card details...')
    check_nulls(df_credit).show()
    # #checking for duplicates
    # print(df_credit.distinct().count()==df_credit.count())
    df_credit.groupBy(df_credit.columns).count().where('count>1').show()
    # Convert day, month, and year to timeid
    df_credit = df_credit.withColumn("TIMEID",concat(df_credit["year"].cast("string"),
            lpad(df_credit["month"].cast("string"), 2, "0"),lpad(df_credit["day"].cast("string"), 2, "0")))
    col_name=['CUST_SSN','BRANCH_CODE','TRANSACTION_ID','TIMEID']
    for cols in col_name:
        df_credit=df_credit.withColumn(cols,col(cols).cast('int'))
    df_credit=df_credit.withColumnRenamed("credit_card_no", "CUST_CC_NO")
    # df_credit = df_credit.withColumn("TIMEID", expr("make_date(year, month, day)"))
    df_credit=df_credit.drop(*['DAY','MONTH','YEAR'])
    col_order=['TRANSACTION_ID','CUST_CC_NO','TRANSACTION_TYPE','TRANSACTION_VALUE','BRANCH_CODE','CUST_SSN','TIMEID']
    df_credit=df_credit.select(*col_order)
    print('Credit_card details -- Transformation done and Writing to Database')
    df_credit.show(3)
    write_to_db(df_credit,'CDW_SAPP_CREDIT_CARD')

def date_dim(df_date):
    df_date=df_date.withColumn('calender_date',col('calender_date').cast('date'))
    df_date.show(3)
    write_to_db(df_date,'Date_Dim')

def loan_etl():
    print('Extracting Loan details using API')
    import requests
    url = 'https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json'
    resp = requests.get(url).json()
    df_loan=pd.DataFrame(columns=['Application_ID', 'Gender',
    'Married','Dependents','Education','Self_Employed',
    'Credit_History','Property_Area','Income','Application_Status'])
    for i in range(len(resp)):
        tempDf=pd.DataFrame([{'Application_ID':resp[i]['Application_ID'],
                    'Gender':resp[i]['Gender'],
                    'Married':resp[i]['Married'],
                    'Dependents':resp[i]['Dependents'],
                    'Education':resp[i]['Education'],
                    'Self_Employed':resp[i]['Self_Employed'],
                    'Credit_History':resp[i]['Credit_History'],
                    'Property_Area':resp[i]['Property_Area'],
                    'Income':resp[i]['Income'],
                    'Application_Status':resp[i]['Application_Status']}])
        df_loan=pd.concat([df_loan,tempDf],ignore_index=True )
    df_loan.to_csv('Loan_Data.csv',index=False)
        #df_loan=loan_etl()
    schema=StructType([
    StructField('Application_ID',StringType()),
    StructField('Gender',StringType()),
    StructField('Married',StringType()),
    StructField('Dependents',StringType()),
    StructField('Education',StringType()),
    StructField('Self_Employed',StringType()),
    StructField('Credit_History',IntegerType()),
    StructField('Property_Area',StringType()),
    StructField('Income',StringType()),
    StructField('Application_Status',StringType())])

    # spark_df=spark.createDataFrame(df_loan,schema=schema)
    # print('conversion done')
    spark=SparkSession.builder.master('local[1]').appName('Credit Card Management System')\
        .config("spark.jars", "mysql-connector-java-8.0.23.jar").getOrCreate()
    spark_df=spark.read.csv('Loan_Data.csv',schema=schema)
    print('Loan Application detials -- Transformation done and Writing to Database')
    spark_df.show(3)
    write_to_db(spark_df,'CDW_SAPP_loan_application')
    print('ETL PROCESS DONE')


def data_etl():
    spark=SparkSession.builder.master('local[1]').appName('Credit Card Management System')\
        .config("spark.jars", "mysql-connector-java-8.0.23.jar").getOrCreate()
    print('Working on cdw_sapp_custmer.json...')
    df_cust=spark.read.json('cdw_sapp_custmer.json')
    cust_transformation(df_cust)
    print('cdw_sapp_custmer.json loaded to database after transformation')
    print()
    print('Working on cdw_sapp_branch.json...')
    df_branch=spark.read.json('cdw_sapp_branch.json')
    branch_transformation(df_branch)
    print('cdw_sapp_branch.json loaded to database after transformation')
    print()
    print('Working on cdw_sapp_credit.json...')
    df_credit=spark.read.json('cdw_sapp_credit.json')
    credit_transformation(df_credit)
    print('cdw_sapp_credit.json loaded to database after transformation')
    df_date=spark.read.format("csv")\
        .option('header','true').option('inferSchema','true')\
        .load('Date_Dim.csv')
    date_dim(df_date)

with DAG(
    dag_id='etl_dag',
    default_args=default_args,
    description='dag_python',
    start_date=datetime(2023,6,25),
    schedule_interval='@daily'
    ) as dag:

    ETL_PIPELINE_JSON=PythonOperator(
        task_id='ETL_PIPELINE_JSON',
        python_callable=data_etl
    )
    ETL_PIPELINE_Loan=PythonOperator(
    task_id='ETL_PIPELINE_Loan',
    python_callable=loan_etl
    )
ETL_PIPELINE_JSON>>ETL_PIPELINE_Loan