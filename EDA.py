import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime
import mysql.connector
spark=SparkSession.builder.master('local[1]').appName('Credit Card Management System').getOrCreate()

def spark_read(table):
    temp=spark.read\
            .format("jdbc")\
            .option("driver","com.mysql.cj.jdbc.Driver")\
            .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone")\
            .option("dbtable", table)\
            .option("user", "root")\
            .option("password", "admin")\
            .load()
    return temp

# Customer account details
df_cust=spark_read("cdw_sapp_customer")
df_cust.createOrReplaceTempView('df_cust')
# Branch details
df_branch = spark_read("cdw_sapp_branch")
df_branch.createOrReplaceTempView('df_branch')
#credit card details
df_credit = spark_read("cdw_sapp_credit_card")
df_credit.createOrReplaceTempView('df_credit')
#date_dim
date_dim = spark_read("date_dim")
date_dim.createOrReplaceTempView('date_dim')
# TRANSACTION DETAILS
# 1)    Used to display the transactions made by customers living in a given zip code for a given month and year. Order by day in descending order.

def customer_transactions():
    try:
        zipcode=int(input('Enter the zipcode of location you want to analyze: '))
        month=int(input('Enter month number: '))
        year=int(input('Enter year: '))
        query=f"select cr.TRANSACTION_ID,cr.CUST_CC_NO,cr.TRANSACTION_TYPE,cr.TRANSACTION_VALUE,cr.BRANCH_CODE,cr.CUST_SSN,\
                                        cu.FIRST_NAME,cu.LAST_NAME,cu.cust_zip,dd.calender_date\
                                        from df_credit cr join df_cust cu on cu.ssn=cr.cust_ssn \
                                        join date_dim dd on cr.timeid=dd.timeid \
                                        where cu.cust_zip={zipcode} \
        and dd.month_no={month} and year(dd.calender_date)= {year} order by dd.calender_date desc"
        df_trans=spark.sql(query)
        df_trans.show()
    except Exception as e:
        print("Please enter valid details")

#2)    Used to display the number and total values of transactions for a given type.

def transaction_value():
    try:
        type=input('Enter the type of transaction: ').title()
        query=f"select count(*) as No_of_Transactions,round(sum(transaction_value),2) as Total_Value from df_credit where transaction_type='{type}'"
        df_value=spark.sql(query)
        df_value.show()
    except Exception as e:
        print('Please enter a valid Transaction Type')

#3)    Used to display the total number and total values of transactions for branches in a given state.
def branch_transactions():
    try:
        state=input("Enter a State code to find its transaction details: ").upper()
        query=f"select count(*) as No_of_Transactions,round(sum(transaction_value),2) as Total_Value from df_credit join df_branch \
        on df_credit.branch_code= df_branch.branch_code where branch_state='{state}'"
        spark.sql(query).show()
    except Exception as e:
        print("Please enter valid State code")

## CUSTOMER DETAILS
# 4) Used to check the existing account details of a customer.
def cust_acct_details():
    try:
        ssn=int(input("Enter customer SSN: "))
        query=f"select cu.SSN,cu.FIRST_NAME,cu.LAST_NAME,cr.TRANSACTION_ID,cr.TRANSACTION_TYPE,cr.TRANSACTION_VALUE,br.* \
            from df_credit cr join df_cust cu on cu.ssn=cr.cust_ssn \
        join df_branch br on cr.branch_code=br.branch_code\
        where cu.ssn={ssn}"
        spark.sql(query).show()
    except Exception as e:
        print("Please enter valid customer ssn")

#5) Used to modify the existing account details of a customer.
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="admin",
  database="creditcard_capstone"
)
mycursor = mydb.cursor()

def modify_acct():
    try:
        ssn=int(input("Enter customer SSN: "))
        column=input("Enter column name which you want to update: ")
        new_value=input("Enter value you want to update: ")
        query = f"UPDATE cdw_sapp_customer SET {column} = '{new_value}' WHERE ssn = {ssn}"
        mycursor.execute(query)
        mydb.commit()
        print(mycursor.rowcount, "record(s) affected")
        df_cust.createOrReplaceTempView('df_cust')
        query=f'select * from df_cust where ssn={ssn}'
        spark.sql(query).show()
    except Exception as e:
        print("Please enter valid details")
#6) Used to generate a monthly bill for a credit card number for a given month and year.
def ccno_monthly_bill():
    try:
        ccno=input("Enter the credit card number to generate monthly bill: ")
        mnth=int(input("Enter the Month: "))
        yr=int(input("Enter year: "))
        query=f"select SUM(Transaction_value) as Montly_Bill from df_credit join date_dim \
                on df_credit.timeid=date_dim.timeid\
                where date_dim.month_no={mnth} and YEAR(date_dim.calender_date)={yr} and cust_cc_no={ccno}"
        spark.sql(query).show()
    except Exception as e:
        print("Please enter valid details")
#7) Used to display the transactions made by a customer between two dates. Order by year, month, and day in descending order

def trans_btw_dates():
    try:
        ssn=input("Enter the customer ssn number to generate monthly bill: ")
        start_date=input("Enter the start date (yyyy-mm-dd): ")
        end_date=input("Enter the end date(yyyy-mm-dd): ")
        query=f"select dc.*,dd.calender_date from df_credit dc join date_dim dd on dc.timeid=dd.timeid where cust_ssn = '{ssn}' \
                and dd.calender_date between '{start_date}' and '{end_date}' order by dd.calender_date desc"
        spark.sql(query).show()
    except Exception as e:
        print("Please enter valid details")

condition=True
while condition:
    try:
        print('''
                1. Display transactions made by customers in a given zipcode for a given month and year.
                2. count and total values based on transaction type
                3. count and total values of transactions for branches in given state
                4. Check the Account Details of a customer
                5. To modify existing account details of a customer
                6. To generate monthly bill for a credit card number for a given month and year
                7. Display the transactions made by a customer between two dates. Order by year, month, and day in descending order
                8: To exit
            ''')
        id=int(input('What do you want to analyze?: '))
        choices={1:customer_transactions,2:transaction_value,3:branch_transactions,4:cust_acct_details,5:modify_acct,
                6:ccno_monthly_bill,7:trans_btw_dates}
        if id in choices.keys():
            choices[id]()
        else:
            if input("Do you want to continue(y/n): ")!='y':
                condition=False
    except Exception as e:
        print("Something went wrong, Please try again")