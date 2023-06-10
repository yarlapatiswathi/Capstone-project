from flask import Flask
from flask_mysqldb import MYSQL

app = Flask(__name__)
app.config['MYSQL_HOST']='localhost'
app.config['MYSQL_USER']='root'
app.config['MYSQL_PASSWORD']='admin'
app.config['MYSQL_DB']='creditcard_capstone'

db=MYSQL(app)

from application import routes