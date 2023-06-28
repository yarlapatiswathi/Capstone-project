FROM apache/airflow:2.6.2

USER root

# Install OpenJDK-11
RUN apt update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get install -y default-libmysqlclient-dev && \
    apt-get install -y ant && \
    apt-get install wget && \
    apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /requirements.txt
# Install MySQL connector for Python
RUN pip install mysql-connector-python
RUN pip install apache-airflow-providers-mysql
RUN pip install apache-airflow-providers-apache-spark
# # # Download the MySQL JDBC driver
# RUN mkdir -p /usr/local/spark/jars/
# RUN wget https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.27/mysql-connector-java-8.0.27.jar && \
#     cp mysql-connector-java-8.0.27.jar /opt/airflow/dags

COPY cdw_sapp_custmer.json /cdw_sapp_custmer.json
COPY cdw_sapp_branch.json /cdw_sapp_branch.json
COPY cdw_sapp_credit.json /cdw_sapp_credit.json
COPY Loan_Data.csv /Loan_Data.csv
COPY Date_Dim.csv /Date_Dim.csv

COPY --chown=airflow:root ./dags /opt/airflow/dags