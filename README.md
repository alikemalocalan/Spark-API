Apache Spark Recommendation/DataMinig Api Service

## Add Python dependency

`sudo pip install flask`

##İnstall Spark Your System

For ubuntu; add spark path your system

`export HADOOP_HOME='/usr/local/hadoop'`


`export SPARK_HOME='/home/alikemal/Genel/spark-2.1.0-bin-hadoop2.7'`


`export PATH=$PATH:/usr/local/hadoop/bin/`


`export PATH=$PATH:/home/alikemal/Genel/spark-2.1.0-bin-hadoop2.7/bin/`


`export PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH`



##Run Command


`spark-submit server.py`


## Example HTTp Request

<http://127.0.0.1:5000/>