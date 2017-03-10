Apache Spark Recommendation/DataMinig Api Service

## Add Python dependency

sudo pip install flask 

##Run Command

add path your system  /spark-installion-path/sbin


spark-submit --master local --total-executor-cores 4 --executor-memory 3g server.py

## Example HTTp Request

http://127.0.0.1:5000/people/30