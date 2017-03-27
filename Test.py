from flask import Flask, jsonify
from flask import request
from flask_request_params import bind_request_params
from pyspark import Row
from pyspark.sql import SparkSession

from LogModels import UserLog


def sparktest():
    spark = SparkSession \
        .builder \
        .appName("Spark Test") \
        .getOrCreate()

    sc = spark.sparkContext

    tracks_txt = "dataset/unique_tracks.txt"

    track_raw_RDD = sc.textFile(tracks_txt)

    track_parts = track_raw_RDD.map(lambda l: l.split(",")) \
        .map(lambda p: Row(trackID=p[0], songID=p[1], artistName=p[2], songTitle=p[3])) \
        .cache()

    schemaSongs = spark.createDataFrame(track_parts)
    schemaSongs.createOrReplaceTempView("song")
    print("\n\n AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")

    result = schemaSongs \
        .limit(10) \
        .orderBy(schemaSongs.artistName)
    print("\n\n BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB")

    result1 = result \
        .limit(10) \
        .orderBy(result.songTitle) \
        .show()

    print("\n\n CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC")


app = Flask(__name__)


def save_request(request):
    req_data = {}
    req_data['endpoint'] = request.endpoint
    req_data['method'] = request.method
    req_data['data'] = request.data
    req_data['headers'] = dict(request.headers)
    req_data['args'] = request.args
    req_data['form'] = request.form
    req_data['remote_addr'] = request.remote_addr
    return req_data


def save_response(resp):
    resp_data = {}
    resp_data['status_code'] = resp.status_code
    resp_data['status'] = resp.status
    resp_data['headers'] = dict(resp.headers)
    resp_data['data'] = resp.response
    return resp_data


app.before_request(bind_request_params)


@app.before_request
def before_request():
    print(request.method, request.path, jsonify(request.params))


@app.after_request
def after_request(resp):
    resp.headers.add('Access-Control-Allow-Origin', '*')
    resp.headers.add('Access-Control-Allow-Headers', 'Content-Type, X-Token')
    resp.headers.add('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE')
    resp.headers['Content-Type'] = 'application/json'
    resp.headers['server'] = 'BitirmeServer'
    save_response(resp)
    return resp


@app.route('/')
def index():
    UserLog(email="alikemal@gmail.com",
            # Use `force_insert` so that we get a DuplicateKeyError if
            # another user already exists with the same email address.
            # Without this option, we will update (replace) the user with
            # the same id (email).
            password="123456").save(force_insert=True)
    return jsonify(request.params)


if __name__ == '__main__':
    sparktest()
