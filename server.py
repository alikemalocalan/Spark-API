# -*- coding: utf-8 -*- 
# !/usr/bin/env python
import flask
from flask import Flask, request, jsonify, render_template, make_response, url_for

from Recommendation import Recommendation
from UserMining import Mining

app = Flask(__name__, template_folder='templates')


@app.after_request
def after_request(resp):
    resp.headers.add('Access-Control-Allow-Origin', '*')
    resp.headers.add('Access-Control-Allow-Headers', 'Content-Type, X-Token')
    resp.headers.add('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE')
    if url_for("index") == request.path:
        resp.mimetype = 'text/html'
    else:
        resp.headers['Content-Type'] = 'application/json'
    resp.headers['server'] = 'BitirmeServer'
    return resp


@app.route('/')
def index():
    resp = make_response(render_template('index.html'))
    return resp


# anasayfa  mainStatistics
@app.route('/mainStatistics')
def mainStatistics():
    return jsonify("ok")


# sidebar /userStatistics?userId=userid&sarkiId=sarkiid&genreId=genreid&
@app.route('/userStatistics')
def songbySinger():
    return 'ok'


# /playlistRecommendation?type=1,2,3,4&userId=useridblabla&ulkeId=ulkeid&yas=22
@app.route('/playlistRecommendation', methods=['GET'])
def song():
    typeparam = int(request.args.get('type'))
    userid = request.args.get('userid')
    if typeparam == 0:
        result = engine.listPopulerSong()
        return flask.Response(jsonify(result))
    elif typeparam == 1:
        result = engine.ratingbyUserID(int(userid))
        return jsonify(result)
    elif typeparam == 2:
        result = engine.listPopulerGenre(int(userid))
        return flask.Response(result)
    elif typeparam == 3:
        return None
    elif typeparam == 4:
        return jsonify(request.args)


mining = Mining()


@app.route('/test', methods=['POST'])
def test():
    text = request.form['text']
    result = engine.spark.sql(text).toJSON().collect()
    return flask.Response(result)


if __name__ == '__main__':
    engine = Recommendation()
    app.run(host='0.0.0.0')
