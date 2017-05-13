import flask
from flask import Flask, request, jsonify

from Recommendation import Recommendation
from UserMining import Mining

app = Flask(__name__)


@app.after_request
def after_request(resp):
    resp.headers.add('Access-Control-Allow-Origin', '*')
    resp.headers.add('Access-Control-Allow-Headers', 'Content-Type, X-Token')
    resp.headers.add('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE')
    resp.headers['Content-Type'] = 'application/json'
    resp.headers['server'] = 'BitirmeServer'
    return resp


@app.route('/')
def index():
    return "ok"


# /searchSong?q=sarki_adi
@app.route('/searchSong')
def people():
    param = request.args.get('q')
    if param:
        return jsonify(engine.songsbySongName(param))
    if not param:
        return "Parametre Hatası"


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
    userid = int(request.args.get('userid'))
    print(userid)
    if (typeparam == 0):
        result = engine.listSong().toJSON().collect()
        return flask.Response(result)
    if (typeparam == 1):
        result = engine.ratingbyUserID(userid)
        return jsonify(result)
    if (typeparam == 2):
        result = engine.getRecommend(userid).toJSON().collect()
        return flask.Response(result)
    if (typeparam == 3):
        return jsonify(request.args)
    if (typeparam == 4):
        return jsonify(request.args)


mining = Mining()


@app.route('/test')
def test():
    result = mining.mean_users_age()
    return flask.Response(result)


if __name__ == '__main__':
    engine = Recommendation()
    app.run(host='0.0.0.0')
