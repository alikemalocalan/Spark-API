from flask import Flask
from flask import jsonify

from InitSpark import InitSpark
from Recommendation import Recommendation

app = Flask(__name__)


@app.route('/')
def index():
    return jsonify(engine.getSongs())


@app.route('/songbyID/<trackID>')
def people(trackID):
    return jsonify(engine.getSongbyTrackID(trackID))


@app.route('/songbysinger/<singer_name>')
def songbySinger(singer_name):
    return jsonify(engine.songsbySinger(singer_name))


@app.route('/rank')
def song():
    return jsonify(engine.rating().collect())


@app.route('/recommend')
def recommend():
    return jsonify(engine.getRecommend())

if __name__ == '__main__':
    init = InitSpark()
    engine = Recommendation(init)
    app.run()
