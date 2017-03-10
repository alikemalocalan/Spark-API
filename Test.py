spark = SparkSession \
    .builder \
    .appName("PythonWordCount") \
    .getOrCreate()
sc = spark.sparkContext

tracks_txt = "dataset/unique_tracks.txt"
peoples_txt = "dataset/people.txt"

track_line = sc.textFile(tracks_txt)
track_parts = track_line.map(lambda l: l.split(","))
songs = track_parts.map(lambda p: Row(trackID=p[0], songID=p[1], artistName=p[2], songTitle=p[3]))

# Infer the schema, and register the DataFrame as a table.
schemaSongs = spark.createDataFrame(songs)
schemaSongs.createOrReplaceTempView("song")

result = schemaSongs \
    .filter(schemaPeople.columns('artistName') == '') \
    .limit(20) \
    .show()
