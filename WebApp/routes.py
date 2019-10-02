from flask import Flask, escape, render_template
import psycopg2
# import jsonify
import config

app = Flask(__name__)

# home page
@app.route('/')
def home():
	return render_template("home.html")

# def home():
# 	output = ""
# 	with psycopg2.connect(dbname=config.DB_NAME, user=config.USER, password=config.PASSWORD,
# 		host=config.HOST, port=config.PORT) as connection:
# 		with connection.cursor() as cur:
# 			cur.execute("select * from noaa limit 1")
# 			test = cur.fetchall()
# 			for row in test:
# 				output = output + "\n" + str(row)
# 	cur.close()
# 	connection.close()

# 	return output