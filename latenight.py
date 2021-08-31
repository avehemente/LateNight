#Late Night
from flask import Flask, render_template, request, g, jsonify
import sqlite3, requests, json
from multiprocessing import Pool


#YELP API
key = "UW-jkH9pmyT-OyCZ1HjkjCxGiBWwURT8HayafB-95bURRgETCZVlDyJZq6uPfOsa-YgJR29pVb_RjQmTEYyxxc1YxLwRJsTVfIhvSXoM7GYRt1uIegeDyia_kh9CXnYx"
endpoint = "https://api.yelp.com/v3/businesses/search"
headers = {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + key}

def get_restaurants(lat, lon, rad=10000):
    params = {'limit': 10, 'radius': rad, 'latitude': lat, 'longitude': lon, 'categories': 'Restaurants', 'sort_by': 'rating'}
    response = requests.get(url = endpoint, params = params, headers = headers)
    if response.status_code == 200:
        return json.loads(response.content.decode('utf-8'))
    else:
        return None

#Neighboorhood API
def get_neighborhood(lat, lon):
    response = requests.get(url = "https://nominatim.openstreetmap.org/reverse?lat={0}&lon={1}&format=json".format(lat, lon))
    if response.status_code == 200:
        return json.loads(response.content.decode('utf-8'))
    else:
        return None

#Add Crime Score to Restaurants
def add_crime_score(rest):
    print("score added")
    address = get_neighborhood(rest["coordinates"]["latitude"], rest["coordinates"]["longitude"])["address"]
    if "neighbourhood" in address:
        rest["neighbourhood"] = address["neighbourhood"]
        score = query_db("select score from stats where neighborhood = ?", [rest["neighbourhood"]])
        if score:
            rest["crime"] = score[0][0]
    if len(rest["location"]["display_address"]) == 2:
        rest["address"] = rest["location"]["display_address"][0] + ", " + rest["location"]["display_address"][1]
    elif len(rest["location"]["display_address"]) == 1:
        rest["address"] = rest["location"]["display_address"][0]
    del rest["categories"]
    del rest["coordinates"]
    del rest["transactions"]
    del rest["location"]
    return rest


app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])
def home():
    with app.app_context():
        db = get_db()
        if request.method == "POST":
            print('server accessed')
            request_json = request.get_json(force=True)
            restaurants = get_restaurants(request_json['lat'], request_json['lon'])["businesses"]
            p = Pool(10)
            restaurants = p.map(add_crime_score, restaurants)
            rv = {"restaurants": restaurants}
            return rv
        else:
            return "Invalid Request"

#Database commands
DATABASE = './database.db'
def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(DATABASE)
    return db

@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()

def query_db(query, args=(), one=False):
    cur = get_db().execute(query, args)
    rv = cur.fetchall()
    cur.close()
    return (rv[0] if rv else None) if one else rv

if __name__ == '__main__':
    app.run(debug=True)