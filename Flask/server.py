from flask import Flask,render_template, request,jsonify,Response
from flask_sqlalchemy import SQLAlchemy
import pickle
import pandas as pd
import psycopg2 as pg2



app = Flask(__name__)

app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://@localhost/nba_capstone'
db = SQLAlchemy(app)

class predictions(db.Model):
    __tablename__= 'predictions_2018'

    season = db.Column(db.Integer)
    player = db.Column(db.String(60), primary_key=True)
    predicted_rank = db.Column(db.Integer)
    points = db.Column(db.Float)
    rebounds = db.Column(db.Float)
    assists = db.Column(db.Float)
    steals = db.Column(db.Float)
    blocks = db.Column(db.Float)
    turnovers = db.Column(db.Float)
    threes_made = db.Column(db.Float)
    fg_percent = db.Column(db.Float)
    ft_percent = db.Column(db.Float)


    def __init(self,season,player,predicted_rank,points,rebounds,assists,steals,blocks,turnovers,threes_made,FGM,FGA,FTM,FTA):
        self.season  = season
        self.player = player
        self.predicted_rank = predicted_rank
        self.points  =  points
        self.rebounds  = rebounds
        self.assists  =  assists
        self.steals  =  steals
        self.blocks  =  blocks
        self.turnovers  =  turnovers
        self.threes_made  =  threes_made
        self.fg_percent  =  fg_percent
        self.ft_percent  =  ft_percent





@app.route('/',methods=['GET'])
def home():
    predict_player = predictions.query.limit(150)
    return render_template('home.html',players=predict_player)

@app.route('/list_players')
def list_players():
    predict_player = predictions.query.all()
    return render_template('list_players.html',players=predict_player)



'''Now if server.py and open the website http://0.0.0.0:3333
If you changed your route to /home you would need to go to
http://0.0.0.0:3333/home
'''
if __name__ == '__main__':
    app.run(host ='0.0.0.0', port = 3333, debug = True)
