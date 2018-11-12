from sklearn.linear_model import LinearRegression
from sklearn.ensemble import GradientBoostingRegressor,RandomForestRegressor
from sklearn.model_selection import train_test_split,GridSearchCV
import keras
from keras.models import Sequential, Model, Input
from keras.layers import Dense, Dropout, Activation
from sklearn.model_selection import train_test_split
import tensorflow as tf
import numpy as np
import scipy.stats as scs


def class run_test:
    def __init__(self,X,y,test_perc=0.3,holdout_year=None)
        if holdout_year:
            self.X =X[X['season']!=holdout_year].drop(['season'])
            self.y = y[X['season']!=holdout_year]
        else:
            self.X = X
            self.y = y
        
        self.X_train,self.X_test,self.y_train,self.y_test = train_test_split(self.X,self.y,test_size=test_perc)
        self.input_dim=X_train.shape[1]
    
    def run_NN(self,batch=128,n_epochs=2000,units1 = 16, units2=8, units3=4):
        self.NN_model = Sequential()
        self.NN_model.add(Dense(units=units1,input_dim= self.input_dim,activation='relu'))
        self.NN_model.add(Dropout(rate=0.5))
        self.NN_model.add(Dense(units=units2, activation='relu'))
        self.NN_model.add(Dropout(rate=0.5))
        self.NN_model.add(Dense(units=units3, activation='relu'))
        self.NN_model.add(Dropout(rate=0.5))
        self.NN_model.add(Dense(units=1,activation='linear'))
        self.NN_model.compile(loss='mse', optimizer='adam')
        self.NN_model.fit(np.array(X_train), np.array(y_train), epochs=n_epochs,batch_size=batch)
    def make_predictions(self,)

        