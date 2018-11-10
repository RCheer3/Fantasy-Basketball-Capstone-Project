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
    def __init__(self,X,y,test_perc=0.3)
        self.X =X
        self.y = y
        self.X_train,self.X_test,self.y_train,self.y_test = train_test_split(X,y,test_size=test_perc)
        self.input_dim=X_train.shape[1]
    
    def run_NN(self,batch=128,n_epochs=2000):
        self.NN_model = Sequential()
        self.NN_model.add(Dense(units=16,input_dim= self.input_dim,activation='relu'))
        self.NN_model.add(Dense(units=8, activation='relu'))
        self.NN_model.add(Dense(units=4, activation='relu'))
        self.NN_model.add(Dense(units=1,activation='linear'))
        self.NN_model.compile(loss='mse', optimizer='adam')
        self.NN_model.fit(np.array(X_train), np.array(y_train), epochs=n_epochs,batch_size=128)

        