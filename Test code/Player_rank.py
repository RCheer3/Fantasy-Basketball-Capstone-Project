import pandas as pd
import numpy as np


class Player_ranker():
    def __init__(self,playerdata, n=200):
        self.playerdata= playerdata
        self.n = n
        self.top_n = self.playerdata.copy()[self.playerdata.copy()['min_rank']<=self.n]

    def get_category_dist(self):
        self.cat_mean = self.top_n.groupby('season').mean().reset_index()[['season','points','rebounds','assists','steals','blocks','turnovers','threes_made','FGM','FGA','FTM','FTA']]
        self.cat_sd = self.top_n.groupby('season').std().reset_index()[['season','points','rebounds','assists','steals','blocks','turnovers','threes_made','FGM','FGA','FTM','FTA']]
        self.cat_mean.columns= ['season','mean_points','mean_rebounds','mean_assists','mean_steals','mean_blocks','mean_turnovers','mean_threes_made','mean_fgm','mean_fga','mean_ftm','mean_fta']
        self.cat_sd.columns= ['season','sd_points','sd_rebounds','sd_assists','sd_steals','sd_blocks','sd_turnovers','sd_threes_made','sd_fgm','sd_fga','sd_ftm','sd_fta']
        
    def assign_values(self):
        value_merged = pd.merge(pd.merge(self.playerdata,self.cat_mean,how='inner',left_on='season',right_on='season'), self.cat_sd,how='inner',left_on='season',right_on='season')
        value_merged['value_points']=(value_merged['points']-value_merged['mean_points'])/value_merged['sd_points']
        value_merged['value_rebounds']=(value_merged['rebounds']-value_merged['mean_rebounds'])/value_merged['sd_rebounds']
        value_merged['value_assists']=(value_merged['assists']-value_merged['mean_assists'])/value_merged['sd_assists']
        value_merged['value_blocks']=(value_merged['blocks']-value_merged['mean_blocks'])/value_merged['sd_blocks']
        value_merged['value_steals']=(value_merged['steals']-value_merged['mean_steals'])/value_merged['sd_steals']
        value_merged['value_turnovers']=-1*(value_merged['turnovers']-value_merged['mean_turnovers'])/value_merged['sd_turnovers']
        value_merged['value_threes']=(value_merged['threes_made']-value_merged['mean_threes_made'])/value_merged['sd_threes_made']
        value_merged['value_fg']= 20/value_merged['mean_fga']*(value_merged['FGM']*(1-(value_merged['mean_fgm']/value_merged['mean_fga']))-(value_merged['FGA']-value_merged['FGM'])*(value_merged['mean_fgm']/value_merged['mean_fga']))
        value_merged['value_ft']= 10/value_merged['mean_fta']*(value_merged['FTM']*(1-(value_merged['mean_ftm']/value_merged['mean_fta']))-(value_merged['FTA']-value_merged['FTM'])*(value_merged['mean_ftm']/value_merged['mean_fta']))
        value_merged['value_tot']=value_merged['value_points']+value_merged['value_rebounds']+value_merged['value_assists']+value_merged['value_blocks']+value_merged['value_steals']+value_merged['value_turnovers']+value_merged['value_threes']+value_merged['value_fg']+value_merged['value_ft']
        self.value = value_merged[['season','player','value_tot','value_points','value_rebounds','value_assists','value_blocks','value_steals','value_turnovers','value_threes','value_fg','value_ft']]