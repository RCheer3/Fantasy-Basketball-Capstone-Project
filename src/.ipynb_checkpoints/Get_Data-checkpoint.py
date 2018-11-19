import psycopg2 as pg2
import pandas as pd
import numpy as np
from Player_rank import Player_ranker
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split



class Get_Category_Data:
    '''
    Now that we have all of the data that we need stored in a SQL database nba_capstone, we'll need to get training and test data to run
    tests on.  These queries grab the data from the SQL database and store them in X/y training and test groups for each of the categories to be predicted
    
    '''
    def __init__(self,test_year=2018):
        conn = pg2.connect(dbname = 'nba_capstone',host='localhost')
        cur = conn.cursor()
        self.test_year=test_year
        
        
        '''Test whether to use point query or to use FGM*2+FTM+3PM as points.  If we use this version, need to scale the other 3 categories'''
        
        point_query = '''select * from points_pred where season>2008'''
        cur.execute(point_query)
        data_points = cur.fetchall()
        points_df = pd.DataFrame(np.array(data_points))
        points_df.columns = ['season','player','age','team','points','points_ly','change_points_ly','starter_change','Games','C_PF','PG','SG_SF','high_usageplayer_added','usagemin_opened','maxusage_added','high_usageplayer_dropped','points_opened','max_pointsdropped','max_pointsadded','three_ar_ly','change_3ar','per_ly','change_per','usagerank','usagerank_ly','offensive_winshares','offensive_boxplusminus','boxplusminus','value_overreplacement','max_teammatepts','max_teammateusage','max_teammateto','max_teammateshot_attempts','career_points','yearspro']
        points_df['age_squared']= points_df['age']*points_df['age']
        points = points_df[points_df['points_ly'].notna()]
        for i in points.columns:
            if i not in(['player','team']):
                points[i]=pd.to_numeric(points[i])
        self.points = points.fillna(0)
        
        
        rebound_query = '''select * from rebounds_pred where season>2008'''
        cur.execute(rebound_query)
        data_rebounds = cur.fetchall()
        rebounds_df = pd.DataFrame(np.array(data_rebounds))
        rebounds_df.columns = ['season','player','age','team','rebounds','rebounds_ly','change_rebounds_ly','Games','C_PF','PG','SG_SF','starter_change','high_usageplayer_added','usagemin_opened','maxusage_added','high_usageplayer_dropped','rebounds_opened','max_reboundsdropped','max_reboundsadded','per_ly','change_per','usagerank','usagerank_ly','reb_perc_ly','change_reb_perc','defensive_winshares','defensive_boxplusminus','boxplusminus','value_overreplacement','max_teammatereb','max_teammate_usage','max_teammateto','max_teammateshot_attempts','career_rebounds','yearspro']
        rebounds_df['age_squared'] = rebounds_df['age']*rebounds_df['age']
        rebounds_df['yearspro_squared'] = rebounds_df['yearspro']*rebounds_df['yearspro']
        
        rebounds = rebounds_df[rebounds_df['rebounds_ly'].notna()]
        for i in rebounds.columns:
            if i not in(['player','team']):
                rebounds[i]=pd.to_numeric(rebounds[i])
        self.rebounds = rebounds.fillna(0)
        
        
        assist_query = ''' select * from assists_pred where season>2008'''
        cur.execute(assist_query)
        data_assists = cur.fetchall()
        assists_df = pd.DataFrame(np.array(data_assists))
        assists_df.columns = ['season','player','age','team','assists','assists_ly','change_assists_ly','Games','C_PF','PG','SG_SF','starter_change','high_usageplayer_added','usagemin_opened','maxusage_added','high_usageplayer_dropped','assists_opened','max_assistsdropped','max_assistsadded','points_opened','threes_opened','per_ly','change_per','usagerank','usagerank_ly','ast_perc_ly','change_ast_perc','offensive_winshares','offensive_boxplusminus','boxplusminus','value_overreplacement','max_teammateast','max_teammate_usage','max_teammateto','max_teammateshot_attempts','career_assists','yearspro']
        
        assists_df['age_squared']=assists_df['age']*assists_df['age']
        assists_df['yearspro_squared'] = assists_df['yearspro']*assists_df['yearspro']
        assists = assists_df[assists_df['assists_ly'].notna()]
        for i in assists.columns:
            if i not in(['player','team']):
                assists[i]=pd.to_numeric(assists[i])
        self.assists = assists.fillna(0)
        
        
        steals_query= '''select * from steals_pred where season>2009'''
        cur.execute(steals_query)
        data_steals = cur.fetchall()
        steals_df = pd.DataFrame(np.array(data_steals))
        steals_df.columns = ['season','player','age','team','steals','steals_ly','change_steals_ly','Games','C_PF','PG','SG_SF','starter_change','per_ly','change_per','stl_perc_ly','change_stl_perc','defensive_winshares','defensive_boxplusminus','boxplusminus','value_overreplacement','max_teammate_usage','max_teammateto','max_teammateshot_attempts','career_steals','yearspro']
        steals_df['age_squared']=steals_df['age']*steals_df['age']
        steals_df['yearspro_squared'] = steals_df['yearspro']*steals_df['yearspro']
        steals = steals_df[steals_df['steals_ly'].notna()]
        for i in steals.columns:
            if i not in(['player','team']):
                steals[i]=pd.to_numeric(steals[i])
        self.steals = steals.fillna(0)
        
        blocks_query = '''select * from blocks_pred where season>2009'''
        cur.execute(blocks_query)
        data_blocks = cur.fetchall()
        blocks_df = pd.DataFrame(np.array(data_blocks))
        blocks_df.columns = ['season','player','age','team','blocks','blocks_ly','change_blocks_ly','Games','C_PF','PG','SG_SF','starter_change','per_ly','change_per','blk_perc_ly','change_blk_perc','defensive_winshares','defensive_boxplusminus','boxplusminus','value_overreplacement','max_teammate_usage','max_teammateto','max_teammateshot_attempts','career_blocks','yearspro']
        blocks_df['age_squared']=blocks_df['age']*blocks_df['age']
        blocks_df['yearspro_squared'] = blocks_df['yearspro']*blocks_df['yearspro']
        blocks = blocks_df[blocks_df['blocks_ly'].notna()]
        for i in blocks.columns:
            if i not in(['player','team']):
                blocks[i]=pd.to_numeric(blocks[i])
        self.blocks = blocks.fillna(0)
        
        
        threes_query = '''select * from threes_pred where season>2008'''
        cur.execute(threes_query)
        data_threes = cur.fetchall()
        threes_df = pd.DataFrame(np.array(data_threes))
        threes_df.columns = ['season','player','age','team','3PM','3PM_ly','3PM_change','points_ly','change_points_ly','starter_change','C_PF','PG','SG_SF','Games','high_usageplayer_added','usagemin_opened','maxusage_added','high_usageplayer_dropped','points_opened','max_pointsdropped','max_pointsadded','three_ar_ly','change_3ar','per_ly','change_per','usagerank','usagerank_ly','offensive_winshares','offensive_boxplusminus','boxplusminus','value_overreplacement','max_teammatepts','max_teammate_usage','max_teammateto','max_teammateshot_attempts','career_3PM','yearspro'] 
        threes_df['age_squared']=threes_df['age']*threes_df['age']
        threes_df['yearspro_squared'] = threes_df['yearspro']*threes_df['yearspro']
        threes = threes_df[threes_df['3PM_ly'].notna()]
        for i in threes.columns:
            if i not in(['player','team']):
                threes[i]=pd.to_numeric(threes[i])
        self.threes = threes.fillna(0)
        
        
        turnover_query = '''select * from turnovers_pred where season>2008'''
        cur.execute(turnover_query)
        data_turnovers = cur.fetchall()
        to_df = pd.DataFrame(np.array(data_turnovers))
        to_df.columns = ['season','player','age','team','turnovers','turnovers_ly','change_turnovers_ly','starter_change','Games','C_PF','PG','SG_SF','high_usageplayer_added','usagemin_opened','maxusage_added','high_usageplayer_dropped','per_ly','change_per','usagerank','usagerank_ly','offensive_winshares','offensive_boxplusminus','max_teammatepts','max_teammate_usage','max_teammateto','max_teammateshot_attempts','career_turnovers','yearspro']
        
        to_df['age_squared']=to_df['age']*to_df['age']
        to_df['yearspro_squared'] = to_df['yearspro']*to_df['yearspro']
        tos = to_df[to_df['turnovers_ly'].notna()]
        for i in tos.columns:
            if i not in(['player','team']):
                tos[i]=pd.to_numeric(tos[i])
        self.tos = tos.fillna(0)
        
        
        
        percentages_query = ''' select * from percentages where season>2008'''

        cur.execute(percentages_query)
        data = cur.fetchall()
        percentages_df = pd.DataFrame(np.array(data))
        percentages_df.columns = ['season','player','age','team','FGM','FGA','FG_percent','FG_percent_ly','FGM_ly','FGA_ly','FTM','FTA','FT_percent','FT_percent_ly','FTM_ly','FTA_ly','3PM_ly','3PM_change','points_ly','change_points_ly','starter_change','C_PF','PG','SG_SF','Games','high_usageplayer_added','usagemin_opened','maxusage_added','high_usageplayer_dropped','points_opened','max_pointsdropped','max_pointsadded','three_ar_ly','change_3ar','per_ly','change_per','usagerank','usagerank_ly','offensive_winshares','offensive_boxplusminus','boxplusminus','value_overreplacement','max_teammatepts','max_teammate_usage','max_teammateto','max_teammateshot_attempts','career_FGM','career_FGA','career_FGpercent','career_FTM','career_FTA','career_FTPercent','yearspro']
        
        percentages_df['age_squared']=percentages_df['age']*percentages_df['age']
        percentages_df['yearspro_squared'] = percentages_df['yearspro']*percentages_df['yearspro']
        percentages = percentages_df[percentages_df['FGM_ly'].notna()]
        for i in percentages.columns:
            if i not in(['player','team']):
                percentages[i]=pd.to_numeric(percentages[i])
        self.percentages = percentages.fillna(0)
    
    def get_train_test_split(self, test_proportion=0.3):
        self.y_points = self.points[(self.points['Games']>30) & (self.points['season']<self.test_year)]['points']
        self.X_points = self.points[(self.points['Games']>30) & (self.points['season']<self.test_year)].drop(['points','player','season','team','Games'],axis=1)
        self.X_train_points,self.X_test_points,self.y_train_points,self.y_test_points = train_test_split(self.X_points,self.y_points, test_size=test_proportion)
        self.X_points_holdout = self.points[self.points['season']==self.test_year].drop(['points','player','season','team','Games'],axis=1)
        self.y_points_holdout = self.points[self.points['season']==self.test_year][['points','player']]
        
        
        self.X_rebounds = self.rebounds[(self.rebounds['Games']>30) & (self.rebounds['season']<self.test_year)].drop(['player','team','rebounds','Games'],axis=1)
        self.y_rebounds = self.rebounds[(self.rebounds['Games']>30) & (self.rebounds['season']<self.test_year)]['rebounds']
        self.X_train_rebounds,self.X_test_rebounds,self.y_train_rebounds,self.y_test_rebounds = train_test_split(self.X_rebounds,self.y_rebounds,test_size = test_proportion)
        self.X_rebounds_holdout = self.rebounds[self.rebounds['season']==self.test_year].drop(['player','team','rebounds','Games'],axis=1)
        self.y_rebounds_holdout = self.rebounds[self.rebounds['season']==self.test_year][['rebounds','player']]
        
        self.X_assists = self.assists[(self.assists['Games']>30) & (self.assists['season']<self.test_year)].drop(['player','team','assists','Games'],axis=1)
        self.y_assists = self.assists[(self.assists['Games']>30) & (self.assists['season']<self.test_year)]['assists']
        self.X_train_assists,self.X_test_assists,self.y_train_assists,self.y_test_assists = train_test_split(self.X_assists,self.y_assists,test_size = test_proportion)
        self.X_assists_holdout = self.assists[self.assists['season']==self.test_year].drop(['player','team','assists','Games'],axis=1)
        self.y_assists_holdout = self.assists[self.assists['season']==self.test_year][['assists','player']]

        self.X_steals = self.steals[(self.steals['Games']>30) & (self.steals['season']<self.test_year)].drop(['player','team','steals','Games'],axis=1)
        self.y_steals = self.steals[(self.steals['Games']>30) & (self.steals['season']<self.test_year)]['steals']
        self.X_train_steals,self.X_test_steals,self.y_train_steals,self.y_test_steals = train_test_split(self.X_steals,self.y_steals,test_size = 0.3)
        self.X_steals_holdout = self.steals[self.steals['season']==self.test_year].drop(['player','team','steals','Games'],axis=1)
        self.y_steals_holdout = self.steals[self.steals['season']==self.test_year][['steals','player']]
        
        self.X_blocks = self.blocks[(self.blocks['Games']>30) & (self.blocks['season']<self.test_year)].drop(['player','team','blocks','Games'],axis=1)
        self.y_blocks = self.blocks[(self.blocks['Games']>30) & (self.blocks['season']<self.test_year)]['blocks']
        self.X_train_blocks,self.X_test_blocks,self.y_train_blocks,self.y_test_blocks = train_test_split(self.X_blocks,self.y_blocks,test_size = 0.3)
        self.X_blocks_holdout = self.blocks[self.blocks['season']==self.test_year].drop(['player','team','blocks','Games'],axis=1)
        self.y_blocks_holdout = self.blocks[self.blocks['season']==self.test_year][['blocks','player']]
        
        self.X_threes = self.threes[(self.threes['Games']>30) & (self.threes['season']<self.test_year)].drop(['player','team','3PM','Games'],axis=1)
        self.y_threes = self.threes[(self.threes['Games']>30) & (self.threes['season']<self.test_year)]['3PM']
        self.X_train_threes,self.X_test_threes,self.y_train_threes,self.y_test_threes = train_test_split(self.X_threes,self.y_threes,test_size = 0.3)
        self.X_threes_holdout = self.threes[self.threes['season']==self.test_year].drop(['player','team','3PM','Games'],axis=1)
        self.y_threes_holdout = self.threes[self.threes['season']==self.test_year][['3PM','player']]
        
        self.X_tos = self.tos[(self.tos['Games']>30) & (self.tos['season']<self.test_year)].drop(['player','team','turnovers','Games'],axis=1)
        self.y_tos = self.tos[(self.tos['Games']>30) & (self.tos['season']<self.test_year)]['turnovers']
        self.X_train_tos,self.X_test_tos,self.y_train_tos,self.y_test_tos = train_test_split(self.X_tos,self.y_tos,test_size = 0.3)
        self.X_tos_holdout = self.tos[self.tos['season']==self.test_year].drop(['player','team','turnovers','Games'],axis=1)
        self.y_tos_holdout = self.tos[self.tos['season']==self.test_year][['turnovers','player']]
        
        self.X_FGM = self.percentages[(self.percentages['Games']>30) & (self.percentages['season']<self.test_year)].drop(['player','team','FGM','FGA','FG_percent','FTM','FTA','Games'],axis=1)
        self.y_FGM = self.percentages[(self.percentages['Games']>30) & (self.percentages['season']<self.test_year)]['FGM']
        self.X_train_FGM,self.X_test_FGM,self.y_train_FGM,self.y_test_FGM = train_test_split(self.X_FGM,self.y_FGM,test_size = 0.3)
        self.X_FGM_holdout = self.percentages[self.percentages['season']==self.test_year].drop(['player','team','FGM','FGA','FG_percent','FTM','FTA','Games'],axis=1)
        self.y_FGM_holdout = self.percentages[self.percentages['season']==self.test_year][['FGM','player']]
        
        self.X_FTM = self.percentages[(self.percentages['Games']>30) & (self.percentages['season']<self.test_year)].drop(['player','team','FTM','FGA','FG_percent','FTM','FTA','Games'],axis=1)
        self.y_FTM = self.percentages[(self.percentages['Games']>30) & (self.percentages['season']<self.test_year)]['FTM']
        self.X_train_FTM,self.X_test_FTM,self.y_train_FTM,self.y_test_FTM = train_test_split(self.X_FTM,self.y_FTM,test_size = 0.3)
        self.X_FTM_holdout = self.percentages[self.percentages['season']==self.test_year].drop(['player','team','FTM','FGA','FG_percent','FTM','FTA','Games'],axis=1)
        self.y_FTM_holdout = self.percentages[self.percentages['season']==self.test_year][['FTM','player']]
        
        self.X_FGP = self.percentages[(self.percentages['Games']>30) & (self.percentages['season']<self.test_year)][['FG_percent_ly','career_FGpercent','age','age_squared','yearspro','yearspro_squared','max_teammateto','max_teammateshot_attempts','max_teammate_usage','max_teammatepts','usagerank']]
        self.y_FGP = self.percentages[(self.percentages['Games']>30) & (self.percentages['season']<self.test_year)]['FG_percent']
        self.X_train_FGP,self.X_test_FGP,self.y_train_FGP,self.y_test_FGP = train_test_split(self.X_FGP,self.y_FGP,test_size = 0.3)
        self.X_FGP_holdout=self.percentages[self.percentages['season']==self.test_year][['FG_percent_ly','career_FGpercent','age','age_squared','yearspro','yearspro_squared','max_teammateto','max_teammateshot_attempts','max_teammate_usage','max_teammatepts','usagerank']]
        self.y_FGP_holdout = self.percentages[self.percentages['season']==self.test_year][['FG_percent','player']]
        
        self.X_FTP = self.percentages[(self.percentages['Games']>30) & (self.percentages['season']<self.test_year)][['FT_percent_ly','career_FTPercent','age','age_squared','yearspro','yearspro_squared','max_teammateto','max_teammateshot_attempts','max_teammate_usage','max_teammatepts','usagerank']]
        self.y_FTP = self.percentages[(self.percentages['Games']>30) & (self.percentages['season']<self.test_year)]['FT_percent']
        self.X_train_FTP,self.X_test_FTP,self.y_train_FTP,self.y_test_FTP = train_test_split(self.X_FTP,self.y_FTP,test_size = 0.3)
        self.X_FTP_holdout=self.percentages[self.percentages['season']==self.test_year][['FT_percent_ly','career_FTPercent','age','age_squared','yearspro','yearspro_squared','max_teammateto','max_teammateshot_attempts','max_teammate_usage','max_teammatepts','usagerank']]
        self.y_FTP_holdout = self.percentages[self.percentages['season']==self.test_year][['FT_percent','player']]
