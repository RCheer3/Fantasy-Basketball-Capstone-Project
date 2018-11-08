import pandas as pd
import numpy as np


def setup_data()
    regstats_2017 = pd.read_csv('2017 NBA data.csv')
    regstats_2016 = pd.read_csv('2016 NBA data.csv')
    regstats_2015 = pd.read_csv('2015 NBA data.csv')
    regstats_2014 = pd.read_csv('2014 NBA data.csv')
    regstats_2013 = pd.read_csv('2013 NBA data.csv')

    regstats_2016.columns = ['Rk', 'Player', '2016_Pos', '2016_Age', '2016_Tm', '2016_G', '2016_GS', '2016_MP', '2016_FG', '2016_FGA', '2016_FG%',
           '2016_3P', '2016_3PA', '2016_3P%', '2016_2P', '2016_2PA', '2016_2P%', '2016_eFG%', '2016_FT', '2016_FTA', '2016_FT%',
           '2016_ORB', '2016_DRB', '2016_TRB', '2016_AST', '2016_STL', '2016_BLK', '2016_TOV', '2016_PF', '2016_PS/G']
    regstats_2015.columns = ['Rk', 'Player', '2015_Pos', '2015_Age', '2015_Tm', '2015_G', '2015_GS', '2015_MP', '2015_FG', '2015_FGA', '2015_FG%',
           '2015_3P', '2015_3PA', '2015_3P%', '2015_2P', '2015_2PA', '2015_2P%', '2015_eFG%', '2015_FT', '2015_FTA', '2015_FT%',
           '2015_ORB', '2015_DRB', '2015_TRB', '2015_AST', '2015_STL', '2015_BLK', '2015_TOV', '2015_PF', '2015_PS/G']
    regstats_2014.columns = ['Rk', 'Player', '2014_Pos', '2014_Age', '2014_Tm', '2014_G', '2014_GS', '2014_MP', '2014_FG', '2014_FGA', '2014_FG%',
           '2014_3P', '2014_3PA', '2014_3P%', '2014_2P', '2014_2PA', '2014_2P%', '2014_eFG%', '2014_FT', '2014_FTA', '2014_FT%',
           '2014_ORB', '2014_DRB', '2014_TRB', '2014_AST', '2014_STL', '2014_BLK', '2014_TOV', '2014_PF', '2014_PS/G']
    regstats_2013.columns = ['Rk', 'Player', '2013_Pos', '2013_Age', '2013_Tm', '2013_G', '2013_GS', '2013_MP', '2013_FG', '2013_FGA', '2013_FG%',
           '2013_3P', '2013_3PA', '2013_3P%', '2013_2P', '2013_2PA', '2013_2P%', '2013_eFG%', '2013_FT', '2013_FTA', '2013_FT%',
           '2013_ORB', '2013_DRB', '2013_TRB', '2013_AST', '2013_STL', '2013_BLK', '2013_TOV', '2013_PF', '2013_PS/G']


    past_four_years = pd.merge(
                    pd.merge(
                    pd.merge(
                    pd.merge(regstats_2017,regstats_2016, how = 'left', left_on = 'Player',right_on = 'Player')
                              ,regstats_2015, how = 'left',left_on = 'Player',right_on = 'Player')
                            ,regstats_2014, how = 'left',left_on = 'Player',right_on = 'Player')
                            ,regstats_2013, how = 'left',left_on = 'Player',right_on = 'Player')


    traded_2017 = pd.DataFrame(regstats_2017.groupby('Player').count()['G']>1).reset_index()
    traded_2017.columns = ['Player','Traded']
    cleaned_2017 = pd.merge(regstats_2017,traded_2017, how = 'inner',left_on = 'Player',right_on = 'Player')
    df_2017 = cleaned_2017[((cleaned_2017.Traded ==False).astype(int)+ (cleaned_2017.Tm == 'TOT').astype(int)).astype(bool)]

    traded_2016 = pd.DataFrame(regstats_2016.groupby('Player').count()['2016_G']>1).reset_index()
    traded_2016.columns = ['Player','Traded']
    cleaned_2016 = pd.merge(regstats_2016,traded_2016, how = 'inner',left_on = 'Player',right_on = 'Player')
    df_2016 = cleaned_2016[((cleaned_2016.Traded ==False).astype(int)+ (cleaned_2016['2016_Tm'] == 'TOT').astype(int)).astype(bool)]

    traded_2015 = pd.DataFrame(regstats_2015.groupby('Player').count()['2015_G']>1).reset_index()
    traded_2015.columns = ['Player','Traded']
    cleaned_2015 = pd.merge(regstats_2015,traded_2015, how = 'inner',left_on = 'Player',right_on = 'Player')
    df_2015 = cleaned_2015[((cleaned_2015.Traded ==False).astype(int)+ (cleaned_2015['2015_Tm'] == 'TOT').astype(int)).astype(bool)]

    traded_2014 = pd.DataFrame(regstats_2014.groupby('Player').count()['2014_G']>1).reset_index()
    traded_2014.columns = ['Player','Traded']
    cleaned_2014 = pd.merge(regstats_2014,traded_2014, how = 'inner',left_on = 'Player',right_on = 'Player')
    df_2014 = cleaned_2014[((cleaned_2014.Traded ==False).astype(int)+ (cleaned_2014['2014_Tm'] == 'TOT').astype(int)).astype(bool)]

    traded_2013 = pd.DataFrame(regstats_2013.groupby('Player').count()['2013_G']>1).reset_index()
    traded_2013.columns = ['Player','Traded']
    cleaned_2013 = pd.merge(regstats_2013,traded_2013, how = 'inner',left_on = 'Player',right_on = 'Player')
    df_2013 = cleaned_2013[((cleaned_2013.Traded ==False).astype(int)+ (cleaned_2013['2013_Tm'] == 'TOT').astype(int)).astype(bool)]
    
    past_four_years['2016_TotMinutes'] = (past_four_years['2016_G']*past_four_years['2016_MP']/82)
    past_four_years['2015_TotMinutes'] = (past_four_years['2015_G']*past_four_years['2015_MP']/82)
