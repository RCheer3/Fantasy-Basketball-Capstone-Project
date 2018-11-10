import psycopg2 as pg2
import pandas as pd
import numpy as np
from Player_rank import Player_ranker
from sqlalchemy import create_engine


class Data_Setup:
    def __init__(self,advanced,regular):
        self.conn = pg2.connect(dbname = 'postgres',host='localhost')
        self.conn.autocommit = True
        self.cur = conn.cursor()
        self.advanced = advanced
        self.regular = regular
    def clean_DB(self)
        self.cur.execute('DROP DATABASE IF EXISTS nba_capstone;')  
        self.cur.execute('CREATE DATABASE nba_capstone;')
        self.cur.close()
        self.conn.close()
        self.conn = pg2.connect(dbname = 'nba_capstone',host='localhost')
        self.conn.autocommit = True
        self.cur = conn.cursor()
    def load_data(self):
        query = '''
        CREATE TABLE NBA_stats (
            Season integer, 
            Player varchar(50), 
            Pos varchar(10),
            Age int,
            Tm varchar(15),
            G int,
            GS int,
            MP float,
            FG float,
            FGA float,
            FG_Percentage float,
            Threes_Made float,
            Threes_Attempted float,
            Three_Percentage float,
            Twos_Made float,
            Twos_Attempted float,
            Twos_Percentage float,
            eff_FG_Percentage float,
            FTM float,
            FTA float,
            FT_Percentage float,
            ORB float,
            DRB float,
            Rebounds float,
            AST float,
            STL float,
            BLK float,
            TOV float,
            Fouls float,
            Points float
        );
        
        COPY NBA_stats 
        FROM '/Users/rcheer/Desktop/Galvanize/Capstone/Fantasy-Basketball-Capstone-Project/NBA stats.csv' 
        DELIMITER ',' 
        CSV HEADER;
        
        
        CREATE TABLE nba_advanced (
            Season integer, 
            Player varchar(50), 
            Pos varchar(10),
            Age int,
            Tm varchar(15),
            G int,
            total_MP float,
            PER float,
            True_Shooting float,
            Three_Attempt_Rate float,
            FT_rate float,
            ORB_Percentage float,
            DRB_Percentage float,
            Rebound_Percentage float,
            Assist_Percentage float,
            Steal_Percentage float,
            Block_Percentage float,
            Turnover_Percentage float,
            Usage_Percentage float,
            Offensive_WinShares float,
            Defensive_WinShares float,
            WinShares float,
            WinShares_Per48 float,
            Offensive_BoxPlusMinus float,
            Defensive_BoxPlusMinus float,
            BoxPlusMinus float,
            Value_overReplacement float
        );
        
        COPY nba_advanced 
        FROM '/Users/rcheer/Desktop/Galvanize/Capstone/Fantasy-Basketball-Capstone-Project/NBA Advanced.csv' 
        DELIMITER ',' 
        CSV HEADER;
        '''

        self.cur.execute(query)
        
    def create_tables(self):
        query1 = '''
            update nba_stats set Tm = 'NOP' where Tm = 'NOH';
            update nba_advanced set Tm = 'NOP' where Tm = 'NOH';
            update nba_stats set Tm = 'CHA' where Tm = 'CHO';
            update nba_advanced set Tm = 'CHA' where Tm = 'CHO';
            
            DROP TABLE IF EXISTS players;
            CREATE TABLE players AS
            select season,player,max(G) as Games from NBA_stats where Tm!='TOT' group by season,player;
            
            DROP TABLE IF EXISTS y_predictions;
            CREATE TABLE y_predictions AS
            select d.season,d.player,d.pos,d.age,MAX(case when p.player is not null then d.Tm else NULL end) as StartingTeam,SUM(G) as Games,SUM(GS) as GS,
            max(MP) as minutes
            from NBA_stats d
            left join players p
                on d.season = p.season
                and d.player = p.player
                and d.G = p.Games
            where d.Tm!='TOT'
            group by d.season,d.player,d.pos,d.age;
            
            update y_predictions set StartingTeam = 'NOP' where startingTeam = 'NOH';
            update y_predictions set StartingTeam = 'CHA' where startingTeam = 'CHO';
            
            DROP TABLE IF EXISTS rank_by_minutes;
            CREATE TABLE rank_by_minutes AS
            select y.*,n.points,n.rebounds,n.ast,n.stl,n.blk,n.tov,n.threes_made,n.fg,n.fga,n.ftm,n.fta,
            case when cast(y.GS as float)/y.Games >0.6 then 1 else 0 end as starter,
            row_number() over(partition by n.season order by MP*G desc) as min_rank from NBA_stats n
            inner join y_predictions y
                ON n.player = y.player
                and n.season = y.season
                and n.Tm=y.startingTeam;
            
            select * from rank_by_minutes        
                
        '''
        self.cur.execute(query1)
        data = cur.fetchall()
        self.player_data = pd.DataFrame(np.array(data))
        self.player_data.columns = ['season','player','position','age','team','gamesPlayed','gamesStarted','minutes','points','rebounds','assists','steals','blocks','turnovers','threes_made','FGM','FGA','FTM','FTA','starter','min_rank']
        self.rank_players()
        engine = create_engine("postgresql://@localhost/nba_capstone")
        self.value_copy.to_sql(name='value', con=engine, if_exists = 'replace', index=False)
        
        query = '''
        DROP TABLE IF EXISTS player_value;
        CREATE TABLE player_value AS
        select ROW_NUMBER() OVER(PARTITION BY season ORDER BY value_tot DESC),* from value;
        
        DROP TABLE IF EXISTS value;
        
        select * from player_value;
        
        '''
        self.cur.execute(query)
        
        
        query = '''
        DROP TABLE IF EXISTS adv_top10min;
        CREATE TEMPORARY TABLE adv_top10min as 
        select a.*,usage_percentage*total_MP/G as usage_withMins,row_number() over(partition by a.season,tm order by usage_percentage*total_MP/G desc) as usage_rank
        from(select *,row_number() over(partition by season,tm order by total_MP desc) as min_rank from nba_advanced) a
        inner join y_predictions y
                ON a.player = y.player
                and a.season = y.season
                and a.Tm=y.startingTeam
        where min_rank<=10;
        
        DROP TABLE IF EXISTS Change_Teams;
        CREATE TABLE Change_Teams AS
        select na.tm as old_team,na2.tm as new_team,na2.player,na2.pos,p.season,na.usage_withMins,
        n.points,n.rebounds,n.ast,n.threes_made,na.usage_rank 
        from player_value p
        inner join adv_top10min na
            on p.player = na.player
            and p.season = na.season+1
        inner join adv_top10min na2
            on na2.player = na.player
            and na2.season = p.season
            and na2.tm != na.tm
        inner join rank_by_minutes n
            ON N.player = na.player
            and n.season = na.season;
        
        
        
        DROP TABLE IF EXISTS incoming_by_team;
        CREATE TABLE incoming_by_team AS
        select new_team,season,SUM(case when usage_withmins >1000 then 1 else 0 end) as high_usageplayer_added,
        SUM(usage_withmins) as usagemin_added, MAX(usage_withmins) as max_usageadded,
        SUM(points) as points_added, MAX(points) as max_pointsadded,SUM(rebounds) as rebounds_added,
        MAX(rebounds) as max_reboundsadded, SUM(ast) as ast_added, MAX(ast) as max_astadded,
        SUM(threes_made) as threes_added, MAX(threes_made) as max_threesadded 
        from change_teams
        group by new_team,season;
        
        DROP TABLE IF EXISTS outgoing_by_team;
        CREATE TABLE outgoing_by_team AS
        select old_team,season,SUM(case when usage_withmins >1000 then 1 else 0 end) as high_usageplayer_dropped,
        SUM(usage_withmins) as usagemin_dropped, MAX(usage_withmins) as max_usagedropped,
        SUM(points) as points_dropped, MAX(points) as max_pointsdropped,SUM(rebounds) as rebounds_dropped,
        MAX(rebounds) as max_reboundsdropped, SUM(ast) as ast_dropped, MAX(ast) as max_astdropped,
        SUM(threes_made) as threes_dropped, MAX(threes_made) as max_threesdropped
        from change_teams
        group by old_team,season;
        
        DROP TABLE IF EXISTS Team_Changes;
        CREATE TABLE Team_Changes AS
        select c.new_team as team, c.season,c.high_usageplayer_added,o.usagemin_dropped-c.usagemin_added as usagemin_opened,
        c.max_usageadded,o.high_usageplayer_dropped,o.max_usagedropped,
        o.points_dropped-c.points_added as points_opened,max_pointsdropped,max_pointsadded,
        o.rebounds_dropped-c.rebounds_added as rebounds_opened,max_reboundsdropped,max_reboundsadded,
        o.ast_dropped-c.ast_added as ast_opened,max_astdropped,max_astadded,
        o.threes_dropped-c.threes_added as threes_opened,max_threesdropped,max_threesadded
        from incoming_by_team c
        inner join outgoing_by_team o
            ON o.old_team = c.new_team
            and o.season = c.season;
            
        DROP TABLE IF EXISTS Team_maxes;
        CREATE TABLE Team_maxes AS
        select R.season,R.startingTeam,MAX(R2.points) as pts, max(r2.rebounds) as reb, max(r2.ast) as ast,
        MAX(R2.Tov) as TO,MAX(r2.FGA) as shot_attempts, cast(NULL as float) as max_usage 
        from rank_by_minutes R
        inner join rank_by_minutes R2
            ON R2.startingTeam = R.startingTeam
            and R2.season+1 = R.season
        group by R.season,R.startingTeam;
        
        
        update Team_maxes T
        set max_usage = usage
        from
        (select T.season,T.startingTeam,T.pts,t.reb,t.ast,t.TO,MAX(a.usage_percentage) as usage from Team_maxes T
        inner join adv_top10min a
            ON a.season+1 = t.season
            and a.tm = t.startingteam
        group by T.season,T.startingTeam,T.pts,t.reb,t.ast,t.TO) A
        WHERE A.season = T.season and  A.startingTeam = T.startingTeam;
        
        
        
        '''


        cur.execute(query)
        
        query = '''
        DROP TABLE IF EXISTS player_stats;
        CREATE TABLE player_stats AS
        select r.*,r2.starter as starter_ly,r3.points-r2.points as change_points_ly,r2.points as points_ly
        ,r3.rebounds-r2.rebounds as change_reb_ly, r2.rebounds as rebounds_ly,
        r3.ast-r2.ast as change_ast_ly, r2.ast as ast_ly,r3.stl-r2.stl as change_stl_ly,r2.stl as stl_ly,
        r3.blk-r2.blk as change_blk_ly, r2.blk as blk_ly,r3.tov-r2.tov as change_tov_ly,r2.tov as tov_ly
        from rank_by_minutes r
        left join rank_by_minutes r2
            on r.player = r2.player
            and r.season = r2.season+1
        left join rank_by_minutes r3
            ON r3.player = r2.player
            and r3.season+1 = r2.season;
        
        DROP TABLE IF EXISTS player_advstats;
        CREATE TABLE player_advstats AS
        select y.player,y.season,y.startingteam,a.per as per_ly, a2.per-a.per as change_per,a.three_attempt_rate as threeAR_ly,
        a2.three_attempt_rate-a.three_attempt_rate as change_3AR, a.rebound_percentage as reb_perc_ly, a2.rebound_percentage-a.rebound_percentage as change_reb_perc
        ,a.assist_percentage as ast_perc_ly, a2.assist_percentage-a.assist_percentage as change_assist_perc
        ,a.steal_percentage as stl_perc_ly, a2.steal_percentage-a.steal_percentage as change_stl_perc_ly
        ,a.block_percentage as blk_perc_ly, a2.block_percentage-a.block_percentage as change_blk_perc_ly
        ,a.turnover_percentage as TO_perc_ly, a2.turnover_percentage-a.turnover_percentage as change_turnover_perc_ly,
        rank() over(partition by y.season,y.startingTeam order by a.usage_percentage) as usagerank,
        rank() over(partition by y.season,a.tm order by a.usage_percentage) as usagerank_ly,
        a.offensive_winshares,
        a.defensive_winshares,a.winshares,a.winshares_per48,a.offensive_boxplusminus,a.defensive_boxplusminus,
        a.boxplusminus,a.value_overreplacement        
        from y_predictions y
        left join nba_advanced a
            ON a.player = y.player
            and a.season+1 = y.season
        left join nba_advanced a2
            ON a2.player = a.player
            and a2.season+1 = a.season;
    
        
        DROP TABLE IF EXISTS player_careerstats;
        CREATE TABLE player_careerstats AS
        select r.player,r.season,SUM(case when r2.player is not null then 1 else 0 end) as YearsPro, avg(r2.points) as career_points
        ,avg(r2.rebounds) as career_rebounds,avg(r2.ast) as career_ast, avg(r2.stl) as career_stl, avg(r2.blk) as career_blk
        ,avg(r2.tov) as career_TO, avg(r2.threes_made) as career_threesmade,avg(r2.ftm) as career_ftm,avg(r2.fta) as career_fta
        ,avg(r2.fga) as career_fga, avg(r2.fg) as career_fgm
        from rank_by_minutes r
        inner join rank_by_minutes r2
            ON r.player = r2.player
            and r.season > r2.season
        group by r.player,r.season;
       
        '''


        cur.execute(query)
        
        query= '''
        
        DROP TABLE IF EXISTS points_pred;
        CREATE TABLE points_pred(
        season int, --these come from player_stats
        player varchar(50),
        Age int,
        team varchar(50),
        points float, -- these come from player_stats
        points_ly float,
        change_points_ly float,
        
        starter_change int, -- these come from team_changes
        high_usageplayer_added int,
        usagemin_opened float,
        maxusage_added float,
        high_usageplayer_dropped int,
        points_opened float,
        max_pointsdropped float,
        max_pointsadded float,
        
        three_ar_ly float, -- from player_advstats
        change_3ar float,
        per_ly float,
        change_per float,
        usagerank float,
        usagerank_ly float,
        offensive_winshares float,
        offensive_boxplusminus float,
        boxplusminus float,
        value_overreplacement float,
        
        career_points float,
        yearspro int
        );
        
        INSERT INTO points_pred(season,player,age,team,points,points_ly,change_points_ly,starter_change)
        SELECT season,player,age,startingteam,points,points_ly,change_points_ly,starter-starter_ly from player_stats;
        
        update points_pred pp
        set high_usageplayer_added = tc.high_usageplayer_added,usagemin_opened=tc.usagemin_opened,
        maxusage_added=tc.max_usageadded,high_usageplayer_dropped=tc.high_usageplayer_dropped,points_opened=tc.points_opened,
        max_pointsdropped=tc.max_pointsdropped,max_pointsadded=tc.max_pointsadded
        from team_changes tc
        where tc.team = pp.team and pp.season=tc.season;
        
        update points_pred pp
        set three_ar_ly = pa.threear_ly,change_3ar=pa.change_3ar,per_ly=pa.per_ly,change_per=pa.change_per,
        usagerank=pa.usagerank,usagerank_ly=pa.usagerank_ly,offensive_winshares=pa.offensive_winshares,
        offensive_boxplusminus=pa.offensive_boxplusminus,boxplusminus=pa.boxplusminus,value_overreplacement=pa.value_overreplacement
        from player_advstats pa
        where pp.player = pa.player and pp.season = pa.season and pp.team = pa.startingteam;
        
        update points_pred pp
        set career_points = pc.career_points, yearspro = pc.yearspro
        from player_careerstats pc
        where pp.player = pc.player and pp.season = pc.season;
        
        
        select * from points_pred where season>2009
        '''


        cur.execute(query)
        data = cur.fetchall()
        points_df = pd.DataFrame(np.array(data))
        points_df.columns = ['season','player','age','team','points','points_ly','change_points_ly','starter_change','high_usageplayer_added','usagemin_opened','maxusage_added','high_usageplayer_dropped','points_opened','max_pointsdropped',
                            'max_pointsadded','three_ar_ly','change_3ar','per_ly','change_per','usagerank','usagerank_ly','offensive_winshares','offensive_boxplusminus','boxplusminus','value_overreplacement','career_points','yearspro']
    
    def rank_players(self):
        self.player_rater = Player_ranker(df)
        self.player_rater.get_category_dist()
        self.player_rater.assign_values()
        self.value_copy = player_rater.value.copy()
        


