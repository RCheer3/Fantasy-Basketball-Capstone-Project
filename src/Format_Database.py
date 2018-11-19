import psycopg2 as pg2
import pandas as pd
import numpy as np
from Player_rank import Player_ranker
from sqlalchemy import create_engine

def rank_players(df):
    player_rater = Player_ranker(df)
    player_rater.get_category_dist()
    player_rater.assign_values()
    value_copy = player_rater.value.copy()
    return value_copy


'''
Format Database:
This gathers all of the queries needed to do any data manipulation to the basketball reference data.

It first loads in the files, then creates many different tables based on what features are important for each individual category.

The intermediate tables are used to store players career data and teammates data so that we can use that information to predict what a players future statistics will be.

'''



conn = pg2.connect(dbname = 'postgres',host='localhost')
conn.autocommit = True
cur = conn.cursor()
regular = pd.read_csv('~/Desktop/Galvanize/Capstone/Fantasy-Basketball-Capstone-Project/NBA stats.csv')
advanced = pd.read_csv('~/Desktop/Galvanize/Capstone/Fantasy-Basketball-Capstone-Project/NBA Advanced.csv')

cur.execute('DROP DATABASE IF EXISTS nba_capstone;')  
cur.execute('CREATE DATABASE nba_capstone;')
cur.close()
conn.close()
conn = pg2.connect(dbname = 'nba_capstone',host='localhost')
conn.autocommit = True
cur = conn.cursor()

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

cur.execute(query)


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

    DROP TABLE IF EXISTS adv_withminrank;
    CREATE TABLE adv_withminrank AS
    select y.*,n.per,n.true_shooting,n.three_attempt_rate,n.ft_rate,n.rebound_percentage,n.assist_percentage
    ,n.steal_percentage,n.block_percentage,n.turnover_percentage,n.usage_percentage,n.offensive_winshares,
    n.defensive_winshares,winshares,winshares_per48,offensive_boxplusminus,defensive_boxplusminus,boxplusminus,value_overreplacement,
    case when cast(y.GS as float)/y.Games >0.6 then 1 else 0 end as starter,
    row_number() over(partition by n.season order by total_mp desc) as min_rank from NBA_advanced n
    inner join y_predictions y
        ON n.player = y.player
        and n.season = y.season
        and n.Tm=y.startingTeam;


    select * from rank_by_minutes          

'''
cur.execute(query1)
data = cur.fetchall()
player_data = pd.DataFrame(np.array(data))
player_data.columns = ['season','player','position','age','team','gamesPlayed','gamesStarted','minutes','points','rebounds','assists','steals','blocks','turnovers','threes_made','FGM','FGA','FTM','FTA','starter','min_rank']
for i in player_data.columns:
    if i not in ['player','team','position']:
        player_data[i]=pd.to_numeric(player_data[i])
value_copy = rank_players(player_data)

engine = create_engine("postgresql://@localhost/nba_capstone")
value_copy.to_sql(name='value', con=engine, if_exists = 'replace', index=False)

query = '''
DROP TABLE IF EXISTS player_value;
CREATE TABLE player_value AS
select ROW_NUMBER() OVER(PARTITION BY season ORDER BY value_tot DESC),* from value;

DROP TABLE IF EXISTS value;

select * from player_value;

'''
cur.execute(query)




query = '''
DROP TABLE IF EXISTS adv_top10min;
CREATE TABLE adv_top10min as 
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

DROP TABLE IF EXISTS Teammate_maxes;
CREATE TABLE Teammate_maxes AS
select R.season,R.startingTeam,R.player,MAX(R2.points) as max_teammatepts, max(r2.rebounds) as max_teammatereb, max(r2.ast) as max_teammateast,
MAX(R2.Tov) as max_teammateTO,MAX(r2.FGA) as max_teammateshot_attempts, cast(NULL as float) as max_teammate_usage 
from rank_by_minutes R
inner join rank_by_minutes R2
    ON R2.startingTeam = R.startingTeam
    and R2.season+1 = R.season
    and R2.player != R.player
group by R.season,R.startingTeam, R.player;


--would like to change this to max teammate and usage usagewithmins rather than usage
update Teammate_maxes T
set max_teammate_usage = usagemins
from
(select T.season,T.startingTeam,T.player,MAX(a2.usage_withMins) as usagemins from Teammate_maxes T
inner join adv_top10min a
    ON A.season = T.season
    and A.tm = T.startingteam
    and A.player != T.player
inner join adv_top10min a2
    ON a2.season+1 = a.season
    and a2.player=a.player
group by T.season,T.startingTeam,T.player) A
WHERE A.season = T.season and  A.startingTeam = T.startingTeam and A.player = T.player;



DROP TABLE IF EXISTS player_stats;
CREATE TABLE player_stats AS
select r.*,r2.starter as starter_ly,r2.points-r3.points as change_points_ly,r2.points as points_ly
,r2.rebounds-r3.rebounds as change_reb_ly, r2.rebounds as rebounds_ly,
r2.ast-r3.ast as change_ast_ly, r2.ast as ast_ly,r2.stl-r3.stl as change_stl_ly,r2.stl as stl_ly,
r2.blk-r3.blk as change_blk_ly, r2.blk as blk_ly,r2.tov-r3.tov as change_tov_ly,r2.tov as tov_ly,
r2.threes_made as threes_ly,r2.threes_made-r3.threes_made as change_threes,r2.fg as fg_ly, r2.fga as fga_ly,
r2.ftm as ftm_ly,r2.fta as fta_ly
from rank_by_minutes r
left join rank_by_minutes r2
    on r.player = r2.player
    and r.season = r2.season+1
left join rank_by_minutes r3
    ON r3.player = r2.player
    and r3.season+1 = r2.season;

DROP TABLE IF EXISTS player_advstats;
CREATE TABLE player_advstats AS
select y.player,y.season,y.startingteam,a.per as per_ly, a.per-a2.per as change_per,a.three_attempt_rate as threeAR_ly,
a.three_attempt_rate-a2.three_attempt_rate as change_3AR, a.rebound_percentage as reb_perc_ly, a.rebound_percentage-a2.rebound_percentage as change_reb_perc
,a.assist_percentage as ast_perc_ly, a.assist_percentage-a2.assist_percentage as change_assist_perc
,a.steal_percentage as stl_perc_ly, a.steal_percentage-a2.steal_percentage as change_stl_perc_ly
,a.block_percentage as blk_perc_ly, a.block_percentage-a2.block_percentage as change_blk_perc_ly
,a.turnover_percentage as TO_perc_ly, a.turnover_percentage-a2.turnover_percentage as change_turnover_perc_ly,
rank() over(partition by y.season,y.startingTeam order by cast(CONCAT(a.usage_percentage*a.minutes,0) as float) DESC) as usagerank,
rank() over(partition by y.season,a.startingTeam order by a.usage_percentage DESC) as usagerank_ly,
a.offensive_winshares,
a.defensive_winshares,a.winshares,a.winshares_per48,a.offensive_boxplusminus,a.defensive_boxplusminus,
a.boxplusminus,a.value_overreplacement        
from y_predictions y
left join adv_withminrank a
    ON a.player = y.player
    and a.season+1 = y.season
left join adv_withminrank a2
    ON a2.player = a.player
    and a2.season+1 = a.season;


DROP TABLE IF EXISTS player_careerstats;
CREATE TABLE player_careerstats AS
select r.player,r.season,SUM(case when r2.player is not null then 1 else 0 end) as YearsPro, SUM(r2.points*r2.games)/SUM(r2.games) as career_points
,SUM(r2.games*r2.rebounds)/SUM(r2.games) as career_rebounds,SUM(r2.games*r2.ast)/SUM(r2.games) as career_ast, SUM(r2.stl*r2.games)/SUM(r2.games) as career_stl
,SUM(r2.blk*r2.games)/SUM(r2.games) as career_blk,SUM(r2.tov*r2.games)/SUM(r2.games) as career_TO, SUM(r2.threes_made*r2.games)/SUM(r2.games) as career_threesmade
,avg(r2.ftm) as career_ftm,avg(r2.fta) as career_fta,avg(r2.fga) as career_fga, avg(r2.fg) as career_fgm
from rank_by_minutes r
inner join rank_by_minutes r2
    ON r.player = r2.player
    and r.season > r2.season
group by r.player,r.season;


DROP TABLE IF EXISTS points_pred;
CREATE TABLE points_pred(
season int, --these come from player_stats
player varchar(50),
Age int,
team varchar(50),
points float, -- these come from player_stats
points_ly float,
change_points_ly float,
starter_change int,
Games int,
C_PF int,
PG int,
SG_SF int,

 -- these come from team_changes
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

max_teammatepts float,
max_teammate_usage float,
max_teammateto float,
max_teammateshot_attempts float,

career_points float,
yearspro int
);

INSERT INTO points_pred(season,player,age,team,points,points_ly,change_points_ly,starter_change,Games,C_PF,PG,SG_SF)
SELECT season,player,age,startingteam,points,points_ly,change_points_ly,starter-starter_ly,Games,
case when pos in ('C','PF') then 1 else 0 end,case when pos='PG' then 1 else 0 end,case when pos in('SG','SF') then 1 else 0 end from player_stats;

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
set max_teammatepts = tm.max_teammatepts,max_teammate_usage=tm.max_teammate_usage,max_teammateto=tm.max_teammateto,
max_teammateshot_attempts=tm.max_teammateshot_attempts
from teammate_maxes tm
where tm.season = pp.season and tm.player = pp.player;

update points_pred pp
set career_points = pc.career_points, yearspro = pc.yearspro
from player_careerstats pc
where pp.player = pc.player and pp.season = pc.season;


DROP TABLE IF EXISTS rebounds_pred;
CREATE TABLE rebounds_pred(
season int, --these come from player_stats
player varchar(50),
Age int,
team varchar(50),
rebounds float, -- these come from player_stats
rebounds_ly float,
change_rebounds_ly float,
Games int,
C_PF int,
PG int,
SG_SF int,
starter_change int, 

-- these come from team_changes
high_usageplayer_added int,
usagemin_opened float,
maxusage_added float,
high_usageplayer_dropped int,
rebounds_opened float,
max_reboundsdropped float,
max_reboundsadded float,

-- from player_advstats
per_ly float,
change_per float,
usagerank float,
usagerank_ly float,
reb_perc_ly float,
change_reb_perc float,
defensive_winshares float,
defensive_boxplusminus float,
boxplusminus float,
value_overreplacement float,

max_teammatereb float,
max_teammate_usage float,
max_teammateto float,
max_teammateshot_attempts float,

career_rebounds float,
yearspro int
);

INSERT INTO rebounds_pred(season,player,age,team,rebounds,rebounds_ly,change_rebounds_ly,Games,starter_change,C_PF,PG,SG_SF)
SELECT season,player,age,startingteam,rebounds,rebounds_ly,change_reb_ly,Games,starter-starter_ly,
case when pos in ('C','PF') then 1 else 0 end,case when pos='PG' then 1 else 0 end,case when pos in('SG','SF') then 1 else 0 end from player_stats;

update rebounds_pred rp
set high_usageplayer_added = tc.high_usageplayer_added,usagemin_opened=tc.usagemin_opened,
maxusage_added=tc.max_usageadded,high_usageplayer_dropped=tc.high_usageplayer_dropped,rebounds_opened=tc.rebounds_opened,
max_reboundsdropped=tc.max_reboundsdropped,max_reboundsadded=tc.max_reboundsadded
from team_changes tc
where tc.team = rp.team and rp.season=tc.season;

update rebounds_pred rp
set per_ly=pa.per_ly,change_per=pa.change_per,usagerank=pa.usagerank,usagerank_ly=pa.usagerank_ly
,reb_perc_ly = pa.reb_perc_ly,change_reb_perc = pa.change_reb_perc,defensive_winshares=pa.defensive_winshares,
defensive_boxplusminus=pa.defensive_boxplusminus,boxplusminus=pa.boxplusminus,value_overreplacement=pa.value_overreplacement
from player_advstats pa
where rp.player = pa.player and rp.season = pa.season and rp.team = pa.startingteam;

update rebounds_pred rp
set max_teammatereb = tm.max_teammatereb,max_teammate_usage=tm.max_teammate_usage,max_teammateto=tm.max_teammateto,
max_teammateshot_attempts=tm.max_teammateshot_attempts
from teammate_maxes tm
where tm.season = rp.season and tm.player = rp.player;

update rebounds_pred rp
set career_rebounds = pc.career_rebounds, yearspro = pc.yearspro
from player_careerstats pc
where rp.player = pc.player and rp.season = pc.season;



DROP TABLE IF EXISTS assists_pred;
CREATE TABLE assists_pred(
season int, --these come from player_stats
player varchar(50),
Age int,
team varchar(50),
ast float, -- these come from player_stats
ast_ly float,
change_ast_ly float,
Games float,
C_PF int,
PG int,
SG_SF int,
starter_change int, 

-- these come from team_changes
high_usageplayer_added int,
usagemin_opened float,
maxusage_added float,
high_usageplayer_dropped int,
assists_opened float,
max_assistsdropped float,
max_assistsadded float,
points_opened float,
threes_opened float,

-- from player_advstats
per_ly float,
change_per float,
usagerank float,
usagerank_ly float,
ast_perc_ly float,
change_assist_perc float,
offensive_winshares float,
offensive_boxplusminus float,
boxplusminus float,
value_overreplacement float,

max_teammateast float,
max_teammate_usage float,
max_teammateto float,
max_teammateshot_attempts float,

career_ast float,
yearspro int
);

INSERT INTO assists_pred(season,player,age,team,ast,ast_ly,change_ast_ly,starter_change,Games,C_PF,PG,SG_SF)
SELECT season,player,age,startingteam,ast,ast_ly,change_ast_ly,starter-starter_ly,Games,
case when pos in ('C','PF') then 1 else 0 end,case when pos='PG' then 1 else 0 end,case when pos in('SG','SF') then 1 else 0 end from player_stats;

update assists_pred ap
set high_usageplayer_added = tc.high_usageplayer_added,usagemin_opened=tc.usagemin_opened,
maxusage_added=tc.max_usageadded,high_usageplayer_dropped=tc.high_usageplayer_dropped,assists_opened=tc.ast_opened,
max_assistsdropped=tc.max_astdropped,max_assistsadded=tc.max_astadded,points_opened = tc.points_opened,threes_opened = tc.threes_opened
from team_changes tc
where tc.team = ap.team and ap.season=tc.season;

update assists_pred ap
set per_ly=pa.per_ly,change_per=pa.change_per,usagerank=pa.usagerank,usagerank_ly=pa.usagerank_ly
,ast_perc_ly = pa.ast_perc_ly,change_assist_perc = pa.change_assist_perc,offensive_winshares=pa.offensive_winshares,
offensive_boxplusminus=pa.offensive_boxplusminus,boxplusminus=pa.boxplusminus,value_overreplacement=pa.value_overreplacement
from player_advstats pa
where ap.player = pa.player and ap.season = pa.season and ap.team = pa.startingteam;

update assists_pred ap
set max_teammateast = tm.max_teammateast,max_teammate_usage=tm.max_teammate_usage,max_teammateto=tm.max_teammateto,
max_teammateshot_attempts=tm.max_teammateshot_attempts
from teammate_maxes tm
where tm.season = ap.season and tm.player = ap.player;

update assists_pred ap
set career_ast = pc.career_ast, yearspro = pc.yearspro
from player_careerstats pc
where ap.player = pc.player and ap.season = pc.season;




DROP TABLE IF EXISTS steals_pred;
CREATE TABLE steals_pred(
season int, --these come from player_stats
player varchar(50),
Age int,
team varchar(50),
stl float, -- these come from player_stats
stl_ly float,
change_stl_ly float,
Games float,
C_PF int,
PG int,
SG_SF int,
starter_change int, 


-- from player_advstats
per_ly float,
change_per float,
stl_perc_ly float,
change_stl_perc_ly float,
defensive_winshares float,
defensive_boxplusminus float,
boxplusminus float,
value_overreplacement float,

max_teammate_usage float,
max_teammateto float,
max_teammateshot_attempts float,

career_stl float,
yearspro int
);

INSERT INTO steals_pred(season,player,age,team,stl,stl_ly,change_stl_ly,starter_change,Games,C_PF,PG,SG_SF)
SELECT season,player,age,startingteam,stl,stl_ly,change_stl_ly,starter-starter_ly,Games,
case when pos in ('C','PF') then 1 else 0 end,case when pos='PG' then 1 else 0 end,case when pos in('SG','SF') then 1 else 0 end from player_stats;


update steals_pred sp
set per_ly=pa.per_ly,change_per=pa.change_per,stl_perc_ly = pa.stl_perc_ly,change_stl_perc_ly = pa.change_stl_perc_ly
,defensive_winshares=pa.defensive_winshares,defensive_boxplusminus=pa.defensive_boxplusminus
,boxplusminus=pa.boxplusminus,value_overreplacement=pa.value_overreplacement
from player_advstats pa
where sp.player = pa.player and sp.season = pa.season and sp.team = pa.startingteam;

update steals_pred sp
set max_teammate_usage=tm.max_teammate_usage,max_teammateto=tm.max_teammateto,max_teammateshot_attempts=tm.max_teammateshot_attempts
from teammate_maxes tm
where tm.season = sp.season and tm.player = sp.player;

update steals_pred sp
set career_stl = pc.career_stl, yearspro = pc.yearspro
from player_careerstats pc
where sp.player = pc.player and sp.season = pc.season;


DROP TABLE IF EXISTS blocks_pred;
CREATE TABLE blocks_pred(
season int, --these come from player_stats
player varchar(50),
Age int,
team varchar(50),
blk float, -- these come from player_stats
blk_ly float,
change_blk_ly float,
Games float,
C_PF int,
PG int,
SG_SF int,
starter_change int, 


-- from player_advstats
per_ly float,
change_per float,
blk_perc_ly float,
change_blk_perc_ly float,
defensive_winshares float,
defensive_boxplusminus float,
boxplusminus float,
value_overreplacement float,

max_teammate_usage float,
max_teammateto float,
max_teammateshot_attempts float,

career_blk float,
yearspro int
);

INSERT INTO blocks_pred(season,player,age,team,blk,blk_ly,change_blk_ly,starter_change,Games,C_PF,PG,SG_SF)
SELECT season,player,age,startingteam,blk,blk_ly,change_blk_ly,starter-starter_ly,Games,
case when pos in ('C','PF') then 1 else 0 end,case when pos='PG' then 1 else 0 end,case when pos in('SG','SF') then 1 else 0 end from player_stats;


update blocks_pred bp
set per_ly=pa.per_ly,change_per=pa.change_per,blk_perc_ly = pa.blk_perc_ly,change_blk_perc_ly = pa.change_blk_perc_ly
,defensive_winshares=pa.defensive_winshares,defensive_boxplusminus=pa.defensive_boxplusminus
,boxplusminus=pa.boxplusminus,value_overreplacement=pa.value_overreplacement
from player_advstats pa
where bp.player = pa.player and bp.season = pa.season and bp.team = pa.startingteam;

update blocks_pred bp
set max_teammate_usage=tm.max_teammate_usage,max_teammateto=tm.max_teammateto,max_teammateshot_attempts=tm.max_teammateshot_attempts
from teammate_maxes tm
where tm.season = bp.season and tm.player = bp.player;

update blocks_pred bp
set career_blk = pc.career_blk, yearspro = pc.yearspro
from player_careerstats pc
where bp.player = pc.player and bp.season = pc.season;

DROP TABLE IF EXISTS threes_pred;
CREATE TABLE threes_pred(
season int, --these come from player_stats
player varchar(50),
Age int,
team varchar(50),
threes_made float, -- these come from player_stats
threes_ly float,
change_threes float,
points_ly float,
change_points_ly float,
starter_change int,
C_PF int,
PG int,
SG_SF int,
Games int,

 -- these come from team_changes
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

max_teammatepts float,
max_teammate_usage float,
max_teammateto float,
max_teammateshot_attempts float,

career_threes float,
yearspro int
);

INSERT INTO threes_pred(season,player,age,team,threes_made,threes_ly,change_threes,points_ly,change_points_ly,starter_change,C_PF,PG,SG_SF,Games)
SELECT season,player,age,startingteam,threes_made,threes_ly,change_threes,points_ly,change_points_ly,starter-starter_ly,
case when pos in ('C','PF') then 1 else 0 end,case when pos='PG' then 1 else 0 end,case when pos in('SG','SF') then 1 else 0 end,Games
from player_stats;

update threes_pred tp
set high_usageplayer_added = tc.high_usageplayer_added,usagemin_opened=tc.usagemin_opened,
maxusage_added=tc.max_usageadded,high_usageplayer_dropped=tc.high_usageplayer_dropped,points_opened=tc.points_opened,
max_pointsdropped=tc.max_pointsdropped,max_pointsadded=tc.max_pointsadded
from team_changes tc
where tc.team = tp.team and tp.season=tc.season;

update threes_pred tp
set three_ar_ly = pa.threear_ly,change_3ar=pa.change_3ar,per_ly=pa.per_ly,change_per=pa.change_per,
usagerank=pa.usagerank,usagerank_ly=pa.usagerank_ly,offensive_winshares=pa.offensive_winshares,
offensive_boxplusminus=pa.offensive_boxplusminus,boxplusminus=pa.boxplusminus,value_overreplacement=pa.value_overreplacement
from player_advstats pa
where tp.player = pa.player and tp.season = pa.season and tp.team = pa.startingteam;

update threes_pred tp
set max_teammatepts = tm.max_teammatepts,max_teammate_usage=tm.max_teammate_usage,max_teammateto=tm.max_teammateto,
max_teammateshot_attempts=tm.max_teammateshot_attempts
from teammate_maxes tm
where tm.season = tp.season and tm.player = tp.player;

update threes_pred tp
set career_threes = pc.career_threesmade, yearspro = pc.yearspro
from player_careerstats pc
where tp.player = pc.player and tp.season = pc.season;


DROP TABLE IF EXISTS turnovers_pred;
CREATE TABLE turnovers_pred(
season int, -- these come from player_stats
player varchar(50),
Age int,
team varchar(50),
turnovers float, 
turnovers_ly float,
change_tov_ly float,
starter_change int,
Games int,
C_PF int,
PG int,
SG_SF int,

 -- these come from team_changes
high_usageplayer_added int,
usagemin_opened float,
maxusage_added float,
high_usageplayer_dropped int,

per_ly float,
change_per float,
usagerank float,
usagerank_ly float,
offensive_winshares float,
offensive_boxplusminus float,

max_teammatepts float,
max_teammate_usage float,
max_teammateto float,
max_teammateshot_attempts float,

career_to float,
yearspro int
);

INSERT INTO turnovers_pred(season,player,age,team,turnovers,turnovers_ly,change_tov_ly,starter_change,Games,C_PF,PG,SG_SF)
SELECT season,player,age,startingteam,tov,tov_ly,change_tov_ly,starter-starter_ly,Games,
case when pos in ('C','PF') then 1 else 0 end,case when pos='PG' then 1 else 0 end,case when pos in('SG','SF') then 1 else 0 end from player_stats;

update turnovers_pred pp
set high_usageplayer_added = tc.high_usageplayer_added,usagemin_opened=tc.usagemin_opened,
maxusage_added=tc.max_usageadded,high_usageplayer_dropped=tc.high_usageplayer_dropped
from team_changes tc
where tc.team = pp.team and pp.season=tc.season;

update turnovers_pred pp
set per_ly=pa.per_ly,change_per=pa.change_per,
usagerank=pa.usagerank,usagerank_ly=pa.usagerank_ly,offensive_winshares=pa.offensive_winshares,
offensive_boxplusminus=pa.offensive_boxplusminus
from player_advstats pa
where pp.player = pa.player and pp.season = pa.season and pp.team = pa.startingteam;

update turnovers_pred pp
set max_teammatepts = tm.max_teammatepts,max_teammate_usage=tm.max_teammate_usage,max_teammateto=tm.max_teammateto,
max_teammateshot_attempts=tm.max_teammateshot_attempts
from teammate_maxes tm
where tm.season = pp.season and tm.player = pp.player;

update turnovers_pred pp
set career_to = pc.career_to, yearspro = pc.yearspro
from player_careerstats pc
where pp.player = pc.player and pp.season = pc.season;



DROP TABLE IF EXISTS percentages;
CREATE TABLE percentages(
season int, --these come from player_stats
player varchar(50),
Age int,
team varchar(50),
FGM float, -- these come from player_stats
FGA float,
FG_percent float,
FG_percent_ly float,
FGM_ly float,
FGA_ly float,
FTM float,
FTA float,
FT_percent float,
FT_percent_ly float,
FTM_ly float,
FTA_ly float,
threes_ly float,
change_threes float,
points_ly float,
change_points_ly float,
starter_change int,
C_PF int,
PG int,
SG_SF int,
Games int,

 -- these come from team_changes
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

max_teammatepts float,
max_teammate_usage float,
max_teammateto float,
max_teammateshot_attempts float,


career_FGM float,
career_FGA float,
career_FGPercent float,
career_FTM float,
career_FTA float,
career_FTPercent float,
yearspro int
);

INSERT INTO percentages(season,player,age,team,FGM,FGA,FG_percent,FG_percent_ly,FGM_ly,FGA_ly,FTM,FTA,FT_percent
,FT_percent_ly,FTM_ly,FTA_ly,threes_ly,change_threes,points_ly,change_points_ly,starter_change,C_PF,PG,SG_SF,Games)
SELECT season,player,age,startingteam,FG,FGA,case when FGA>0 then FG/FGA else 0 end as FG_percent,
case when fga_ly>0 then fg_ly/fga_ly else 0 end as FG_percent_ly,FG_ly,fga_ly,FTM,FTA,
case when FTA>0 then FTM/FTA else 0 end, case when fta_ly>0 then FTM_ly/FTA_ly else 0 end, FTM_ly,FTA_ly,
threes_ly,change_threes,points_ly,change_points_ly,starter-starter_ly,case when pos in ('C','PF') then 1 else 0 end,
case when pos='PG' then 1 else 0 end,case when pos in('SG','SF') then 1 else 0 end,Games
from player_stats;

update percentages tp
set high_usageplayer_added = tc.high_usageplayer_added,usagemin_opened=tc.usagemin_opened,
maxusage_added=tc.max_usageadded,high_usageplayer_dropped=tc.high_usageplayer_dropped,points_opened=tc.points_opened,
max_pointsdropped=tc.max_pointsdropped,max_pointsadded=tc.max_pointsadded
from team_changes tc
where tc.team = tp.team and tp.season=tc.season;

update percentages tp
set three_ar_ly = pa.threear_ly,change_3ar=pa.change_3ar,per_ly=pa.per_ly,change_per=pa.change_per,
usagerank=pa.usagerank,usagerank_ly=pa.usagerank_ly,offensive_winshares=pa.offensive_winshares,
offensive_boxplusminus=pa.offensive_boxplusminus,boxplusminus=pa.boxplusminus,value_overreplacement=pa.value_overreplacement
from player_advstats pa
where tp.player = pa.player and tp.season = pa.season and tp.team = pa.startingteam;

update percentages tp
set max_teammatepts = tm.max_teammatepts,max_teammate_usage=tm.max_teammate_usage,max_teammateto=tm.max_teammateto,
max_teammateshot_attempts=tm.max_teammateshot_attempts
from teammate_maxes tm
where tm.season = tp.season and tm.player = tp.player;

update percentages tp
set career_fgm = pc.career_fgm,career_fga = pc.career_fga,career_FGpercent = case when pc.career_FGA>0 then pc.career_FGM/pc.career_FGA else 0 end,
career_FTM = pc.career_FTM,career_FTA = pc.career_FTA,career_FTPercent = case when pc.career_FTA>0 then pc.career_FTM/pc.career_FTA else 0 end,yearspro = pc.yearspro
from player_careerstats pc
where tp.player = pc.player and tp.season = pc.season;
'''

cur.execute(query)

