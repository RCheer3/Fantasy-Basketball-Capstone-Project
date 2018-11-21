# Fantasy-Basketball-Capstone-Project
This is a fantasy basketball draft kit which gives predictions for the upcoming year.  You can also use predictions to make in season adjustments, trades, adds and drops by entering in your team and the opposing to compare rough statistics and categories that you may be able to flip

## The Data
Season long traditional and advanced statistics were gathered from basketball-reference.com for 2005-2018.  All data was loaded into and stored in PostgreSQL Databases.

## Ranking Players
Players are ranked based on a sum of values of all 9 standard categories (Points, Rebounds, Assists, Steals, Blocks, Threes, Turnovers, FG%, FT%).  The top 200 players based on minutes are used to get mean and SD in each category and each player is ranked in each category based on standard deviations above the mean.

## Finding Features
There are many things that can have an impact on a players performance.  The most important features are the players last season and career statistics as players skill level is the best predictor of their performance.  However, there are many other things that can be used to explain the remaining variance.  Age as a non-linear polynomial (along with years pro) acts as a big indicator, and the usage ranking of that player (highest usage player on the team generally has the ball a lot).

Then there are the less obvious features.  Finding the changes in teammates and how that impacts players are huge.  A big indicator of change in performance is whether or not a player gained or lost a starting spot.  If you lose a starting spot, your minutes are likely to decrease and your production likely to drop.  In addition, if you added a star player, even if you are still playing, there is only one basketball so if they are taking a lot of shots you don't get as many.  If you added a big man that gets 15 rebounds a game, there aren't as many rebounds for you to get.


## Testing
In this project I used k-fold cross validation to test grid searched Gradient Boosting Regressors against Linear Regression and Rectified Linear Unit-based 3-4 layer Neural Networks.
