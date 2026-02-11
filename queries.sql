-- Part 3: Database Queries

-- 3a. Who were the top 5 teams in regular season wins in 2025? (gametype = R)

WITH team_wins as(
    SELECT hometeamid as team_id, hometeamname as team FROM game
    WHERE hometeamscore > awayteamscore AND gametype = 'R'
    UNION ALL
    SELECT awayteamid as team_id, awayteamname as team FROM game
    WHERE awayteamscore > hometeamscore AND gametype = 'R'
)

SELECT team_id, team, COUNT(*) as wins
FROM team_wins
GROUP BY team_id, team
ORDER BY wins DESC
LIMIT 5;

-- 3b. Which players had 35 or more stolen bases in the 2025 MLB regular season?
SELECT r.runnerid, r.runnerfullname, COUNT(*) as stolen_bases
FROM runner_play r 
JOIN game g on r.gamepk = g.gamepk
WHERE r.movementreason LIKE '%stolen_base%'
 AND g.gametype = 'R'
 AND r.is_out = FALSE
GROUP BY r.runnerid, r.runnerfullname
HAVING COUNT(*) >= 35
ORDER BY stolen_bases DESC;

-- 3c. Which 10 players had the most "extra bases taken"? - where a baserunner advanced more 
-- than 1 base on a single or more than 2 bases on a double

SELECT runnerid, runnerfullname, COUNT (*) as extra_bases_taken
FROM runner_play
WHERE (
        (
            eventtype = 'Single' AND 
            (
                (
                (startbase = '1B' AND reachedbase IN ('3B', 'HM')) OR
                (startbase = '2B' AND reachedbase = 'HM')
                )
            )
        )
         OR
         (
            eventtype = 'Double' AND 
            (
                (startbase = '1B' AND reachedbase = 'HM')
            )
       )
)
GROUP BY runnerid, runnerfullname
ORDER BY extra_bases_taken DESC
LIMIT 10;

--3d. What team (in what gamepk) had the largest deficit but came back to win in 2025?

WITH winning_teams AS (
    SELECT gamepk, hometeamid as team_id, hometeamname as team_name
    FROM game
    WHERE hometeamscore > awayteamscore 

    UNION ALL

    SELECT gamepk, awayteamid as team_id, awayteamname as team_name
    FROM game
    WHERE awayteamscore > hometeamscore
), 

max_deficits AS (
    SELECT l.gamepk, t.team_id, t.team_name , MIN(l.battingteam_score_diff) as max_deficit
    FROM linescore l
    JOIN winning_teams t
    ON l.gamepk = t.gamepk AND l.battingteamid = t.team_id
    GROUP BY l.gamepk, t.team_id, t.team_name
)

SELECT gamepk, team_id, team_name, max_deficit
FROM max_deficits
WHERE max_deficit < 0
ORDER BY max_deficit ASC
LIMIT 1;

-- 3e. Which 5 teams had the most come-from-behind wins in all games in 2025?

WITH winning_teams AS (
    SELECT gamepk, hometeamid as team_id, hometeamname as team_name
    FROM game 
    WHERE hometeamscore > awayteamscore

    UNION ALL

    SELECT gamepk, awayteamid as team_id, awayteamname as team_name
    FROM game
    WHERE awayteamscore > hometeamscore
), 

max_deficits AS(
    SELECT l.gamepk, t.team_id, t.team_name, MIN(l.battingteam_score_diff) as max_deficit
    FROM linescore l
    JOIN winning_teams t
    ON l.gamepk = t.gamepk AND l.battingteamid = t.team_id
    GROUP BY l.gamepk, t.team_id, t.team_name
)

SELECT team_id, team_name, COUNT(*) as come_from_beind_wins
FROM max_deficits
WHERE max_deficit < 0
GROUP BY team_id, team_name
ORDER BY come_from_beind_wins DESC
LIMIT 5;

-- 3f. Write a query that identifies the most aggressive baserunners and explain your reasoning

WITH stats_tables AS(
    SELECT runnerid, runnerfullname, 
    SUM (CASE WHEN is_firsttothird OR is_secondtohome THEN 1 ELSE 0 END) as aggressive_runs, 
    SUM (CASE WHEN movementreason ILIKE '%stolen_base%' AND is_out = FALSE THEN 1 ELSE 0 END) as successful_stolen_bases,
    SUM (CASE WHEN movementreason ILIKE '%stolen_base%' AND is_out = TRUE THEN 1 ELSE 0 END) as caught_stealing ,
    SUM (CASE WHEN eventtype = 'Single' AND (
                (startbase = '1B' AND reachedbase IN ('3B', 'HM')) OR
                (startbase = '2B' AND reachedbase = 'HM')
                ) THEN 1 ELSE 0 END) as extra_bases_taken_on_singles,
    SUM (CASE WHEN eventtype = 'Double' AND (
                (startbase = '1B' AND reachedbase = 'HM')
                ) THEN 1 ELSE 0 END) as extra_bases_taken_on_doubles, 
    COUNT(*) as total_plays
    FROM runner_play
    GROUP BY runnerid, runnerfullname
    HAVING COUNT(*) > 10
    )

SELECT runnerid, runnerfullname, 
       ROUND((aggressive_runs::NUMERIC / total_plays), 4) as aggressive_run_percentage, 
       ROUND(successful_stolen_bases::NUMERIC / NULLIF(successful_stolen_bases + caught_stealing, 0), 4) as stolen_base_success_rate,
       ROUND((extra_bases_taken_on_singles::NUMERIC / total_plays), 4) as extra_bases_on_singles_percentage,
       ROUND((extra_bases_taken_on_doubles::NUMERIC / total_plays), 4) as extra_bases_on_doubles_percentage
FROM stats_tables
ORDER BY aggressive_run_percentage DESC, stolen_base_success_rate DESC, extra_bases_on_singles_percentage DESC, extra_bases_on_doubles_percentage DESC
LIMIT 10;


