-- Part 1: Table Definitions

-- Drop tables if they exist
DROP TABLE IF EXISTS game;
DROP TABLE IF EXISTS linescore;
DROP TABLE IF EXISTS runner_play;

-- 1a. Create 'game' table
CREATE TABLE game (
    gamepk INT PRIMARY KEY,
    gamedate TIMESTAMP WITH TIME ZONE NOT NULL,
    officialdate DATE NOT NULL, 
    sportid INT NOT NULL,
    gametype VARCHAR(1) NOT NULL, 
    codedgamestate VARCHAR(10) NOT NULL,
    detailedstate VARCHAR(50) NOT NULL,
    awayteamid INT NOT NULL,
    awayteamname VARCHAR(100) NOT NULL,
    awayteamscore INT ,
    hometeamid INT NOT NULL,
    hometeamname VARCHAR(100) NOT NULL,
    hometeamscore INT,
    venueid INT NOT NULL,
    venuename VARCHAR(100) NOT NULL,
    scheduledinnings INT NOT NULL

);

--1b. Create 'linescore' table
CREATE TABLE linescore (
    gamepk INT NOT NULL, 
    inning INT NOT NULL,
    half INT NOT NULL,
    battingteamid INT NOT NULL,
    runs INT,
    hits INT,
    errors INT, 
    leftonbase INT,
    battingteam_score INT NOT NULL, 
    battingteam_score_diff INT NOT NULL, 
    PRIMARY KEY (gamepk, inning, half)
);

--1c. Create 'runner_play' table
CREATE TABLE runner_play (
    gamepk INT NOT NULL,
    atbatindex INT NOT NULL,
    playindex INT NOT NULL,
    runnerid INT NOT NULL,
    playid UUID,
    runnerfullname VARCHAR(100) NOT NULL,
    startbase VARCHAR(2) NOT NULL, 
    endbase VARCHAR(2) NOT NULL,
    reachedbase VARCHAR(2) NOT NULL,
    is_out BOOLEAN,
    eventtype VARCHAR(50) NOT NULL,
    movementreason VARCHAR(50),
    is_risp BOOLEAN NOT NULL,
    is_firsttothird BOOLEAN NOT NULL,
    is_secondtohome BOOLEAN NOT NULL,
    PRIMARY KEY (gamepk, atbatindex, playindex, runnerid)
);