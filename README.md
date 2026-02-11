# MLB 2025 Data Engineering Project

## Project Overview

This project implements a complete Extraction, Transformation and Loading (ETL) pipeline for Major League Baseball play-by-play data from the 2025 season. The solution transforms raw CSV data, using Pandas in Python, into a normalized PostgreSQL database schema optimized for the advanced baseball analytics.

## Project Structure

**Part 1 (Table Definitions)** : `schema.sql`\
**Part 2 (Transformation and Load Code)** : `etl_pipeline.py`\
**Part 3 (Analysis Query)** : `queries.py`\
**Part 3 Answers** : Located at the bottom of the README\

```
mlb-2025-analytics/
|-- MLB_Data_2025           #Provided Data (csv files)
|-- .env                    #Database configurations
|-- queries.sql             Part 3: Analytical SQL Queries (SQL).
|-- etl_pipeline.py         Part 2: Main Transform and Load Orchestration Script (Python).
|-- schema.sql              Part 1: Database Schema Definitions (SQL).
|-- README.md               
|-- requirements.txt
|-- .gitignore
```
## How to run this project:

### Prerequisites

1. Python 3.9+
2. PostgreSQL (Local instance or Cloud Instance)
3. Pip Installer


### Setup & Installation

#### 1. Clone this Git repo in your local machine. 

Create a folder in your local.
Open the terminal within that folder and type :
``` bash
git clone https://github.com/msgokul/mlb-2025-analytics.git
```
#### 2. Configure Database connection

After the project is cloned in the local, create an **.env** file in the root directory of the project.
Within this **.env** file paste your Database connection URL and save it.

```bash
#For Local Postgres
DATABASE_URL = postgresql://<username>:<password>@localhost:5432/<your db>
```

**Important** : Replace `username` , `password`, `your db` with your respective credentials

#### 3. Install Python Dependencies

Open the terminal within the project folder. Then type this in the terminal:

```bash
pip install -r requirements.txt
```
---

### How to execute the files:

#### 1. Initialize DB and create the schema (Part 1)

To create the schema for the tables - **game**, **linescore**, **runner_play** run the **schema.sql** file in the terminal opened in the root directory as follows:

```bash
psql -U <user> -d <your db> -f schema.sql
                OR
psql postgresql://<username>:<password>@localhost:5432/<your db> -f schema.sql
```

**Important** : Replace `user` and `db` with your respective credentials and dbname.

#### 2. Run the Transform and Load Python File (Part 2)

After successfully creating the table schema, run the `etl_pipeline.py` Python file to transform the raw data csv files and load the result into the Postgres DB. For that run the following in the terminal opened with the root directoy of the project:

```bash
python etl_pipeline.py
```
#### 3. Run the Analytical SQL Queries (Part 3)

To get the result of the 6 Analytical queries asked, run the **queries.sql** file in the terminal opened in the root directory as follows:

```bash
psql -U <user> -d <your db> -f queries.sql
                 OR
psql postgresql://<username>:<password>@localhost:5432/<your db> -f queries.sql
```

**Important** : Replace `user` , `password`and `db` with your respective credentials and dbname.


## Results of the 6 Analytical Queries. (Part 3)

### 3a. Who were the top 5 teams in regular season wins in 2025? (gametype = R)

**Answer**
```
 team_id |         team          | wins
---------+-----------------------+------
     158 | Milwaukee Brewers     |   97
     143 | Philadelphia Phillies |   96
     147 | New York Yankees      |   94
     141 | Toronto Blue Jays     |   94
     119 | Los Angeles Dodgers   |   93
(5 rows)
```

### 3b. Which players had 35 or more stolen bases in the 2025 MLB regular season?

**Answer**
```
 runnerid |  runnerfullname  | stolen_bases
----------+------------------+--------------
   676609 | JosΘ Caballero   |           46
   608070 | JosΘ Ramφrez     |           44
   802415 | Chandler Simpson |           40
   665742 | Juan Soto        |           38
   665833 | Oneil Cruz       |           37
   677951 | Bobby Witt Jr.   |           35
   682829 | Elly De La Cruz  |           35
(7 rows)
```

### 3c. Which 10 players had the most "extra bases taken"? - where a baserunner advanced more than 1 base on a single or more than 2 bases on a double

**Answer**
```
 runnerid |  runnerfullname  | extra_bases_taken
----------+------------------+-------------------
   676391 | Ernie Clement    |                14
   694192 | Jackson Chourio  |                13
   677951 | Bobby Witt Jr.   |                13
   668930 | Brice Turang     |                12
   682829 | Elly De La Cruz  |                12
   621439 | Byron Buxton     |                12
   802415 | Chandler Simpson |                11
   608070 | JosΘ Ramφrez     |                11
   683002 | Gunnar Henderson |                11
   678246 | Miguel Vargas    |                10
(10 rows)
```

### 3d. What team (in what gamepk) had the largest deficit but came back to win in 2025?

**Answer**
```

 gamepk | team_id |    team_name     | max_deficit
--------+---------+------------------+-------------
 776919 |     115 | Colorado Rockies |          -9
(1 row)
```

### 3e. Which 5 teams had the most come-from-behind wins in all games in 2025?

**Answer**
```
 team_id |       team_name       | come_from_behind_wins
---------+-----------------------+----------------------
     141 | Toronto Blue Jays     |                   63
     119 | Los Angeles Dodgers   |                   61
     136 | Seattle Mariners      |                   55
     143 | Philadelphia Phillies |                   54
     118 | Kansas City Royals    |                   49
(5 rows)
```

### 3f. Write a query that identifies the most aggressive baserunners and explain your reasoning

**Answer**
```

 runnerid |  runnerfullname   | aggressive_run_percentage | stolen_base_success_rate | extra_bases_on_singles_percentage | extra_bases_on_doubles_percentage
----------+-------------------+---------------------------+--------------------------+-----------------------------------+-----------------------------------
   693459 | Kyler Fedko       |                    0.1667 |                          |                            0.1667 |                            0.0000
   690985 | Colby Halter      |                    0.1538 |                   1.0000 |                            0.0769 |                            0.0000
   806964 | Sebastian Walcott |                    0.1333 |                          |                            0.0667 |                            0.0000
   683006 | Chris Newell      |                    0.1250 |                   1.0000 |                            0.0000 |                            0.0000
   640459 | Brian Navarreto   |                    0.0952 |                          |                            0.0476 |                            0.0000
   695603 | Benny Montgomery  |                    0.0952 |                   1.0000 |                            0.0952 |                            0.0000
   624523 | Riley Unroe       |                    0.0952 |                   1.0000 |                            0.0476 |                            0.0000
   802355 | Tanner Schobel    |                    0.0909 |                          |                            0.0909 |                            0.0000
   680709 | Nick Dunn         |                    0.0909 |                          |                            0.0000 |                            0.0000
   803475 | Colby Jones       |                    0.0909 |                          |                            0.0000 |                            0.0000
(10 rows)

```
### Reason:
For measuring aggressive base running, I chose to track:
1. **Aggressive_Run_Percentage**: How often the runners advances from 1st base to 3rd base, or from 2nd to Home
2. **stolen_base_success_rate**: How effectively a runner steals a base successfully relative to the number of total attempts taken.
3. **extra_bases_percentage**: How frequently a runner takes extra bases on Singles and Doubles .

These key metrics provide an idea on how the runners are trying to be more aggressive to reach more bases than possible or to score runs faster than playing normal.