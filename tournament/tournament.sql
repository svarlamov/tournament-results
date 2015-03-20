-- Table definitions for the tournament project.

-- Drop all of our tables to ensure that we are working with a clean DB
DROP TABLE IF EXISTS players CASCADE;
DROP TABLE IF EXISTS matches CASCADE;
DROP VIEW IF EXISTS standings CASCADE;

-- Create the players table that will store all of our players
CREATE TABLE players(
	id SERIAL PRIMARY KEY,
	name TEXT);

-- Create the matches table that will store all of the matches
CREATE TABLE matches(
	id SERIAL PRIMARY KEY,
	winner_id INTEGER REFERENCES players,
	loser_id INTEGER REFERENCES players);

-- Create the standing table that will store all the standings for the tournament
CREATE VIEW standings AS
	SELECT players.id as player_id, players.name, 
		(SELECT count(*) FROM matches WHERE matches.winner_id = players.id) AS matches_won, 
		(SELECT count(*) FROM matches WHERE players.id in (winner_id, loser_id)) as matches_played
		FROM players
	GROUP BY players.id
	ORDER BY matches_won DESC;