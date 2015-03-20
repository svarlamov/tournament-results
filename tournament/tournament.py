#!/usr/bin/env python
# 
# tournament.py -- implementation of a Swiss-system tournament
#

import psycopg2


def connect():
    """Connect to the PostgreSQL database.  Returns a database connection."""
    return psycopg2.connect("dbname=tournament")

def commit_and_close(connection):
    """Commit all changes to the db connection provided, and then close it."""
    # Commit our changes
    connection.commit()
    # Close the DB connection
    connection.close()

def close_conn(connection):
    """Close the specified DB connection."""
    # CLose the connection
    connection.close()

def get_cursor(connection):
    """Return a cursor for the DB connection provided in the parameters."""
    # Return a cursor for the requested db connection
    return connection.cursor()


def deleteMatches():
    """Remove all the match records from the database."""
    # Connect to the DB and get a cursor to execute statements
    connection = connect()
    cursor = get_cursor(connection)

    # Execute a delete statement to delete all of the matches
    cursor.execute("DELETE FROM matches")

    commit_and_close(connection)
    
    


def deletePlayers():
    """Remove all the player records from the database."""
    # Connect to the DB and get a cursor to execute statements
    connection = connect()
    cursor = get_cursor(connection)

    # Execute a delete statement to delete all of the matches
    cursor.execute("DELETE FROM players")
    
    commit_and_close(connection)


def countPlayers():
    """Returns the number of players currently registered."""
    # Connect to the DB and get a cursor to execute statements
    connection = connect()
    cursor = get_cursor(connection)

    # Execute a statement on the DB to get the number of players in the table
    cursor.execute("SELECT COUNT(*) FROM players")
    player_count = cursor.fetchone()[0]
    
    close_conn(connection)

    # Return the number of players
    return player_count


def registerPlayer(name):
    """Adds a player to the tournament database.
  
    The database assigns a unique serial id number for the player.  (This
    should be handled by your SQL database schema, not in your Python code.)
  
    Args:
      name: the player's full name (need not be unique).
    """
    # Connect to the DB and get a cursor to execute statements
    connection = connect()
    cursor = get_cursor(connection)

    # Execute a statement on the DB to add a player to the players table
    cursor.execute("INSERT INTO players (name) VALUES (%s)",(name,))

    commit_and_close(connection)


def playerStandings():
    """Returns a list of the players and their win records, sorted by wins.

    The first entry in the list should be the player in first place, or a player
    tied for first place if there is currently a tie.

    Returns:
      A list of tuples, each of which contains (id, name, wins, matches):
        id: the player's unique id (assigned by the database)
        name: the player's full name (as registered)
        wins: the number of matches the player has won
        matches: the number of matches the player has played
    """
    # Initialize an array to store the resut of our query
    values = []
    # Connect to the DB and get a cursor to execute our query
    connection = connect()
    cursor = get_cursor(connection)

    # Execute a query on the DB to the specified player's standings
    cursor.execute("SELECT player_id, name, matches_won, matches_played FROM standings ORDER BY matches_won DESC");
    
    # Put the results of our query into the array previously initialized at the top of the method
    values = cursor.fetchall()
    
    close_conn(connection)
    
    return values


def reportMatch(winner, loser):
    """Records the outcome of a single match between two players.

    Args:
      winner:  the id number of the player who won
      loser:  the id number of the player who lost
    """
    # Connect to the DB and get a cursor to execute statements
    connection = connect()
    cursor = get_cursor(connection)

    # Insert the results of the match into the table
    cursor.execute("INSERT INTO matches (winner_id, loser_id) VALUES (%s,%s)",(winner,loser))

    commit_and_close(connection)
 
 
def swissPairings():
    """Returns a list of pairs of players for the next round of a match.
  
    Assuming that there are an even number of players registered, each player
    appears exactly once in the pairings.  Each player is paired with another
    player with an equal or nearly-equal win record, that is, a player adjacent
    to him or her in the standings.
  
    Returns:
      A list of tuples, each of which contains (id1, name1, id2, name2)
        id1: the first player's unique id
        name1: the first player's name
        id2: the second player's unique id
        name2: the second player's name
    """
    # Connect to the DB and get a cursor to execute statements
    connection = connect()
    cursor = get_cursor(connection)

    # Get the players and their standings from the standings table in our db
    cursor.execute("SELECT player_id, name, matches_won FROM standings")
    players = cursor.fetchall()

    # Close the connection now since we will not be needing any more in the rest of this statement
    close_conn(connection)

    # Initialize an array for the pairings that we will be creating in the future few lines
    pairings = []
    
    # Pair all of the players
    for i in range(0,len(players) - 1,2):
        pairing = (players[i][0], players[i][1], players[i+1][0],players[i+1][1])
        pairings.append(pairing)
    
    return pairings