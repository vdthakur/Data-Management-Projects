# IMPORT LIBRARIES
import sys
from sqlalchemy import create_engine
import pymysql
import pandas as pd

# NOTE: DO NOT modify any variable or function names given in the template!

# Choose EITHER SQLAlchemy OR PyMySQL:

# OPTION 1: SQLAlchemy
# DATABASE_URI = ''

# OPTION 2: PyMySQL
# Replace the placeholders with the actual database connection details

pymysql.install_as_MySQLdb()

DATABASE_URI = 'mysql+mysqldb://dsci551:Dsci-551@localhost/CINEMA'

def get_movies_by_actor(actor_name):
    """
    Fetches titles of movies that the specified actor has acted in.
    INPUT: actor_name (str) - The name of the actor
    RETURN: A list of movie titles (list)
    
    Sample Terminal Command:python3 search.py "John Doe"
    Expected Sample Output: Movies featuring John Doe: ['The Great Adventure', 'Dreams of Space']
    """

    # Placeholder for the result list:
    movies = []
    ## Your code goes here
    connection = create_engine(DATABASE_URI)
    query = f"""
    SELECT m.title 
    FROM Movies m
    INNER JOIN ActIn ai ON m.id = ai.movie_id
    INNER JOIN Actors a ON ai.actor_id = a.id
    WHERE a.name = '{actor_name}'"""

    
    movies_df = pd.read_sql(query, connection)
    for title in movies_df['title']:
        movies.append(title)
    return movies


# Use the below main method to test your code
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python search.py '<actor_name>'")
        sys.exit(1)
    actor_name = sys.argv[1]
    movies = get_movies_by_actor(actor_name)
    if movies:
        print(f"Movies featuring {actor_name}: {movies}")
    else:
        print(f"No movies found for actor {actor_name}.")
