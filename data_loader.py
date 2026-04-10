import pandas as pd
import sqlite3
import os

# --- Configuration ---
DATABASE_FILE = 'movie_ratings.db'
RATINGS_TABLE_NAME = 'ratings'
MOVIES_TABLE_NAME = 'movies'

def create_db_connection():
    """Establishes and returns a connection to the SQLite database."""
    # Check if the database file exists before trying to connect
    if not os.path.exists(DATABASE_FILE):
        raise FileNotFoundError(f"Database file not found: {DATABASE_FILE}. Please run the setup script first.")
    
    # Return the connection object
    return sqlite3.connect(DATABASE_FILE)


def load_ratings():
    """
    Loads user ratings data from the 'ratings' table in the database.
    (Columns: user_id, item_id -> movie_id, rating)
    """
    conn = create_db_connection()
    print(f"Loading ratings data from {RATINGS_TABLE_NAME} table...")
    
    # SQL Query: Select the columns needed for the ratings DataFrame.
    # We rename 'item_id' in the SQL query to 'movie_id' to match the original function's output name.
    query = f"""
    SELECT 
        user_id, 
        item_id AS movie_id, 
        rating
    FROM {RATINGS_TABLE_NAME};
    """
    
    try:
        # pd.read_sql_query executes the query and returns a DataFrame
        ratings_df = pd.read_sql_query(query, conn)
    finally:
        conn.close() # Always close the connection
        
    return ratings_df


def load_movies():
    """
    Loads movie titles from the 'movies' table in the database.
    (Columns: movie_id, movie_title -> title)
    """
    conn = create_db_connection()
    print(f"Loading movie metadata from {MOVIES_TABLE_NAME} table...")
    
    # SQL Query: Select the columns needed for the movies DataFrame.
    # We rename 'movie_title' in the SQL query to 'title' to match the original function's output name.
    query = f"""
    SELECT 
        movie_id, 
        movie_title AS title
    FROM {MOVIES_TABLE_NAME};
    """
    
    try:
        movies_df = pd.read_sql_query(query, conn)
    finally:
        conn.close() # Always close the connection
        
    return movies_df


def get_merged_data():
    """Loads ratings and movie titles and merges them into a single DataFrame."""
    # These calls now read from the database instead of files
    ratings = load_ratings()
    movies = load_movies()
    
    # Use 'movie_id' as the merge key, which is the standard name in both loaded DataFrames
    return pd.merge(movies, ratings, on='movie_id')


if __name__ == "__main__":
    try:
        df = get_merged_data()
        print("\n✅ Data loading successful.")
        print("Merged DataFrame Head:")
        print(df.head())
        print(f"\nDataFrame shape: {df.shape}")
    except FileNotFoundError as e:
        print(f"ERROR: {e}")
        print("Please ensure your database file 'movie_ratings.db' is in the same directory.")
    except Exception as e:
        print(f"An error occurred during data loading: {e}")