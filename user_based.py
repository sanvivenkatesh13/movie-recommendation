import pandas as pd
import numpy as np
from data_loader import get_merged_data

def compute_user_based_similarity(movie_name):
    """
    Compute user-based collaborative filtering similarity for a given movie.
    
    Parameters:
    - movie_name (str): The name of the movie to find similar movies for.

    Returns:
    - DataFrame: A sorted dataframe of movies similar to the input movie.
    """
    ratings = get_merged_data()

    # Create a user-movie rating matrix
    movie_ratings = ratings.pivot_table(index='user_id', columns='title', values='rating')

    if movie_name not in movie_ratings:
        raise ValueError(f"Movie '{movie_name}' not found in dataset!")

    # Compute similarity with the selected movie
    movie_ratings_target = movie_ratings[movie_name]
    similarity = movie_ratings.corrwith(movie_ratings_target).dropna()

    # Convert to DataFrame
    df = pd.DataFrame(similarity, columns=['similarity'])

    # Get movie statistics (number of ratings & mean rating)
    movie_stats = ratings.groupby('title').agg({'rating': [np.size, np.mean]})

    # Filter movies with at least 100 ratings
    popular_movies = movie_stats[movie_stats['rating']['size'] >= 100]

    # Flatten MultiIndex column names
    popular_movies.columns = [f'{i}|{j}' if j else f'{i}' for i, j in popular_movies.columns]

    # Merge similarity with popular movies
    df = popular_movies.join(df, how='inner')

    return df.sort_values('similarity', ascending=False)

if __name__ == "__main__":
    try:
        movie_name = input("Enter a movie name: ")
        result = compute_user_based_similarity(movie_name)
        print(result.head(10))
    except ValueError as e:
        print(e)
