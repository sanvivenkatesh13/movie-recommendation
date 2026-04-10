import pandas as pd
from data_loader import get_merged_data

def compute_item_based_recommendations(user_id):
    """
    Generate movie recommendations using item-based collaborative filtering.
    :param user_id: The ID of the user for whom we want recommendations.
    :return: A DataFrame containing recommended movies.
    """
    ratings = get_merged_data()

    # Create user-item matrix
    userRatings = ratings.pivot_table(index=['user_id'], columns=['title'], values='rating')

    # Compute item similarity using Pearson correlation with min_periods=100
    corrMatrix = userRatings.corr(method='pearson', min_periods=100)

    # Get the user's ratings (drop missing values)
    if user_id not in userRatings.index:
        raise ValueError(f"User ID {user_id} not found in dataset!")

    myRatings = userRatings.loc[user_id].dropna()

    # Initialize an empty Series to store similarity scores
    simCandidates = pd.Series(dtype='float64')

    # Loop through the movies the user has rated
    for movie in myRatings.index:
        print(f"Processing similarities for '{movie}'...")
        sims = corrMatrix[movie].dropna()  # Get similar movies
        sims = sims.map(lambda x: x * myRatings[movie])  # Scale similarity by rating
        simCandidates = pd.concat([simCandidates, sims])  # Add to similarity candidates

    # Sum scores for movies appearing multiple times
    simCandidates = simCandidates.groupby(simCandidates.index).sum()

    # Remove movies the user has already rated
    filteredSims = simCandidates.drop(myRatings.index, errors="ignore")

    return filteredSims.sort_values(ascending=False).head(10)

if __name__ == "__main__":
    user_id = int(input("Enter User ID: "))
    recommendations = compute_item_based_recommendations(user_id)
    print("\nTop Recommended Movies:\n", recommendations)
