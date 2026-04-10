import streamlit as st
import pandas as pd
import numpy as np
import sqlite3

# Import your core logic functions (assuming they are defined in separate files or copied here)
# For this example, we'll assume the functions are defined/imported successfully.
# Replace these imports with your actual function imports if they are not in the same file.
from user_based import compute_user_based_similarity
from item_based import compute_item_based_recommendations


# --- Configuration ---
DATABASE_FILE = 'movie_ratings.db'

# --- Helper Function (Optional but useful for UI) ---

def get_unique_movies():
    """Fetches a list of all movie titles from the database for the selection box."""
    conn = sqlite3.connect(DATABASE_FILE)
    query = "SELECT movie_title FROM movies ORDER BY movie_title;"
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df['movie_title'].tolist()

def get_unique_users():
    """Fetches a list of all user IDs from the database for the selection box."""
    conn = sqlite3.connect(DATABASE_FILE)
    query = "SELECT DISTINCT user_id FROM ratings ORDER BY user_id;"
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df['user_id'].tolist()


# ====================================================================
# 🖥️ STREAMLIT FRONT END LAYOUT (app.py)
# ====================================================================

st.title("🎬 Movie Recommender System")
st.markdown("---")

# 1. Menu Selection (Sidebar for Cleanliness)
st.sidebar.header("Recommendation Mode")
mode = st.sidebar.radio(
    "Select Collaborative Filtering Type:",
    ('Item-Based CF', 'User-Based CF')
)

st.header(f"Mode: {mode}")

# --- Item-Based CF UI ---
if mode == 'Item-Based CF':
    st.markdown("### Find Recommendations for a User")

    # Get list of unique User IDs for the select box
    user_list = get_unique_users()
    
    # 2. Input: User ID Selector
    user_id_input = st.selectbox(
        'Select a User ID:',
        user_list
    )
    
    if st.button('Generate Item-Based Recommendations'):
        if user_id_input:
            try:
                # 3. Call the core logic function
                with st.spinner(f'Finding recommendations for User {user_id_input}...'):
                    recommendations = compute_item_based_recommendations(user_id_input)
                
                # 4. Display Results
                st.success("Recommendation Complete!")
                
                # Format the output for better display
                st.subheader("Top 10 Recommended Movies:")
                st.dataframe(
                    recommendations.reset_index().rename(columns={0: "Recommendation Score"}),
                    hide_index=True
                )
                
            except ValueError as e:
                st.error(f"Error: {e}")
            except Exception as e:
                st.error(f"An unexpected error occurred: {e}")


# --- User-Based CF UI ---
elif mode == 'User-Based CF':
    st.markdown("### Find Movies Similar to a Target Movie")
    
    # Get list of unique Movie Titles for the select box
    movie_list = get_unique_movies()
    
    # 2. Input: Movie Title Selector
    movie_name_input = st.selectbox(
        'Select a Movie Title:',
        movie_list
    )
    
    if st.button('Find Similar Movies'):
        if movie_name_input:
            try:
                # 3. Call the core logic function
                with st.spinner(f'Calculating similarity for "{movie_name_input}"...'):
                    result_df = compute_user_based_similarity(movie_name_input)
                
                # 4. Display Results
                st.success("Similarity Calculation Complete!")
                
                st.subheader(f"Top 10 Movies Most Similar to '{movie_name_input}':")
                st.dataframe(
                    result_df.head(10).reset_index(),
                    hide_index=True
                )
                
            except ValueError as e:
                st.error(f"Error: {e}")
            except Exception as e:
                st.error(f"An unexpected error occurred: {e}")