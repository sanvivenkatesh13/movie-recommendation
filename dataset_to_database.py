import sqlite3
import csv
import os

# ====================================================================
# 🎬 Configuration
# ====================================================================

DATABASE_FILE = 'movie_ratings.db'

# --- Ratings (u.data) Configuration ---
RATINGS_DATA_FILE = r'C:\Users\Preeya Patil\Desktop\movie_recommendation\data\u.data'
RATINGS_TABLE_NAME = 'ratings'

# --- Movies (u.item) Configuration ---
MOVIE_DATA_FILE = r'C:\Users\Preeya Patil\Desktop\movie_recommendation\data\u.item'
MOVIES_TABLE_NAME = 'movies'
GENRE_COLUMNS = [
    'unknown', 'Action', 'Adventure', 'Animation', "Childrens", 'Comedy', 
    'Crime', 'Documentary', 'Drama', 'Fantasy', 'FilmNoir', 'Horror', 
    'Musical', 'Mystery', 'Romance', 'SciFi', 'Thriller', 'War', 'Western'
]

# ====================================================================
# 🛠️ Ratings Table Functions (u.data)
# ====================================================================

def initialize_ratings_table(database_file, table_name):
    """Connects to the database and creates the ratings table if it doesn't exist."""
    print(f"\n--- Initializing {table_name} Table ---")
    conn = sqlite3.connect(database_file)
    cursor = conn.cursor()

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        user_id INTEGER,
        item_id INTEGER,
        rating INTEGER,
        timestamp INTEGER,
        PRIMARY KEY (user_id, item_id)
    );
    """
    try:
        cursor.execute(create_table_sql)
        conn.commit()
        print(f"Table '{table_name}' ensured/created successfully.")
    except sqlite3.Error as e:
        print(f"An error occurred while creating the table: {e}")
    finally:
        conn.close()

def insert_ratings_data_from_file(data_file, database_file, table_name):
    """Reads rating data (space/tab separated) and inserts it into the SQLite table."""
    
    conn = sqlite3.connect(database_file)
    cursor = conn.cursor()
    insert_sql = f"INSERT INTO {table_name} (user_id, item_id, rating, timestamp) VALUES (?, ?, ?, ?);"
    rows_inserted = 0
    
    try:
        print(f"Reading data from {data_file}...")
        
        data_to_insert = []
        with open(data_file, 'r') as file:
            for line in file:
                parts = [p.strip() for p in line.strip().split()]
                
                if len(parts) == 4:
                    try:
                        # Convert all parts to integers
                        record = tuple(int(p) for p in parts)
                        data_to_insert.append(record)
                    except ValueError:
                        print(f"Skipping line due to invalid integer format: {line.strip()}")
                elif line.strip():
                     print(f"Skipping line due to incorrect column count (expected 4): {line.strip()}")
            
            cursor.executemany(insert_sql, data_to_insert)
            rows_inserted = cursor.rowcount
            conn.commit()
            
    except FileNotFoundError:
        print(f"ERROR: The ratings file '{data_file}' was not found. Please check the path.")
        return
    except sqlite3.Error as e:
        print(f"Database Error during ratings insertion: {e}")
        conn.rollback()
        return
    finally:
        conn.close()

    print(f"✅ Data insertion for **{table_name}** complete. Inserted **{rows_inserted}** rows.")


# ====================================================================
# 🎬 Movies Table Functions (u.item)
# ====================================================================

def initialize_movies_table(database_file, table_name):
    """Connects to the database and creates the movies table if it doesn't exist."""
    print(f"\n--- Initializing {table_name} Table ---")
    conn = sqlite3.connect(database_file)
    cursor = conn.cursor()

    genre_sql_parts = [f"{col} INTEGER" for col in GENRE_COLUMNS]
    
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        movie_id INTEGER PRIMARY KEY,
        movie_title TEXT,
        release_date TEXT,
        video_release_date TEXT,
        IMDb_URL TEXT,
        {', '.join(genre_sql_parts)}
    );
    """
    try:
        cursor.execute(create_table_sql)
        conn.commit()
        print(f"Table '{table_name}' ensured/created successfully.")
    except sqlite3.Error as e:
        print(f"An error occurred while creating the table: {e}")
    finally:
        conn.close()

def insert_movies_data_from_file(data_file, database_file, table_name):
    """Reads movie data (pipe-separated) and inserts it into the SQLite table."""
    
    conn = sqlite3.connect(database_file)
    cursor = conn.cursor()
    
    all_columns = ['movie_id', 'movie_title', 'release_date', 'video_release_date', 'IMDb_URL'] + GENRE_COLUMNS
    placeholders = ', '.join(['?' for _ in all_columns])
    columns_list = ', '.join(all_columns)
    insert_sql = f"INSERT INTO {table_name} ({columns_list}) VALUES ({placeholders});"
    
    rows_inserted = 0
    
    try:
        print(f"Reading data from {data_file}...")
        
        data_to_insert = []
        # Use csv.reader with delimiter='|' and appropriate encoding for MovieLens
        with open(data_file, 'r', encoding='latin-1') as file: 
            reader = csv.reader(file, delimiter='|')
            
            for row in reader:
                if len(row) == 24:
                    movie_data = []
                    
                    try:
                        # 1. movie_id (INTEGER)
                        movie_data.append(int(row[0])) 
                        
                        # 2-5. Text fields (handle empty video_release_date)
                        movie_data.append(row[1])
                        movie_data.append(row[2])
                        movie_data.append(row[3] if row[3] else None) 
                        movie_data.append(row[4])
                        
                        # 6-24. Genre flags (19 INTEGER columns)
                        movie_data.extend([int(x) for x in row[5:]])

                        data_to_insert.append(tuple(movie_data))
                        
                    except ValueError:
                        print(f"Skipping line due to invalid data format: {row}")
                        continue
                
                elif row: 
                    print(f"Skipping line due to incorrect column count (expected 24): {row}")
            
            cursor.executemany(insert_sql, data_to_insert)
            rows_inserted = cursor.rowcount
            conn.commit()
            
    except FileNotFoundError:
        print(f"ERROR: The movies file '{data_file}' was not found. Please check the path.")
        return
    except sqlite3.Error as e:
        print(f"Database Error during movies insertion: {e}")
        conn.rollback()
        return
    finally:
        conn.close()

    print(f"✅ Data insertion for **{table_name}** complete. Inserted **{rows_inserted}** rows.")


# ====================================================================
# 🚀 Main Execution Block
# ====================================================================

if __name__ == "__main__":

    print("Starting database creation and data insertion process...")
    
    # 1. Initialize and Insert Ratings (u.data)
    initialize_ratings_table(DATABASE_FILE, RATINGS_TABLE_NAME)
    insert_ratings_data_from_file(RATINGS_DATA_FILE, DATABASE_FILE, RATINGS_TABLE_NAME)
    
    # 2. Initialize and Insert Movies (u.item)
    initialize_movies_table(DATABASE_FILE, MOVIES_TABLE_NAME)
    insert_movies_data_from_file(MOVIE_DATA_FILE, DATABASE_FILE, MOVIES_TABLE_NAME)
    
    print("\n--- Database Setup Complete! ---")
    
    # 3. Verification (Optional - check both tables)
    
    # Verify Ratings Table
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    print(f"\nVerification: Total count in {RATINGS_TABLE_NAME}:")
    cursor.execute(f"SELECT COUNT(*) FROM {RATINGS_TABLE_NAME};")
    print(cursor.fetchone())
    
    # Verify Movies Table
    print(f"Verification: Total count in {MOVIES_TABLE_NAME}:")
    cursor.execute(f"SELECT COUNT(*) FROM {MOVIES_TABLE_NAME};")
    print(cursor.fetchone())
    
    print("\nVerification: Sample Movie Record:")
    cursor.execute(f"SELECT movie_id, movie_title, release_date, Drama, SciFi FROM {MOVIES_TABLE_NAME} LIMIT 1;")
    columns = [desc[0] for desc in cursor.description]
    print(f"Columns: {columns}")
    print(f"Record: {cursor.fetchone()}")
    
    conn.close()