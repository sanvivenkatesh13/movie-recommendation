from user_based import compute_user_based_similarity
from item_based import compute_item_based_recommendations

def main():
    print("Movie Recommendation System")
    print("1: Item-Based Collaborative Filtering")
    print("2: User-Based Collaborative Filtering")
    
    choice = input("Select an option (1 or 2): ")

    if choice == "1":
        movie_name = input("Enter a movie name: ")
        try:
            result = compute_user_based_similarity(movie_name)
            print("\nTop 10 Similar Movies:\n", result.head(10))
        except ValueError as e:
            print(e)

    elif choice == "2":
        user_id = input("Enter User ID: ")
        try:
            user_id = int(user_id)  # Ensure it's an integer
            result = compute_item_based_recommendations(user_id)
            print("\nTop Recommended Movies:\n", result)
        except ValueError as e:
            print(e)
        except Exception:
            print("Invalid input. Please enter a valid User ID.")

    else:
        print("Invalid choice. Please enter 1 or 2.")

if __name__ == "__main__":
    main()
2
