# IMPORT LIBRARIES

import json
import requests
import sys

# Firebase Database URLs (Replace these URLs with your actual Firebase URLs)
DATABASE_URLS = {
    0: "https://database-0-bbf14-default-rtdb.firebaseio.com/",
    1: "https://database-1-624b3-default-rtdb.firebaseio.com/"
}


## Define any global methods
def hashing(author_name):
    # hash function that returns 0 or 1
    return len(author_name) % 2

def add_book(book_id, book_json):
        # INPUT : book id and book json from command line
        # RETURN : status code after pyhton REST call to add book [response.status_code]
        # EXPECTED RETURN : 200
        author_name = json.loads(book_json)["author"]
        hash_function_result = hashing(author_name)
        # determine which url
        which_url = DATABASE_URLS[hash_function_result]
        # create url
        send_to_url = which_url + "/" + book_id + ".json"
        update_database = requests.put(send_to_url, data=book_json)
        return update_database.status_code


def search_by_author(author_name):
    # Input: Name of the author
    # Return: JSON object with book_ids as keys and book information as values

    hash_function_result = hashing(author_name)
    which_url = DATABASE_URLS[hash_function_result]

    # Append query parameters to filter results based on the author field
    url = f"{which_url}/.json?orderBy=\"author\"&equalTo=\"{author_name}\""

    # Get only the filtered data from Firebase
    response = requests.get(url)
    books_of_author = response.json()

    # Check if any results were found
    if books_of_author is None:
        return {}

    # Return the filtered dictionary directly
    return books_of_author



def search_by_year(year):
    # Input: Year when the book was published
    # Return: JSON object with book_ids as keys and book information as values
    
    books_in_year = {}
    year_str = str(year).strip()  # Ensure the year is a string for comparison

    for all_url in DATABASE_URLS.values():
        # Construct the URL with query parameters to filter by the 'year' field
        url = f"{all_url}/.json?orderBy=\"year\"&equalTo=\"{year_str}\""
        
        # Send a GET request to Firebase to get only the relevant data
        response = requests.get(url)
        filtered_books = response.json()

        # Check if any results were found and merge them into the main dictionary
        if filtered_books:
            books_in_year.update(filtered_books)

    # Return books published in the specified year
    return books_in_year


# Use the below main method to test your code
if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python script.py [operation] [arguments]")

    operation = sys.argv[1].lower()
    if operation == "add_book":
        result = add_book(sys.argv[2], sys.argv[3])
        print(result)
    elif operation == "search_by_author":
        books = search_by_author(sys.argv[2])
        print(books)
    elif operation == "search_by_year":
        year = int(sys.argv[2])
        books = search_by_year(year)
        print(books)
