# IMPORT LIBRARIES
import sys
import json
from lxml import etree

# Define any helper functions here
def hashing(author_name):
    hash = len(author_name) % 2
    return hash

def add_book(book_id, book_json):
    # INPUT : book json from command line
    # RETURN : 1 if successful, else 0
    # Assume JSON is well formed with no missing attributes
    try:
        for file, xml in XML_FILES.items():
            tree = etree.parse(xml)
            exists = bool(tree.xpath(f'/bib/book[@id="{book_id}"]'))
            if exists:
                print("Error: Book ID already exists")
                return 0
        # user inputs book information in json format
        book_info = json.loads(book_json)
        # author name is retrieved from the book json information provided
        author_name = book_info.get('author')
        # hash function is ran to determine if info will be stored in file0.xml or file1.xml
        file = hashing(author_name)
        # used hashing result to retrieve the XML file path that will be used to store the info
        xml_hash = XML_FILES[file]
        # parse the XML file
        tree = etree.parse(xml_hash)
        # define the root element of the XML file to a variable
        root_tree = tree.getroot()
        # create a main element Book which has the attribute id for the book added
        book_elem = etree.Element("book", attrib={'id': str(book_id)})
        for key, value in book_info.items():
            # for each key and value in book_json inputted, make the key a sub-element of the main element (Book)
            subelement = etree.SubElement(book_elem, key)
            # for each key related to sub element created (corresponds to a key from book_json), write the value from
            # the corresponding book_json key as the text
            subelement.text = str(value.title())
        # append the book element with its sub elements to the root of XML file
        root_tree.append(book_elem)
        # writes the content to the XMl file using root_tree
        tree.write(xml_hash, xml_declaration=True, encoding='utf-8', pretty_print=True)
        return 1
    except Exception as e:
        print(f"Error: {e}")
        return 0



def search_by_author(author_name):
    # INPUT: name of author
    # RETURN: list of strings containing only book titles
    # EXPECTED RETURN TYPE: ['book title 1', 'book title 2', ...]
    file = hashing(author_name)
    xml_file = XML_FILES[file]
    tree = etree.parse(xml_file)
    author_title_list = []
    find_books_authors = tree.xpath(f'/bib/book[author="{author_name.title()}"]')
    for book in find_books_authors:
        title = book.findtext('title')
        author_title_list.append(title)

    return author_title_list


def search_by_year(year):
    # INPUT: year of publication
    # RETURN: list of strings containing only book titles
    # EXPECTED RETURN TYPE: ['book name 1', 'book name 2', ...]
    year_title_list = []
    for file in XML_FILES:
        xml_file = XML_FILES[file]
        tree_xml = etree.parse(xml_file)
        find_books_years = tree_xml.xpath(f'/bib/book[year="{year}"]')
        for book in find_books_years:
            title = book.findtext('title')
            year_title_list.append(title)

    return year_title_list

# Use the below main method to test your code
if __name__ == "__main__":
    if len(sys.argv) < 5:
        sys.exit("\nUsage: python3 script.py [path/to/file0.xml] [path/to/file1.xml] [operation] [arguments]\n")

    xml0, xml1 = sys.argv[1], sys.argv[2]

    # Assume XML files exist at mentioned path and are initialized with empty <bib> </bib> tags
    global XML_FILES 
    XML_FILES = {
        0: xml0,
        1: xml1
    }

    operation = sys.argv[3].lower()

    if operation == "add_book":
        result = add_book(sys.argv[4], sys.argv[5])
        print(result)
    elif operation == "search_by_author":
        books = search_by_author(sys.argv[4])
        print(books)
    elif operation == "search_by_year":
        year = int(sys.argv[4])
        books = search_by_year(year)
        print(books)
    else:
        sys.exit("\nInvalid operation.\n")
