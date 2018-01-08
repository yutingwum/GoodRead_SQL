import petl as etl
import sqlite3

ratings = etl.fromcsv('ratings.csv')
to_read = etl.fromcsv('to_read.csv')
tags = etl.fromcsv('tags.csv')
books = etl.fromcsv('books.csv')
book_tags = etl.fromcsv('book_tags.csv')
conn = sqlite3.connect('goodbooks.db')
cur = conn.cursor()

# create the tables and populate them from the csv files
cur.execute('DROP TABLE IF EXISTS Tags')
statement = 'CREATE TABLE IF NOT EXISTS '
statement += 'Tags (tag_id INTEGER PRIMARY_KEY, tag_name CHAR)'
cur.execute(statement)
tags.todb(conn,'Tags')

cur.execute('DROP TABLE IF EXISTS Books')
statement = 'CREATE TABLE IF NOT EXISTS '
statement += 'Books (book_id INTEGER PRIMARY_KEY, goodreads_book_id INTEGER, best_book_id INTEGER, work_id, INTEGER, books_count INTEGER, isbn INTEGER, isbn13 INTEGER, authors TEXT, original_publication_year INTEGER, original_title TEXT, title TEXT, language_code TEXT, average_rating DIGIT, ratings_count INTEGER, work_ratings_count INTEGER, work_text_reviews_count INTEGER, ratings_1 INTEGER, ratings_2 INTEGER, ratings_3 INTEGER, ratings_4 INTEGER, ratings_5 INTEGER, image_url TEXT, small_image_url TEXT)'
cur.execute(statement)
books.todb(conn, 'Books')

cur.execute('DROP TABLE IF EXISTS Ratings')
statement = 'CREATE TABLE IF NOT EXISTS '
statement += 'Ratings (user_id INTEGER, book_id INTEGER, rating INTEGER)'
cur.execute(statement)
ratings.todb(conn,'Ratings')

cur.execute('DROP TABLE IF EXISTS BookTags')
statement = 'CREATE TABLE IF NOT EXISTS '
statement += 'BookTags (goodreads_book_id INTEGER, tag_id INTEGER, count INTEGER)'
cur.execute(statement)
book_tags.todb(conn,'BookTags')


cur.execute('DROP TABLE IF EXISTS ToRead')
statement = 'CREATE TABLE IF NOT EXISTS '
statement += 'ToRead (user_id INTEGER, book_id INTEGER)'
cur.execute(statement)
to_read.todb(conn,'ToRead')


#################################################################

# Answer the question

# make query to get authors and book_id from the book table
q1 = 'SELECT authors, book_id FROM Books'
cur.execute(q1)
authors_books = cur.fetchall()
# create a list of tuples to store the authors and book_id
author_book_tuple_list = []
for item in authors_books:
    authors = item[0].split(',')
    for author in authors:
        author = author.strip()
        author = author.replace('\w+', ' ')
        author_book_tuple_list.append((author, item[1]))


# create an author table with two columns: author, book_id
# use the author, book_id tuple list to populate the table
cur.execute('DROP TABLE IF EXISTS Authors')
statement = 'CREATE TABLE IF NOT EXISTS '
statement += 'Authors (author TEXT, book_id INTEGER)'
cur.execute(statement)
table_spec = 'INSERT INTO Authors VALUES (?, ?)'
for i in author_book_tuple_list:
    cur.execute(table_spec, i)
conn.commit()




# Find the most highly-rated authors
# create a table that stores the author name, the book, and the average rating of the book
cur.execute('DROP TABLE IF EXISTS Authors_Ratings')
statement = 'CREATE TABLE IF NOT EXISTS '
statement += 'Authors_Ratings (author TEXT, book_id INTEGER, avg_rating INTEGER)'
cur.execute(statement)

# select author, book_id, average_rating from joining the Authors and Books table
q3 = 'SELECT Authors.author, Authors.book_id, Books.average_rating FROM Authors JOIN Books on Authors.book_id = Books.book_id'
cur.execute(q3)
ratings_results = cur.fetchall()
# put the query into populating the Authors_Ratings table
author_ratings_table_spec = 'INSERT INTO Authors_Ratings VALUES (?, ?, ?)'
for i in ratings_results:
    cur.execute(author_ratings_table_spec, i)
conn.commit()

# count the sum of all the average ratings, and the number of all the average ratings of each author
statement = 'select author, sum(avg_rating), count(*) from Authors_Ratings group by author'
cur.execute(statement)
author_ratings = cur.fetchall()
# create a list of tuples, where the first element in the tuple is the author, second element is the average of all the average ratings
popular_authors = []
for i in author_ratings:
    popular_authors.append((i[0], round(i[1]/i[2], 2)))
# sort the list by rating, from high to low
popular_authors.sort(key=lambda x:-x[1])
print("The 10 most highly-rated authors are: ")
count = 1
for i in popular_authors:
    if count > 10:
        break
    print(i[0])
    count = count + 1



# create a table that stores the author, and his average of all his average ratings
# use the list of tuples to populate the table
cur.execute('DROP TABLE IF EXISTS Most_HighlyRated_Authors')
statement = 'CREATE TABLE IF NOT EXISTS '
statement += 'Most_HighlyRated_Authors (author TEXT, avg_rating INTEGER)'
cur.execute(statement)
highlyrated_authors_table_spec = 'INSERT INTO Most_HighlyRated_Authors VALUES (?, ?)'
for i in popular_authors:
    cur.execute(highlyrated_authors_table_spec, i)
conn.commit()




# Find the most popular authors on people's to read list
# make an author toread table with three columns: author, book_id, user_id
cur.execute('DROP TABLE IF EXISTS Authors_To_Read')
statement = 'CREATE TABLE IF NOT EXISTS '
statement += 'Authors_To_Read (author TEXT, book_id INTEGER, user_id INTEGER)'
cur.execute(statement)

# make a query to select author, book_id, and user id from joining the Authors table and the ToRead table
q2 = 'SELECT Authors.author, Authors.book_id, ToRead.user_id FROM Authors JOIN ToRead on Authors.book_id = ToRead.book_id'
cur.execute(q2)
joined_result = cur.fetchall()
#use the query result to populate the Authors_To_Read table
toread_table_spec = 'INSERT INTO Authors_To_Read VALUES (?, ?, ?)'
for i in joined_result:
    cur.execute(toread_table_spec, i)
conn.commit()

# From the table, count the occurrences of each author, that will be the number of times the author's books has been added to a toread lsit
statement = 'select author, count(*) from Authors_To_Read group by author'
cur.execute(statement)
rankings = cur.fetchall()
# sort the list by number of occurrences, from high to low
rankings.sort(key=lambda x: -x[1])
print("\nThe 10 most popular authors in to read list are: ")
count = 1
for i in rankings:
    if count > 10:
        break
    print(i[0])
    count = count + 1

# create a Most_Popular_ToRead_Author table to store the data that can answer the question
cur.execute('DROP TABLE IF EXISTS Most_Popular_ToRead_Author')
statement = 'CREATE TABLE IF NOT EXISTS '
statement += 'Most_Popular_ToRead_Author (author TEXT, toread_count INTEGER)'
cur.execute(statement)

# use the author, toread tuple list to populate the table
ranking_table_spec = 'INSERT INTO Most_Popular_ToRead_Author VALUES (?, ?)'
for i in rankings:
    cur.execute(ranking_table_spec, i)
conn.commit()


















