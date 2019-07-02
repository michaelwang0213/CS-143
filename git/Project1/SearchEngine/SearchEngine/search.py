#!/usr/bin/python3
import psycopg2
import re
import string
import sys
import math

_PUNCTUATION = frozenset(string.punctuation)

def _remove_punc(token):
    """Removes punctuation from start/end of token."""
    i = 0
    j = len(token) - 1
    idone = False
    jdone = False
    while i <= j and not (idone and jdone):
        if token[i] in _PUNCTUATION and not idone:
            i += 1
        else:
            idone = True
        if token[j] in _PUNCTUATION and not jdone:
            j -= 1
        else:
            jdone = True
    return "" if i > j else token[i:(j+1)]

def _get_tokens(query):
    rewritten_query = []
    tokens = re.split('[ \n\r]+', query)
    for token in tokens:
        cleaned_token = _remove_punc(token)
        if cleaned_token:
            if "'" in cleaned_token:
                cleaned_token = cleaned_token.replace("'", "''")
            rewritten_query.append(cleaned_token)
    return rewritten_query

def getValidPageTurns(num_results, page_num):
    valid_page_nums = []
    prev_valid = False
    next_valid = False
    num_pages = math.ceil(num_results / 20)
    if(num_pages > page_num):
        next_valid = True
    if(page_num > 1):
        prev_valid = True

    i = 0
    j = -5
    while j < 11 and i < 11 and page_num + j <= num_pages:
        if page_num + j > 0 and page_num + j <= num_pages:
            valid_page_nums.append([page_num + j, j])
            i += 1
        j += 1

    return prev_valid, next_valid, valid_page_nums

def search(query, query_type, is_new_query, page_num):
    rows = []
    rewritten_query = _get_tokens(query)
    

    """TODO
    Your code will go here. Refer to the specification for projects 1A and 1B.
    But your code should do the following:
    1. Connect to the Postgres database.
    2. Graciously handle any errors that may occur (look into try/except/finally).
    3. Close any database connections when you're done.
    4. Write queries so that they are not vulnerable to SQL injections.
    5. The parameters passed to the search function may need to be changed for 1B. 
    """
    '''1'''
    try:
        connection = psycopg2.connect(user = "cs143",
            password = "cs143",
            host = "127.0.0.1",
            dbname = "searchengine")

    except psycopg2.OperationalError as e:
        print('Error, Unable to connect: {}'.format(e))

    else:
        try:
            
            cursor = connection.cursor()

            if is_new_query:
                required_tokens = 0
                if query_type == "and":
                    required_tokens = len(rewritten_query)
                else:
                    required_tokens = 1
                
                string_string = ""
                for token in rewritten_query:
                    string_string += "%s, "

                if(len(rewritten_query) >= 1):
                    string_string = string_string[:-2]

                my_query = """
                    DROP MATERIALIZED VIEW IF EXISTS my_view;
                    CREATE MATERIALIZED VIEW my_view AS
                        SELECT S.song_name AS song, A.artist_name, S.page_link
                        FROM (
                            Select song_id
                            From tfidf
                            Where token In (""" + string_string + """)
                            Group By song_id
                            Having Count(token) >= %s
                            ) L
                            Join tfidf R On L.song_id = R.song_id
                            Join song S On S.song_id = R.song_id
                            Join artist A On A.artist_id = S.artist_id
                            Group By L.song_id, A.artist_name, S.song_name, S.page_link
                            Order By SUM(score) Desc;
                    SELECT COUNT(song) FROM my_view;
                """
               
                my_data = rewritten_query
                my_data.append(required_tokens)
                cursor.execute(my_query, my_data)
                row = cursor.fetchall()

                num_results = row[0][0]

                my_query = "SELECT * FROM my_view LIMIT 20"
                my_data = []
            else:
                my_query = "SELECT COUNT(song) From my_view;"
                my_data = []
                cursor.execute(my_query, my_data)
                row = cursor.fetchall()
                num_results = row[0][0]

                my_query = "SELECT * FROM my_view LIMIT 20 OFFSET %s"
                my_data = []
                my_data.append((page_num - 1) * 20)
            
            # Shamelessly taken from https://stackoverflow.com/questions/283645/python-list-in-sql-query-as-parameter
            # the stackoverflow code is in mysql. The ? had to be replaced with "%s, " and the query data format was slightly different.

            cursor.execute(my_query, my_data)
            row = cursor.fetchall()
            for result in row:
                song_id, token, score = result
                rows.append(result)

        except Exception as e:
            print('Error encountered while executing query: {}'.format(e))

        finally:
            connection.commit()
            connection.close()

    return rows, num_results

if __name__ == "__main__":
    if len(sys.argv) > 2:
        result= search(' '.join(sys.argv[2:]), sys.argv[1].lower(), False, 2)
        print(result)
    else:
        print("USAGE: python3 search.py [or|and] term1 term2 ...")

