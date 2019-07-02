import search
import sys

def dosearch_old(page_num, page_turns):
    query = ""
    qtype = ""
    page_num = int(page_num)
    page_turns = int(page_turns)

    is_new_query = False
    page_num = page_num + page_turns

    """
    TODO:
    Use request.args to extract other information
    you may need for pagination.
    """

    search_results = search.search(query, qtype, is_new_query, page_num)
    for result in search_results:
        song, author, score = result
        print(song)


if __name__ == "__main__":
    result= dosearch_old(sys.argv[1], sys.argv[2])
    
