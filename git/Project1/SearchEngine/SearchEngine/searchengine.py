#!/usr/bin/python3

from flask import Flask, render_template, request

import search

application = app = Flask(__name__)
app.debug = True



@app.route('/search', methods=["GET"])
def dosearch():
    query = request.args['query']
    qtype = request.args['query_type']
    page_num = 1

    is_new_query = True

    """
    TODO:
    Use request.args to extract other information
    you may need for pagination.
    """
    if(query != ""):
        search_results, num_results = search.search(query, qtype, is_new_query, page_num)
    else:
        search_results = []
        num_results = 0

    prev_valid, next_valid, valid_page_nums = search.getValidPageTurns(num_results, page_num)

    result_nums = [(page_num-1)*20 + 1, min((page_num-1)*20 + 20, num_results)]

    return render_template('results.html',
            query=query,
            results=len(search_results),
            search_results=search_results,
            page_num=page_num,
            num_results=num_results,
            result_nums=result_nums,
            prev_valid=prev_valid,
            next_valid=next_valid,
            valid_page_nums=valid_page_nums)

@app.route('/search_old', methods=["GET"])
def dosearch_old():
    query = request.args['query']
    qtype = ""
    page_num = int(request.args['page_num'])
    page_turns = int(request.args['page_turns'])

    is_new_query = False
    page_num = page_num + page_turns

    """
    TODO:
    Use request.args to extract other information
    you may need for pagination.
    """
    print(page_num)
    print(is_new_query)

    search_results, num_results = search.search(query, qtype, is_new_query, page_num)

    prev_valid, next_valid, valid_page_nums = search.getValidPageTurns(num_results, page_num)

    result_nums = [(page_num-1)*20 + 1, min((page_num-1)*20 + 20, num_results)]

    return render_template('results.html',
            query=query,
            results=len(search_results),
            search_results=search_results,
            page_num=page_num,
            num_results=num_results,
            result_nums=result_nums,
            prev_valid=prev_valid,
            next_valid=next_valid,
            valid_page_nums=valid_page_nums)

@app.route("/", methods=["GET"])
def index():
    if request.method == "GET":
        pass
    return render_template('index.html')

if __name__ == "__main__":
    app.run(host='0.0.0.0')
