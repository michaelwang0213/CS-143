#!/usr/bin/env python3

"""Clean comment text for easier parsing."""

from __future__ import print_function

import re
import string
import argparse

import json
import sys


__author__ = "Michael Wang"
__email__ = "michaelwang0213@yahoo.com"

# Depending on your implementation,
# this data may or may not be useful.
# Many students last year found it redundant.
_CONTRACTIONS = {
    "tis": "'tis",
    "aint": "ain't",
    "amnt": "amn't",
    "arent": "aren't",
    "cant": "can't",
    "couldve": "could've",
    "couldnt": "couldn't",
    "didnt": "didn't",
    "doesnt": "doesn't",
    "dont": "don't",
    "hadnt": "hadn't",
    "hasnt": "hasn't",
    "havent": "haven't",
    "hed": "he'd",
    "hell": "he'll",
    "hes": "he's",
    "howd": "how'd",
    "howll": "how'll",
    "hows": "how's",
    "id": "i'd",
    "ill": "i'll",
    "im": "i'm",
    "ive": "i've",
    "isnt": "isn't",
    "itd": "it'd",
    "itll": "it'll",
    "its": "it's",
    "mightnt": "mightn't",
    "mightve": "might've",
    "mustnt": "mustn't",
    "mustve": "must've",
    "neednt": "needn't",
    "oclock": "o'clock",
    "ol": "'ol",
    "oughtnt": "oughtn't",
    "shant": "shan't",
    "shed": "she'd",
    "shell": "she'll",
    "shes": "she's",
    "shouldve": "should've",
    "shouldnt": "shouldn't",
    "somebodys": "somebody's",
    "someones": "someone's",
    "somethings": "something's",
    "thatll": "that'll",
    "thats": "that's",
    "thatd": "that'd",
    "thered": "there'd",
    "therere": "there're",
    "theres": "there's",
    "theyd": "they'd",
    "theyll": "they'll",
    "theyre": "they're",
    "theyve": "they've",
    "wasnt": "wasn't",
    "wed": "we'd",
    "wedve": "wed've",
    "well": "we'll",
    "were": "we're",
    "weve": "we've",
    "werent": "weren't",
    "whatd": "what'd",
    "whatll": "what'll",
    "whatre": "what're",
    "whats": "what's",
    "whatve": "what've",
    "whens": "when's",
    "whered": "where'd",
    "wheres": "where's",
    "whereve": "where've",
    "whod": "who'd",
    "whodve": "whod've",
    "wholl": "who'll",
    "whore": "who're",
    "whos": "who's",
    "whove": "who've",
    "whyd": "why'd",
    "whyre": "why're",
    "whys": "why's",
    "wont": "won't",
    "wouldve": "would've",
    "wouldnt": "wouldn't",
    "yall": "y'all",
    "youd": "you'd",
    "youll": "you'll",
    "youre": "you're",
    "youve": "you've"
}

# You may need to write regular expressions.
def findEndOfLink(text, start):
    for j in range(start, len(text)):
        if text[j] == ']':
            #print(text[j+1:j+5])
            end = j;
            if text[j+1:j+6] == "(http":
                #print(j+1)
                for k in range(j+5, len(text)):
                    if text[k] == ' ':
                        return 0, 0
                    if text[k] == ')':
                        return end, k
    return 0,0
def replaceOneLink(text):
    for i in range(0, len(text)):
        if text[i] == '[':
            valueEnd, linkEnd = findEndOfLink(text, i+1)
            if valueEnd != 0:
                #print(valueEnd)
                text = text[:i] + text[i+1: valueEnd] + text[linkEnd+1:]
                return text, False
    return text, True
def replaceLinks(text):
    done = False
    while done == False: 
        text, done = replaceOneLink(text)
    return text       

def splitEnds(text):
    #text = re.sub(r'([^\s]*)([!?\.\-\,\';:])(\s)', r'\1 \2 \3', text)
    # for ... case
    text = re.sub(r'([^\s!?\.\-\,\';:]*)([!?\.\-\,\';:]*)(\s)', r'\1 \2 ', text)
    # for end of line case
    text = re.sub(r'([^\s!?\.\-\,\';:]*)([!?\.\-\,\';:]*)$', r'\1 \2', text)
    return text

def sanitize(text):
    """Do parse the text in variable "text" according to the spec, and return
    a LIST containing FOUR strings 
    1. The parsed text.
    2. The unigrams
    3. The bigrams
    4. The trigrams
    """

    # YOUR CODE GOES BELOW:
    #8
    text = text.lower()
    #2
    text = replaceLinks(text)
    text = re.sub(r'http[^\s]*', '', text)
    #7 Remove all 'special' characters
    text = re.sub(r'[^\sA-Za-z0-9!?\.\-,\';:]', '', text)
    #6
    text = splitEnds(text)

    #1
    text = text.replace("\r", " ")
    text = text.replace("\n", " ")
    text = text.replace("\t", " ")
    text = re.sub(' +', ' ', text)
    #5
    tokens = text.split()


    #10
    parsed_text = text

    word_tokens = []
    for token in tokens:
        isPunctiation = True;
        pattern = re.compile(r'[^!?\.\-,\';:]')
        for char in token:
            if pattern.match(char):
                isPunctiation = False
        if isPunctiation == False:
            word_tokens.append(token)


    unigrams = ''
    for word in word_tokens:
        unigrams += word
        unigrams += ' '
    unigrams = unigrams[:-1]

    bigrams = ''
    for i in range(0, len(word_tokens)-1):
        bigrams += word_tokens[i] + '_' + word_tokens[i+1] + ' '
    bigrams = bigrams[:-1]

    trigrams = ''
    for i in range(0, len(word_tokens)-2):
        trigrams += word_tokens[i] + '_' + word_tokens[i+1] + '_' + word_tokens[i+2] + ' '
    trigrams = trigrams[:-1]

    #return parsed_text, unigrams, bigrams, trigrams
    
    unigrams_array = unigrams.split()
    bigrams_array = bigrams.split()
    trigrams_array = trigrams.split()
    unigrams_array.extend(bigrams_array)
    unigrams_array.extend(trigrams_array)
    return unigrams_array


if __name__ == "__main__":
    # This is the Python main function.
    # You should be able to run
    # python cleantext.py <filename>
    # and this "main" function will open the file,
    # read it line by line, extract the proper value from the JSON,
    # pass to "sanitize" and print the result as a list.

    # YOUR CODE GOES BELOW.
    if len(sys.argv) == 2:
        data = []

        with open(sys.argv[1]) as f:
            for line in f:
                data.append(json.loads(line))

        output = []
        i = 0
        for line in data:
            i += 1
            if i  < 100:
                print(line['author'])
                print(line['body'])
                line_output = sanitize(line['body'])
                output.append(line_ouput)
                print()
                print(line_output)
                print()

                
    else:
        '''
        string = "all you http://google.com base are belong, you todo-list know that right? Anyway theity.three is one."
        string = re.sub(r'http[^\s]*', '', string)
        string = re.sub(r'[^\sA-Za-z0-9!?\.\-,\';:]', '', string)
        string = splitEnds(string)
        print(string)
        '''
        test_array = []
        test_array.append("Try thinking like this. If the USA ever elected a mentally ill pathological liar as president, how would he act? Answer: He'd act like Donald Trump is. Lying left and right, lashing out at people, taking no responsibility, rant and rave incoherently at the press... The way he acts sends a clear message...")


        for string in test_array:
            output = sanitize(string)
            print()
            print(output)
            print()
        
    # We are "requiring" your write a main function so you can
    # debug your code. It will not be graded.
