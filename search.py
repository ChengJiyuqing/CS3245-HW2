#!/usr/bin/python3
import re
import nltk
import sys
import getopt
import pickle
from index import add_skip_pointer

def usage():
    print("usage: " + sys.argv[0] + " -d dictionary-file -p postings-file -q file-of-queries -o output-file-of-results")

def run_search(dict_file, postings_file, queries_file, results_file):
    """
    using the given dictionary file and postings file,
    perform searching on the given queries file and output the results to a file
    """
    print('running search on the queries...')
    
    # Obtain input and output files, parse the queries into list
    in_file = open(queries_file, 'r', encoding="utf8")
    out_file = open(results_file, 'w', encoding="utf8")
    query_list = in_file.read().splitlines()
    
    while query_list:
        query = query_list[0]
        out_file.write()


def parse_query(query):
    """
    Parse the query using shunting-yard algorithm and output the query as a list of operators and operands in Reverse Polish Notation (RPN)
    """
    stemmer = nltk.stem.PorterStemmer()
    operators = {'AND', 'OR', 'NOT'}
    brackets = {'(', ')'}
    query = deque(query.split()) # put current query into a queue
    
    


def search(query, dictionary, posting):



def find_intersection(list_A, list_B):
    """
    Find the intersection between 2 posting lists with skip pointers
    """
    pointer_A = 0
    pointer_B = 0
    skip_distance_A = int(math.floor(math.sqrt(len(list_A))))
    skip_distance_B = int(math.floor(math.sqrt(len(list_B))))
    
    intersection = []
    
    while (pointer_A < len(list_A) and pointer_B < len(list_B))：
        docID_A = getDocID(list_A, pointer_A)
        docID_B = getDocID(list_B, pointer_B)
        
        if docID_A == docID_B:
            intersection.append(docID_A)
            pointer_A += 1
            pointer_B += 1
        elif docID_A < docID_B:
            if hasSkipPointer(list_A, pointer_A) and getSkipPointer(list_A, pointer_A) <= docID_B:
                while hasSkipPointer(list_A, pointer_A) and getSkipPointer(list_A, pointer_A) <= docID_B:
                    pointer_A += skip_distance_A
                continue
            pointer_A += 1
        else:
            if hasSkipPointer(list_B, pointer_B) and getSkipPointer(list_B, pointer_B) <= docID_A:
                while hasSkipPointer(list_B, pointer_B) and getSkipPointer(list_B, pointer_B) <= docID_A:
                    pointer_B += skip_distance_B
                continue
            pointer_B += 1
            
    intersection = add_skip_pointer(intersection) """ havent finished"""
    return intersection


def getDocID(posting, pointer) :
    """
    Get the docID at current pointer position
    """
    if type(posting[pointer]) == int:
        return posting[pointer]
    else: # has a skip pointer
        return posting[pointer][0]
        
def hasSkipPointer(posting, pointer):
    """
    Check if there is a skip pointer at current pointer position
    """
    if type(posting[pointer]) != int:
        return True
    return False
    
    
def getSkipPointer(posting, pointer):
    """
    Find the docID pointed by the skip pointer
    """
    return posting[pointer][1]
    
    
    
def find_union(list_a, list_b):

def find_complement(list_a, list_b):


dictionary_file = postings_file = file_of_queries = output_file_of_results = None

try:
    opts, args = getopt.getopt(sys.argv[1:], 'd:p:q:o:')
except getopt.GetoptError:
    usage()
    sys.exit(2)

for o, a in opts:
    if o == '-d':
        dictionary_file  = a
    elif o == '-p':
        postings_file = a
    elif o == '-q':
        file_of_queries = a
    elif o == '-o':
        file_of_output = a
    else:
        assert False, "unhandled option"

if dictionary_file == None or postings_file == None or file_of_queries == None or file_of_output == None :
    usage()
    sys.exit(2)

run_search(dictionary_file, postings_file, file_of_queries, file_of_output)
