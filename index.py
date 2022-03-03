#!/usr/bin/python3
import re
import nltk
import sys
import getopt
import glob
import os
import time
import pickle
from nltk.stem.porter import PorterStemmer

def usage():
    print("usage: " + sys.argv[0] + " -i directory-of-documents -d dictionary-file -p postings-file")

# define input and output location
directory_path = "/Users/yunjie/nltk_data/corpora/reuters/training/"
dictionary_file = "dictionary-file"
posting_file = "postings-file"
start_time = time.time()
file_count = 0

def build_index(in_dir, out_dict, out_postings):
    """
    build index from documents stored in the input directory,
    then output the dictionary file and postings file
    """
    print('indexing...')
    
    # constants
    BLOCK_SIZE = 1000  # artificial threshold to simulate memory limit in terms of number of term-docID pairs
    
    # initialize the hash table to store the current dictionary in the form of term-> [id1,id2...]
    temp_dictionary = {}
    
    # intialize counter for (term, DocID) pair
    pair_counter = 0
    
    # initialize counter for number of blocks
    block_counter = 0
    
    
    # get the list of files inside the input directory
    filenames = []
    for filename in os.listdir(in_dir):
        if not filename.endswith('.DS_Store'):
            filenames.append(filename)
    
    # sort filenames before processing data so docIDs in posting are in order
    filenames.sort()
    
    #print(filenames)
    
    #for dirname, dirnames, filenames in os.walk(in_dir):
    for filename in filenames:
        full_filename = os.path.join(in_dir, filename)
        text = open(full_filename, 'r', encoding="utf8").read()
                
        """ Process the input text """
        # Apply case folding, converting text to lower case
        text = text.lower()
                
        # Remove punctuations
        text = re.sub(r'[^a-z0-9A-Z_]',' ', text)

        # Apply tokenization and stemming
        for sentence in nltk.sent_tokenize(text):
            for word in nltk.word_tokenize(sentence):
                stemmed_word = PorterStemmer().stem_word(word)
                
                # Update dictionary with current (term,docID) pair
                # Create a new posting list if the term is not inside the dictionary
                if stemmed_word not in temp_dictionary :
                    temp_dictionary[stemmed_word] = []
                
                # Only add docID to posting list if it's not added previously
                if int(filename) not in temp_dictionary[stemmed_word]:
                    pair_counter += 1
                    temp_dictionary[stemmed_word].add(int(filename))
                        
                    # Check for memory limit, if exceeded, write everything from memory to disk
                    if pair_counter > BLOCK_SIZE:
                        # Sort the dictionary by key
                        output_dictionary = dict(sorted(temp_dictionary.items()))
                        # Write out the index to disk
                        write_block_to_disk(output_dictionary,block_counter)
                        # Reset variables
                        temp_dictionary = {}
                        block_counter += 1
                        pair_counter = 0

            
    
def write_block_to_disk(dictionary,block_counter):
    posting_file_name = "postings{}.txt".format(block_counter)
    dictionary_file_name = "dictionary{}.txt".format(block_counter)
    postings_file = open(posting_file_name, 'wb')
    dictionary_file = open(dictionary_file_name, 'wb')
        
    # Iterate through
    for term in dictionary:
        # get current disk location of the postings file
        offset = postings_file.tell()
        postings_byte = pickle.dumps(dictionary[term])
        postings_size = sys.getsizeof(postings_byte) # get size of posting in bytes
        
        # write current line of term into dictionary file, in the form of (term, posting offset, posting size, length of posting)
        pickle.dump((term, offset, postings_size, len(dictionary[term])), dictionary_file)
        postings_file.write(postings_byte)
    
    del dictionary
    postings_file.close()
    dictionary_file.close()
    return dictionary_file_name, posting_file_name


def merge_by_chunk(block1, block2):
"""we can use pickle.load() just that we can't load the entire pickle file directly, have to combine it with python IO functions to get specific records.
linecache to retrieve a specific line
Intuitively u should thus find a way to store these pointers to each posting list in ur index.py and transfer them over to search.py using pickle
"""


def add_skip_pointer(list):
    """
    Add skip pointers to a posting list
    """
    # Set threshold, if length is below threshold, don't add skip pointers
    if len(list) <= 10:
        return list
    
    else:
        # start from 0
        index = 0
        # calculate optimal skip distance
        skip_distance = int(math.floor(math.sqrt(len(list))))
        
        # store the value of the successor pointed by skip pointer
        while index + skip_distance < len(list):
            list[index] = (list[index], list[index + skip_distance]) # e.g. (2,9) indicates that the value of current docID is 2, and there is a skip pointer pointing to a docID of 9 in the posting list
            index += skip_distance

    return list

    
input_directory = output_file_dictionary = output_file_postings = None

try:
    opts, args = getopt.getopt(sys.argv[1:], 'i:d:p:')
except getopt.GetoptError:
    usage()
    sys.exit(2)

for o, a in opts:
    if o == '-i': # input directory
        input_directory = a
    elif o == '-d': # dictionary file
        output_file_dictionary = a
    elif o == '-p': # postings file
        output_file_postings = a
    else:
        assert False, "unhandled option"

if input_directory == None or output_file_postings == None or output_file_dictionary == None:
    usage()
    sys.exit(2)

build_index(input_directory, output_file_dictionary, output_file_postings)
