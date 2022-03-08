#!/usr/bin/python3
from logging.config import dictConfig
import re
import nltk
import sys
import getopt
import glob
import os
import time
import pickle
import math
from nltk.stem.porter import PorterStemmer


def usage():
    print("usage: " + sys.argv[0] + " -i directory-of-documents -d dictionary-file -p postings-file")

# define input and output location
# directory_path = "/Users/yunjie/nltk_data/corpora/reuters/training/"
# dictionary_file = "dictionary-file"
# posting_file = "postings-file"
# start_time = time.time()
file_count = 0

blocksizes = []

# constants
BLOCK_SIZE = 50  # artificial threshold to simulate memory limit in terms of number of term-docID pairs
# list of file indices for merge later
lst = []

all_docIDs = []
all_docID_file = "all_docID.txt"

def build_index(in_dir, out_dict, out_postings):
    """
    build index from documents stored in the input directory,
    then output the dictionary file and postings file
    """
    print('indexing...')

    
    
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
        
        if not filename in all_docIDs:
            all_docIDs.append(int(filename))
        full_filename = os.path.join(in_dir, filename)
        text = open(full_filename, 'r', encoding="utf8").read()
                
        """ Process the input text """
        # Apply case folding, converting text to lower case
        text = text.lower()
                
        # Remove punctuations
        text = re.sub(r'[^a-z0-9A-Z_]',' ', text)

        # Apply tokenization and stemming & store blocks to disk
        for sentence in nltk.sent_tokenize(text):
            for word in nltk.word_tokenize(sentence):
                stemmed_word = PorterStemmer().stem(word)
                
                # Update dictionary with current (term,docID) pair
                # Create a new posting list if the term is not inside the dictionary
                if stemmed_word not in temp_dictionary :
                    temp_dictionary[stemmed_word] = []
                
                # Only add docID to posting list if it's not added previously
                if int(filename) not in temp_dictionary[stemmed_word]:
                    pair_counter += 1
                    temp_dictionary[stemmed_word].append(int(filename))
                        
                    # Check for memory limit, if exceeded, write everything from memory to disk
                    if pair_counter >= BLOCK_SIZE:
                        # Sort the dictionary by key
                        # each postinglist is already sorted as filenames are sorted in line 50
                        output_dictionary = dict(sorted(temp_dictionary.items()))
                        # Write out the index to disk
                        write_block_to_disk(output_dictionary,block_counter)
                        # Reset variables
                        temp_dictionary = {}
                        block_counter += 1
                        pair_counter = 0
        if pair_counter < BLOCK_SIZE: # the last block
                        output_dictionary = dict(sorted(temp_dictionary.items()))
                        write_block_to_disk(output_dictionary,block_counter)
                        temp_dictionary = {}
                        block_counter += 1
                        pair_counter = 0
                        file_count = block_counter
    
    # Merge blocks in disk   
    for i in range(file_count): # construct a list with all file indices (for retrieval of dictionary blocks later)
        lst.insert(i, i)
    final_index = binary_merge(lst)
    posting_file_name = "postings{}.txt".format(final_index)
    dictionary_file_name = "dictionary{}.txt".format(final_index)
    postings_file = open(posting_file_name, 'rb')
    dictionary_file = open(dictionary_file_name, 'rb') 

    # os.rename(posting_file_name, out_postings)
    # os.rename(dictionary_file_name, out_dict)
    
    dictionary = pickle.load(dictionary_file)

    f_dict = open(out_dict, "wb")
    f_post = open(out_postings, 'wb')
    # # print(all_docIDs)

    # i = 0
    for item in dictionary:
        print(item)
        print(dictionary[item])
        pointer = dictionary[item][0]
        postings_file.seek(pointer)
        postings = pickle.load(postings_file)
        postings = add_skip_pointer(postings)

        pointer = f_post.tell()
        dictionary[item] = (pointer, dictionary[item][1])
        pickle.dump(postings, f_post)
        # print("..", postings)
        # i += 1
    
    
    pointer = f_post.tell()
    pickle.dump(all_docIDs, f_post)
    dictionary['docIDs'] = pointer

    pickle.dump(dictionary, f_dict)
    
    f_dict.close()
    f_post.close()
    # test(lst)
           
    
def write_block_to_disk(dictionary,block_counter):
    posting_file_name = "postings{}.txt".format(block_counter)
    dictionary_file_name = "dictionary{}.txt".format(block_counter)
    postings_file = open(posting_file_name, 'wb')
    dictionary_file = open(dictionary_file_name, 'wb')
        
    # Iterate through
    for term in dictionary:
        # get current disk location of the postings file
        pointer = postings_file.tell()
        postingslist = pickle.dumps(dictionary[term])
        #postings_size = sys.getsizeof(postings_byte) # get size of posting in bytes
        
        

        # write current line of term into dictionary file, in the form of {term: pointer (to postingslist), docFrequency}
        #print(dictionary[term])
        dictionary[term] = pointer, len(dictionary[term])
        postings_file.write(postingslist)
    
    #pickle.dump(len(dictionary), dictionary_file)
    # count = 0
    # for item in dictionary.items():
    #     pickle.dump(item, dictionary_file)
    #     count += 1

    pickle.dump(dictionary, dictionary_file)
    # blocksizes.insert(block_counter, count)
    del dictionary

    
    postings_file.close()
    dictionary_file.close()

    return dictionary_file_name, posting_file_name

def binary_merge(lst):
    #we can use pickle.load() just that we can't load the entire pickle file directly, have to combine it with python IO functions to get specific records.
    #linecache to retrieve a specific line
    #Intuitively u should thus find a way to store these pointers to each posting list in ur index.py and transfer them over to search.py using pickle
    if len(lst) > 2: 
        middle = math.floor(len(lst) / 2)
        first_half = lst[: middle]
        second_half = lst[middle :]
        return binary_merge([binary_merge(first_half), binary_merge(second_half)])
    else:
        if len(lst) == 1:
            return lst[0]    
        else: # length is 2
            return merge_two_blocks(lst[0], lst[1])



def merge_two_blocks(index1, index2):
    print(index1, index2)
    save_index = len(lst)


    posting_file_name_1 = "postings{}.txt".format(index1)
    dictionary_file_name_1 = "dictionary{}.txt".format(index1)
    posting_file_name_2 = "postings{}.txt".format(index2)
    dictionary_file_name_2 = "dictionary{}.txt".format(index2)
    postings_file_1 = open(posting_file_name_1, 'rb')
    dictionary_file_1 = open(dictionary_file_name_1, 'rb')
    postings_file_2 = open(posting_file_name_2, 'rb')
    dictionary_file_2 = open(dictionary_file_name_2, 'rb')
    savepostings_file = open("postings{}.txt".format(save_index), 'wb')

    dict1 = pickle.load(dictionary_file_1)
    dict2 = pickle.load(dictionary_file_2)

    pointer1 = 0
    pointer2 = 0

    terms1 = list(dict1.items())
    terms2 = list(dict2.items())

    newDict = {}

    pointer = savepostings_file.tell()
    while (pointer1 < len(terms1) and pointer2 < len(terms2)):
        term1 = terms1[pointer1]
        term2 = terms2[pointer2]
        if term1[0] < term2[0]:
            postings_file_1.seek(term1[1][0])
            postingslist1 = pickle.load(postings_file_1)
            # postingslist1 = add_skip_pointer(postingslist1)
            

            
            

            term1 = (term1[0], (pointer, term1[1][1])) # update pointer to postings
            newDict[term1[0]] = term1[1]

            pickle.dump(postingslist1, savepostings_file)
            
            # savepostings_file.close()
            pointer1 += 1
        elif term1[0] > term2[0]:
            postings_file_2.seek(term2[1][0])
            postingslist = pickle.load(postings_file_2)
            # postingslist = add_skip_pointer(postingslist)
            

            # savepostings_file = open("postings{}.txt".format(save_index), 'wb')
            # pointer = savepostings_file.tell()

            term2 = (term2[0], (pointer, term2[1][1])) # update pointer to postings
            newDict[term2[0]] = term2[1]

            pickle.dump(postingslist, savepostings_file)
            
            # savepostings_file.close()
            pointer2 += 1
        else:
            newpostings = []

            postings_file_1.seek(term1[1][0])
            postingslist1 = pickle.load(postings_file_1)
            postings_file_2.seek(term2[1][0])
            postingslist2 = pickle.load(postings_file_2)

            for i in postingslist1:
                if not (i in postingslist2):
                    postingslist2.append(i)
            postingslist2.sort()
            
            newpostings = postingslist2

            # savepostings_file = open("postings{}.txt".format(save_index), 'wb')
            # pointer = savepostings_file.tell()

            term2 = (term2[0], (pointer, term2[1][1])) # update pointer to postings
            newDict[term2[0]] = term2[1]

            pickle.dump(newpostings, savepostings_file)
            
            # savepostings_file.close()
            pointer1 += 1
            pointer2 += 1

    while pointer1 < len(terms1):
        for term1 in terms1:
            postings_file_1.seek(term1[1][0])
            postingslist1 = pickle.load(postings_file_1)
            # postingslist1 = add_skip_pointer(postingslist1)
            

            # savepostings_file = open("postings{}.txt".format(save_index), 'wb')
            pointer = savepostings_file.tell()

            term1 = (term1[0], (pointer, term1[1][1])) # update pointer to postings
            newDict[term1[0]] = term1[1]

            pickle.dump(postingslist1, savepostings_file)
            
            # savepostings_file.close()
            pointer1 += 1
    while pointer2 < len(terms2):
        for term2 in terms2:
            postings_file_2.seek(term2[1][0])
            postingslist2 = pickle.load(postings_file_2)
            # postingslist2 = add_skip_pointer(postingslist2)
            

            # savepostings_file = open("postings{}.txt".format(save_index), 'wb')
            pointer = savepostings_file.tell()

            term2 = (term2[0], (pointer, term2[1][1])) # update pointer to postings
            newDict[term2[0]] = term2[1]

            pickle.dump(postingslist2, savepostings_file)
            
            # savepostings_file.close()
            pointer2 += 1
    
    postings_file_1.close()
    dictionary_file_1.close()
    postings_file_2.close()
    dictionary_file_2.close()
    savepostings_file.close()

    savedict_file = open("dictionary{}.txt".format(save_index), 'wb')
    pickle.dump(newDict, savedict_file)
    savedict_file.close()
    lst.insert(len(lst), len(lst))
    return save_index


def test(lst):
    for x in lst:
        posting_file_name_1 = "postings{}.txt".format(x)
        dictionary_file_name_1 = "dictionary{}.txt".format(x)
        
        postings_file_1 = open(posting_file_name_1, 'rb')
        dictionary_file_1 = open(dictionary_file_name_1, 'rb')
        
        c = 0
        while c < blocksizes[x]: 
            print(x, c, blocksizes[x], pickle.load(dictionary_file_1))
            c += 1

def add_skip_pointer(list):

    #Add skip pointers to a posting list
    
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

dictionary_file = open(output_file_dictionary, 'rb')
posting = open(output_file_postings, 'rb')
dictionary = pickle.load(dictionary_file)
pointer = dictionary['you'][0]
print(dictionary)
print(len(dictionary))
posting.seek(pointer)
print(pickle.load(posting))

# build_index("../training_test/", "dictionary.txt", "postings.txt")
# a = {"a": [1, 2], "b": [2], "c": [3]}
# b = {"a": [3], "b": [4], "c": [2]}
# write_block_to_disk(a, 0)
# write_block_to_disk(b, 1)
# binary_merge([0, 1])



