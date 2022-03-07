#!/usr/bin/python3
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

    postings = pickle.load(postings_file)
    dictionary = pickle.load(dictionary_file)

    f_dict = open(out_dict, "wb")
    f_post = open(out_postings, 'wb')

    pickle.dump(all_docIDs, f_post)
    pickle.dump(postings, f_post)
    pickle.dump(dictionary, f_dict)

    print(final_index)
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
        
        for i in postingslist:
            if not i in all_docIDs:
                all_docIDs.append(i)

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

def merge_two_blocks_1(index1, index2, total_files):
    print("index1: ", index1, "index2: ", index2)
    num_of_repeated_items = 0
    num = 0

    save_index = len(blocksizes)
    # if index1 < index2: 
    #     save_index = index1 + file_count
    # else: 
    #     save_index = index2 + file_count
    
    posting_file_name_1 = "postings{}.txt".format(index1)
    dictionary_file_name_1 = "dictionary{}.txt".format(index1)
    posting_file_name_2 = "postings{}.txt".format(index2)
    dictionary_file_name_2 = "dictionary{}.txt".format(index2)
    postings_file_1 = open(posting_file_name_1, 'rb')
    dictionary_file_1 = open(dictionary_file_name_1, 'rb')
    postings_file_2 = open(posting_file_name_2, 'rb')
    dictionary_file_2 = open(dictionary_file_name_2, 'rb')
    
    pointer1 = 0
    pointer2 = 0
    length1 = blocksizes[index1]
    length2 = blocksizes[index2]

    print("length1: ", length1)
    print("lengh2: ", length2)
    
    POINTER1 = 0
    POINTER2 = 1

    last_moved = -1

    # print(pickle.load(dictionary_file_1))
    # print(pickle.load(dictionary_file_1))
    # print(pickle.load(dictionary_file_1))

    while pointer1 < length1 and pointer2 < length2:
        print("pointers: ", pointer1, pointer2)

        if last_moved == POINTER2: 
            if pointer2 + 1 < length2:
                item2 = pickle.load(dictionary_file_2)
        elif last_moved == POINTER1:
            if pointer1 + 1 < length1:
                item1 = pickle.load(dictionary_file_1)
        else:
            if pointer1 + 1 < length1:
                item1 = pickle.load(dictionary_file_1)
            if pointer2 + 1 < length2:
                item2 = pickle.load(dictionary_file_2)

        print("item1: ", item1)
        print("item2: ", item2)
        
        if item1[0] < item2[0] :
            postings_file_1.seek(item1[1][0])
            postingslist1 = pickle.load(postings_file_1)
            print(postingslist1)
            

            savepostings_file = open("postings{}.txt".format(save_index), 'wb')
            savedict_file = open("dictionary{}.txt".format(save_index), 'wb')
            pointer = savepostings_file.tell()
            item1 = (item1[0], (pointer, item1[1][1])) # update pointer to postings
            pickle.dump(item1, savedict_file)
            pickle.dump(postingslist1, savepostings_file)
            num += 1
            savepostings_file.close()
            savedict_file.close()

            pointer1 = pointer1 + 1
            last_moved = POINTER1
        
        elif item1[0] > item2[0]:
            postings_file_2.seek(item2[1][0])
            postingslist2 = pickle.load(postings_file_2)
            print(postingslist2)
            
            
            savepostings_file = open("postings{}.txt".format(save_index), 'wb')
            savedict_file = open("dictionary{}.txt".format(save_index), 'wb')
            pointer = savepostings_file.tell()
            item2 = (item2[0], (pointer, item2[1][1])) # update pointer to postings
            pickle.dump(item2, savedict_file)
            pickle.dump(postingslist2, savepostings_file)
            num += 1
            savepostings_file.close()
            savedict_file.close()

            pointer2 = pointer2 + 1
            last_moved = POINTER2
        
        else:
            num_of_repeated_items += 1

            postings_file_1.seek(item1[1][0])
            postingslist1 = pickle.load(postings_file_1)
            postings_file_2.seek(item2[1][0])
            postingslist2 = pickle.load(postings_file_2)
            for x in postingslist1:
                if not (x in postingslist2) :
                    postingslist2.append(x)
            postingslist2.sort()
            print(postingslist2)

            
            
            savepostings_file = open("postings{}.txt".format(save_index), 'wb')
            savedict_file = open("dictionary{}.txt".format(save_index), 'wb')
            pointer = savepostings_file.tell()
            item1 = (item1[0], (pointer, len(postingslist2)))
            pickle.dump(item1, savedict_file)
            pickle.dump(postingslist2, savepostings_file)
            num += 1
            savepostings_file.close()
            savedict_file.close()

            pointer1 = pointer1 + 1
            pointer2 = pointer2 + 1

            last_moved = -1
    
    if pointer1 < length1:
        while pointer1 < length1:
            if pointer1 < length1:
                item1 = pickle.load(dictionary_file_1)

            postings_file_1.seek(item1[1][0])
            postingslist1 = pickle.load(postings_file_1)
            

            savepostings_file = open("postings{}.txt".format(save_index), 'wb')
            savedict_file = open("dictionary{}.txt".format(save_index), 'wb')
            pointer = savepostings_file.tell()
            item1 = (item1[0], (pointer, item1[1][1])) # update pointer to postings
            pickle.dump(item1, savedict_file)
            pickle.dump(postingslist1, savepostings_file)
            savepostings_file.close()
            savedict_file.close()

            pointer1 = pointer1 + 1
            # 
            #     

    elif pointer2 < length2:
        while pointer2 < length2:
            if pointer2 < length2:
                item2 = pickle.load(dictionary_file_2)

            postings_file_2.seek(item2[1][0])
            postingslist2 = pickle.load(postings_file_2)
            
            savepostings_file = open("postings{}.txt".format(save_index), 'wb')
            savedict_file = open("dictionary{}.txt".format(save_index), 'wb')
            pointer = savepostings_file.tell()
            item2 = (item2[0], (pointer, item2[1][1])) # update pointer to postings
            pickle.dump(item2, savedict_file)
            pickle.dump(postingslist2, savepostings_file)
            savepostings_file.close()
            savedict_file.close()

            pointer2 = pointer2 + 1
            # if pointer2 < length2:
            #     

    blocksizes.append(num)
    postings_file_1.close()
    dictionary_file_1.close()
    postings_file_2.close()
    dictionary_file_2.close()


    return len(blocksizes) - 1


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

    while (pointer1 < len(terms1) and pointer2 < len(terms2)):
        term1 = terms1[pointer1]
        term2 = terms2[pointer2]
        if term1[0] < term2[0]:
            postings_file_1.seek(term1[1][0])
            postingslist1 = pickle.load(postings_file_1)
            postingslist1 = add_skip_pointer(postingslist1)
            

            
            pointer = savepostings_file.tell()

            term1 = (term1[0], (pointer, term1[1][1])) # update pointer to postings
            newDict[term1[0]] = term1[1]

            pickle.dump(postingslist1, savepostings_file)
            
            # savepostings_file.close()
            pointer1 += 1
        elif term1[0] > term2[0]:
            postings_file_2.seek(term2[1][0])
            postingslist = pickle.load(postings_file_2)
            postingslist = add_skip_pointer(postingslist)
            

            # savepostings_file = open("postings{}.txt".format(save_index), 'wb')
            pointer = savepostings_file.tell()

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
            
            newpostings = add_skip_pointer(postingslist2)

            # savepostings_file = open("postings{}.txt".format(save_index), 'wb')
            pointer = savepostings_file.tell()

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
            postingslist1 = add_skip_pointer(postingslist1)
            

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
# build_index("../training_test/", "dictionary.txt", "postings.txt")
# a = {"a": [1, 2], "b": [2], "c": [3]}
# b = {"a": [3], "b": [4], "c": [2]}
# write_block_to_disk(a, 0)
# write_block_to_disk(b, 1)
# binary_merge([0, 1])



