import sys
import os
import math
import time

from pyspark import SparkContext, SparkConf
import mmh3


def proper_round(num):
    '''
    The python function round doesnt work properly, then we decided to define a new function to do the proper round.
    input:
        - num = number 
    output:
        - int_num = number after proper_round
    '''
    num = float(num)
    int_num = int(float(num))
    difference = num - int_num
    if difference >= 0.5:
        return int_num + 1
    else:
        return int_num


##### ------------------------------ COMPUTE PARAMETERS JOB ------------------------------ #####

def get_film_rating(line):
    '''
    This function extract the rating of each film from each line of the tsv file
    input:
        - line = line of the tsv file 
    output:
        - film_ratings = film rating extracted from the line
    '''
    film_ratings = []
    words = line.split("\t")
    film_ratings.append(proper_round(words[1]))
    return film_ratings


def get_parameters(pair):
    '''
    This function computes the parameters of the bloomfilter foreach rating
    input:
        - pair = (rating, number of instances)
    output:
        - m = number of bits of the bloomfilter
        - k = number of hash functions 
    '''
    rating = pair[0]
    n = pair[1]

    m = int(- (n * math.log(broadcast_p.value)) /
            math.pow(math.log(2), 2))  # Compute m
    k = int((m/n) * math.log(2))  # compute k

    return (rating, (m, k))


def compute_parameters(films):
    '''
    This function gets the films in input and returns the parameters foreach bloomfilter.
    input:
        - films
    output:
        - parameters = (r,(m,k)) foreach bloomfilter 
    '''

    # Extract the rating from each film
    ratings = films.flatMap(get_film_rating)

    # Foreach film output the pair (film_rating, 1)
    rating_instances = ratings.map(lambda rating: (rating, 1))

    counts = rating_instances.reduceByKey(
        lambda x, y: x+y)  # Sum the instances foreach rating

    parameters = counts.map(lambda x: get_parameters(x)
                            )  # Compute the parameters

    parameters.saveAsTextFile("Parameters")

    return parameters

##### ------------------------------ CREATE BLOOMFILTERS JOB ------------------------------ #####


def get_film_info(line):
    '''
    This function extract the pair (filmID, filmRating) of each film from each line of the tsv file
    input:
        - line = line of the tsv file 
    output:
        - film_info = (filmID, filmRating)
    '''

    films_info = []

    words = line.split("\t")

    films_info.append((words[0], proper_round(words[1])))

    return films_info


def get_indexes(x):
    '''
    This function computes the indexes (computed by the hash functions) of each film, in the bloomfilter 
    input:
        - x = (filmID, rating)
    output:
        - (rating, indexes) = Calculates the indexes given a film and outputs the pair (rating, indexes)
    '''

    film_id = x[0]
    rating = x[1]

    indexes = []

    m, k = broadcast_parameters.value.get(rating)

    for i in range(k):
        index = mmh3.hash(film_id, i)
        if (index <= 0):
            index = int((-index) % m)
        else:
            index = int(index % m)
        indexes.append(index)

    return (rating, indexes)


def create_bloomfilter(x):
    '''
    This function creates the bloomfilter associated to the rating
    input:
        - x = (rating, indexes)
    output:
        - (rating, bloomfilter)
    '''
    rating = x[0]
    indexes = x[1]

    m, k = broadcast_parameters.value.get(rating)

    bloomfilter = [0] * m

    for index in indexes:
        bloomfilter[index] = 1

    return (rating, bloomfilter)


def concatenate_indexes(x, y):
    x.extend(y)
    return x


def bloom_filter_creation(films):

    films_info = films.flatMap(get_film_info)  # Get film_id and film_rating

    # Get the indexes of the film (computed with the hash functions)
    film_indexes = films_info.map(lambda x: get_indexes(x))

    # Concatenate foreach rating the film's indexes
    indexes = film_indexes.reduceByKey(lambda x, y: concatenate_indexes(x, y))

    # Create the bloomfilter foreach rating from the list of indexes
    bloomfilters = indexes.map(lambda x: create_bloomfilter(x))

    # Sort the bf list by key (rating)
    sorted_bfs = bloomfilters.sortByKey(lambda x: x[0])

    sorted_bfs.saveAsTextFile("BloomFilters")

    return sorted_bfs

##### ------------------------------ TEST BLOOMFILTERS JOB ------------------------------ #####


def check_presence(x):
    '''
    This function checks the presence of the film in each bloomfilter different from his rating 
    input:
        - x = (filmID, rating)
    output:
        - list_of_checks = List of boolean values foreach film that represent the presence of the film in the specific bloomfilter
    '''

    film_id = x[0]
    rating = x[1]

    list_of_checks = []

    for i in range(10):
        # If the rating is different from the film's rating
        if ((i+1) != rating):
            # Get the bf and his parameters
            m, k = broadcast_parameters.value.get(i+1)
            bf = broadcast_bfs.value[i]

            # Get the indexes of the film computing the hash functions
            counter = 0
            for j in range(k):
                index = mmh3.hash(film_id, j)
                if (index <= 0):
                    index = int((-index) % m)
                else:
                    index = int(index % m)

                if bf[index] == 1:
                    counter += 1

            # If all the indexes given by the hash function are set equal to 1 in the bf, the film is "present" in the bf
            if counter == k:
                list_of_checks.append((i+1, 1))
            else:
                list_of_checks.append((i+1, 0))

    return list_of_checks


def compute_fp_rate(x):
    '''
    This function computes the false positive rate
    input:
        - x = (rating, film_presence_list)
    output:
        - p = false positive rate
    '''
    rating = x[0]
    list_of_bool = x[1]
    counter = 0  # Counts the number of instances that tried their presence in the bf
    counter_fp = 0  # Counts the number of false positive
    for value in list_of_bool:
        if value == 1:
            counter_fp += 1
        counter += 1
    p = counter_fp/counter  # Compute the false positive rate
    return (rating, p)


def test(films):

    films_info = films.flatMap(get_film_info)

    check_film_presence_list = films_info.flatMap(lambda x: check_presence(x))

    rating_presence_values = check_film_presence_list.groupByKey()

    get_p = rating_presence_values.map(lambda x: compute_fp_rate(x))

    get_p.saveAsTextFile("testBloomFilters")


##### ------------------------------ MAIN ------------------------------ #####
master = "yarn"

if __name__ == "__main__":

    global broadcast_p, broadcast_parameters, broadcast_bfs

    if len(sys.argv) != 4:
        print("Usage: driver <file> <false_positive rate> <partitions>",
              file=sys.stderr)
        sys.exit(-1)

    os.environ['PYSPARK_PYTHON'] = './environment/bin/python3'

    config = SparkConf()
    config.set('spark.archives', 'pyspark_venv.tar.gz#enviroment')

    sc = SparkContext(master, "CreateBloomFilter", conf=config)

    partitions = int(sys.argv[3])

    start_time = time.time()

    films = sc.textFile(sys.argv[1], minPartitions=partitions)  # Load tsv file
    p = float(sys.argv[2])  # Get p parameter

    broadcast_p = sc.broadcast(p)

    parameters = compute_parameters(films)  # Compute Parameters

    broadcast_parameters = sc.broadcast(parameters.collectAsMap())

    bfs = bloom_filter_creation(films)  # Compute BloomFilters

    bfs = [e[1] for e in bfs.collect()]

    broadcast_bfs = sc.broadcast(bfs)

    test(films)

    print("--- %s ms ---" % ((time.time() - start_time)*1000))
