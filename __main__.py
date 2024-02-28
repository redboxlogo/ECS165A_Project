from lstore.db import Database
from lstore.query import Query
from time import time
from random import choice, randrange, seed
from lstore.config import *

# Student Id and 4 grades
db = Database()                                                             # create database object from db.py
grades_table = db.create_table('Grades', 5, 0)                              # call create_table() to create table with name = Grades, columns = 5, index = 0 from db.py
query = Query(grades_table)                                                 # call Query() to create Query object rom query.py 
keys = []                                                                   # create local "keys" variable
seed(42069)
# print(process_time())



# # Example usage:
# number = 123456789
# nibble_array = parse_integer_to_nibbles(number)
# print(nibble_array)


################################################################################################################################

#using Query insert entry into "Grades" table

insertFlag = True

if(insertFlag == True):
    insert_time_0 = time()                                              #get time
    print(insert_time_0)
    for i in range(0, 10000):                                                   #for loop for query inserts and key appends
        query.insert(906659671 + i, 93, 0, 0, 0)                                #call insert(self, *columns) from query
        keys.append(906659671 + i)                                              #call append() from key
    insert_time_1 = time()                                              #get time
    print(insert_time_1)
    # print(insert_time_1 - insert_time_0)
    print("Inserting 1M records took:  \t\t\t", insert_time_1 - insert_time_0) #print

print(query.table.index.lookup(906659673))

################################################################################################################################

updateFlag = False

if(updateFlag == True):
    # Measuring update Performance
    update_cols = [                                                             #create "update_cols" matrix
    [None, None, None, None, None],
    [None, randrange(0, 100), None, None, None],
    [None, None, randrange(0, 100), None, None],
    [None, None, None, randrange(0, 100), None],
    [None, None, None, None, randrange(0, 100)],
    ]

    update_time_0 = time()                                              #get time 
    print(update_time_0)

    for i in range(0, 1000000):                                                   #update 10k queries
        query.update(906659671+i, *(choice(update_cols)))                      #call update() from query
    update_time_1 = time()                                              #get time
    print(update_time_1)
    # print(update_time_1 - update_time_0)
    print("Updating 1M records took:  \t\t\t", update_time_1 - update_time_0)  #print




################################################################################################################################

selectFlag = False

if(selectFlag == True):
    # Measuring Select Performance
    select_time_0 = time()                                              #get time
    print(select_time_0)
    for i in range(0, 1000000):                                                   #select 10k records
        query.select(choice(keys),0 , [1, 1, 1, 1, 1])                          #call select() from query
    select_time_1 = time()                                              #get time
    print(select_time_1)
    # print(select_time_1 - select_time_0)
    print("Selecting 1M records took:  \t\t\t", select_time_1 - select_time_0) #print

################################################################################################################################

sumFlag = False

if(sumFlag == True):
    # Measuring Aggregate Performance
    agg_time_0 = time()                                                 #get time
    print(agg_time_0)
    for i in range(0, 1000000, 100):                                              #for loop
        start_value = 906659671 + i                                             #create start value
        end_value = start_value + 100                                           #create end value
        result = query.sum(start_value, end_value - 1, randrange(0, 5))         #call sum() from query for columns 0-4
        # print(result)
    agg_time_1 = time()                                                 #get time
    print(agg_time_1)
    print("Aggregate 1M of 100 record batch took:\t", agg_time_1 - agg_time_0) #print

################################################################################################################################

deleteFlag = False

if(deleteFlag == True):
    # Measuring Delete Performance
    delete_time_0 = time()                                              #get time
    print(delete_time_0)
    for i in range(0, 1000000):                                                   #for loop for delete
        query.delete(906659671 + i)                                             #call delete from query
    delete_time_1 = time()                                              #get time
    print(delete_time_1)
    # print(delete_time_1 - delete_time_0)
    print("Deleting 1M records took:  \t\t\t", delete_time_1 - delete_time_0)  #print