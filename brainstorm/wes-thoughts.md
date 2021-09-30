# Todo
- come up with data (maybe student, classes, grades etc?) 
- come up with schema for several tables 
- we could save data in csv files 
  - 1 block could be 1 file  
  - each table could have own directory

- write script to generate data (csv files)
  - we could have two versions of data (1 small for development and 1 large for experiments)

- write functions to load csv files into memory
  - we could maybe use pandas library to hold data in memory
- write functions to simulate queries


another thing to keep in mind is keeping the code general so we can run different experiments.
we could at least try different block sizes

#Rishi

- could modify seaborn datasets to use for this or for the larger experiment
- could predefine block size as N number of relations in the df,make a function that breaks the large df to block size and writes csv's to disk, so that we can change block size anytime for different types of tests
- can use memory_profiler to get memory utilized by each join function (Also use the memory used by the standard pandas as a benchmark),
  use import big_o to disply time complexity \\these external libraries might be allowed since theres no other way to measure these two
- use the formulas for each join to calculate cost as well in case the above two are not allowed
- Tool to write blocks to memory would basically just have to break dataframe into block size and write seperate csv files for each block


- Since the last point says we might have to accept different queries as well, it might also me a good idea to use sqlite3 in place of csv files. each block being a separate table in the db and merged into a large table on being read. We can then accept different join/search sql queries and run them on this db. New datasets can just be written as new tables in the db.