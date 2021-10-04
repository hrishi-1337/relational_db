# Todo ideas
thoughts on things to do to finish project

### 1. query prompt
make a script that runs a query prompt that only accepts **select** commands. Will have basic parsing to handle:
- where
- join

### 2. generalize scan and joins
parameters for scan:
- table
- where conditions

parameters for join:
- table 1
- table 2

### 3. scan and joins return cost (wall time and number of block reads)
count number of block reads and start timer. return both.

### 4. implement memory limitation
limit to M number of blocks that can be in memory at once. I think this would make for the most interesting
experiments. We could test cost based on the size of M.

### 4. cost estimation
come up with way to estimate cost of query for each scan or join technique *prior to
running the query*.

### 5. optimization
once a query is run, our model estimates the best query plan (based on 4) and then
executes the query plan by calling the necessary scan and/or join


### 6. experiment
run experiments tracking he following things:
- binary vs csv blocks
- query cost over different memory limitations

### add tests
keep adding tests for all funcitonality
