wes october 6 updates:

indexed loop join is implemented. I haven't implemented the where clause yet

to generalize the other joins you could use the same parameters I used when initializing ```IndexedJoin```.



after the prompt parses a query, it calls ```execute_query``` in file ```src/performance/execution_plan.py```. This function is not implemented yet. This function needs to
- either call a join function or just call a scan to return data if there is no join in the query.
- scan again for the where clause to remove data that is not true. This could also be added directly in the joins


setting a block memory limit seems like it might be challenging. I'm thinking maybe we just do some experiments on the different joins and start the paper, and then go back add mem limit if we have time.
