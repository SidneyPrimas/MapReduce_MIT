# MapReduce Exploration
For this project, I completed the implementation of a Go-based MapReduce library. Then, I used the MapReduce library to implement two simple MapReduce programs. 

I completed this lab as part of MIT’s 2016 Distributed System course ([6.824](http://nil.csail.mit.edu/6.824/2016/index.html)). MapReduce was the starter project for the course. Almost the entire class was focused on the 2nd project, building a [fault-tolerant, sharded key-value store](https://github.com/SidneyPrimas/Distributed_Systems_MIT). Check it out!

For more information, see the [original MapReduce paper](http://nil.csail.mit.edu/6.824/2016/labs/lab-1.html) or the [MapReduce wiki entry](https://en.wikipedia.org/wiki/MapReduce). 

Check out the lab instructions [here](http://nil.csail.mit.edu/6.824/2016/labs/lab-1.html).

### MapReduce Library
We coded portions of the MapReduce reduce framework. Specifically, we built the following parts: 
* **The data processing routines run by each worker to complete the map and reduce phase:** More specifically, we implemented the process by which workers organize outputs (after mapping) into the file system, and the process by which workers aggregate keys from different mapping workers. 
* **The master's scheduler that splits the processing across multiple parallel nodes:** The scheduler keeps track of available workers and distributes tasks to each worker. 
**The infrustructure within the master to handle node failures and unreliable networks** 

### MapReduce Programs
I built two MapReduce programs:
* **Word Count:** A simple program reports the occcurence of each word across many books. To accomplish this, we define the map() function to split input contents into words. And, we define the reduce() function to count the occurence for each key (or word). 
* **Inverted Index:** A program that reports in which books each word can be found. 

## MapReduce Overview
MapReduce is a data processing tool that enables parallel, distributed processing of large datasets with built-in fault-tolerance. A program run ontop of the MapReduce infrastructure includes a map function (that filters and sorts datasets into ouput key-value pairs) and a reduce function (that summarizes the results for each key). A master system orchestrates the parallel computing, ensuring accurate results despite unreliable networks and node failures. 

To be more specific, MapReduce usually includes three steps: 
1. Map: Worker nodes apply the map function to the input that that they have been assigned. Each worker persists their key/value outputs. 
2. Shuffle: The master node orchestrates the process of assiging each key to a specific worker node, and distributes the data accordingly.
3. Reduce: The worker nodes apply the reduce function to each of their assigned key, and output the results. 


## Setup and Testing 
First, install [Go](https://golang.org/) (I used v1.5). Then, get setup with the below commands. Finally, run test-mr.sh to run all the tests and obtain a test summary.
```
$ git clone https://github.com/SidneyPrimas/MapReduce_MIT.git
$ cd MapReduce_MIT
$ export GOPATH=$(pwd)
$ cd src/main 
$ ./test-mr.sh
```