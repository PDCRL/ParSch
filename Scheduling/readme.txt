 Terminal commands for parallel scheduler:
 g++ -std=c++17 parallelScheduler.cpp -o scheduler -pthread
 ./scheduler
 
 Terminal commands for sequential scheduler:
 g++ -std=c++17 sequentialScheduler.cpp  -o scheduler -pthread
./scheduler

config.txt and source files must be in the same folder
config.txt contains numThreads, numJobs, d(number of resources), k(best K servers), maxRetries










