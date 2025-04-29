// Refactored Multithreaded Job Scheduling System
#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <math.h>
#include <memory>
#include <mutex>
#include <queue>
#include <random>
#include <shared_mutex>
#include <thread>
#include <vector>

using namespace std;

int64_t currTime() {
    return chrono::duration_cast<chrono::microseconds>(
               chrono::steady_clock::now().time_since_epoch())
        .count();
}

struct Config {
    int numThreads;
    int numJobs;
    int d; // number of resources
    int k;
    int maxRetries;
};

Config loadConfig(const string &filename) {
    Config cfg;
    ifstream infile(filename);
    infile >> cfg.numThreads >> cfg.numJobs >> cfg.d >> cfg.k >> cfg.maxRetries;
    return cfg;
}

class FairLock {
public:
    shared_mutex rwLock;
    void readLock() { rwLock.lock_shared(); }
    void readUnlock() { rwLock.unlock_shared(); }
    void writeLock() { rwLock.lock(); }
    void writeUnlock() { rwLock.unlock(); }
};

class Job {
public:
    int jobId;
    int serverId;
    // int64_t startByTime;
    int64_t duration;
    int64_t endTime;
    int64_t queueEntryTime;
    int64_t schedulingEndTime;

    vector<int> resourceRequired;

    Job(int id, int64_t queueEntryTime, int64_t dur, const vector<int> &res)
        : jobId(id), serverId(-1), queueEntryTime(queueEntryTime), duration(dur), endTime(0), resourceRequired(res) {}

    bool operator<(const Job &other) const {
        return endTime < other.endTime;
    }
};

class Server {
public:
    int serverId;
    int64_t startTime, endTime;
    vector<int> resourceAvailable;
    vector<int> capacity;
    FairLock lock;
    vector<shared_ptr<Job>> assignedJobs;

    Server(int id, const vector<int> &cap) : serverId(id), startTime(INT64_MAX), endTime(0), capacity(cap), resourceAvailable(cap) {}

    bool canHandle(const Job &job) {
        for (size_t i = 0; i < job.resourceRequired.size(); ++i) {
            if (resourceAvailable[i] < job.resourceRequired[i])
                return false;
        }
        return true;
    }

    void assignJob(Job &job, int64_t currentTime) {
        startTime = min(startTime, currentTime);
        job.endTime = currentTime + job.duration;
        endTime = max(endTime, job.endTime);
        job.serverId = serverId;
        for (size_t i = 0; i < job.resourceRequired.size(); ++i) {
            resourceAvailable[i] -= job.resourceRequired[i];
        }
        assignedJobs.push_back(make_shared<Job>(job));
    }

    void releaseJob(const Job &job) {
        for (size_t i = 0; i < job.resourceRequired.size(); ++i) {
            resourceAvailable[i] += job.resourceRequired[i];
        }
    }

    bool isIdle() {
        return equal(resourceAvailable.begin(), resourceAvailable.end(), capacity.begin());
    }
};

class Servers {
public:
    map<int, shared_ptr<Server>> activeServerMap;
    atomic<int> serverCounter{0};
    vector<int> commonCapacity;
    mutable shared_mutex mapMutex;

    Servers(const vector<int> &capacity) : commonCapacity(capacity) {}

    shared_ptr<Server> createServer() {
        int id = serverCounter++;
        auto server = make_shared<Server>(id, commonCapacity);
        activeServerMap[id] = server;
        return server;
    }
};

atomic<int64_t> averageTimeTakenToSchedule(0);
mutex avgLock, createServerMutex;
atomic<int> counter(0);
atomic<int64_t> schedulingTime(0);
atomic<int64_t> cost(0);
atomic<int> serverCount(0);
atomic<int> scheduledJobCounter(0);

void updateAvg(int64_t elapsedTime) {
    lock_guard<mutex> lock(avgLock);
    int c = ++counter;
    averageTimeTakenToSchedule = (averageTimeTakenToSchedule * (c - 1) + elapsedTime) / c;
}

shared_ptr<Server> scheduleJob(shared_ptr<Job> job, Servers &servers, int k, int maxRetries,
                               int retries,
                               priority_queue<pair<int64_t, Job>, vector<pair<int64_t, Job>>, greater<>> &activeJobs,
                               mutex &jobsMutex) {
    int64_t startOfScheduling = currTime();
    // cout << "job" << job->jobId << "->startOfScheduling" << startOfScheduling << endl;
    shared_ptr<Server> serverGreedy = nullptr;

    // cout << " startOfScheduling:" << startOfScheduling << " retries:" << retries << endl;

    // cout << "1:shared_lock mapMutex bef acq" << endl;
    shared_lock<shared_mutex> lock(servers.mapMutex);
    // cout << "1:shared_lock mapMutex acq" << endl;
    if ((retries > maxRetries) || (servers.activeServerMap.empty()) /*|| (!servers.activeServerMap.empty() &&
        job->startByTime - startOfScheduling < averageTimeTakenToSchedule)*/
    ) {

        int check = 0;
        if (servers.activeServerMap.empty()) {
            check = 1;
        }

        lock.unlock();
        shared_ptr<Server> newServer;
        {
            // cout << "2:unique_lock mapMutex bef acq" << endl;
            lock_guard<shared_mutex> lock(servers.mapMutex);
            // cout << "2:unique_lock mapMutex acq" << endl;

            newServer = servers.createServer();
            this_thread::sleep_for(chrono::milliseconds(25));
        }
        // cout << "3.write_lock bef acq" << endl;
        newServer->lock.writeLock();
        // cout << "3.write_lock acq" << endl;
        newServer->assignJob(*job, startOfScheduling);
        this_thread::sleep_for(chrono::milliseconds(15));

        // cout << "3.write_unlock bef acq" << endl;
        newServer->lock.writeUnlock();
        // cout << "3.write_unlock acq" << endl;

        job->schedulingEndTime = currTime();
        schedulingTime += (job->schedulingEndTime - job->queueEntryTime);
        // updateAvg(currTime() - startOfScheduling);
        {
            // cout << "4.unique_lock jobMutex bef acq" << endl;
            lock_guard<mutex> lock(jobsMutex);
            // cout << "4.unique_lock jobMutex acq" << endl;

            activeJobs.push({job->endTime, *job});
        }

        // cout << "job" << job->jobId << ":new server opened at retry:" << retries << " check empty map:" << check << endl;
        serverCount++;
        return newServer;
    } else {
        lock.unlock();
    }
    priority_queue<pair<int64_t, shared_ptr<Server>>> bestK;
    {

        // cout << "servers possible for job:" << job->jobId << " are ";

        // cout << "5.shared_lock mapMutex bef acq" << endl;
        shared_lock<shared_mutex> lock(servers.mapMutex);
        // cout << "5.shared_lock mapMutex acq" << endl;

        for (auto &[id, srv] : servers.activeServerMap) {
            srv->lock.readLock();
            if (srv->canHandle(*job))
                bestK.push({srv->endTime, srv});
            this_thread::sleep_for(chrono::milliseconds(5));
            srv->lock.readUnlock();
            // cout << srv->serverId << " ";
        }
        // cout << endl;
    }

    while (!bestK.empty() && k--) {
        auto [_, srv] = bestK.top();
        bestK.pop();
        // cout << "hi1" << endl;
        // cout << "6.write_lock bef acq" << endl;
        srv->lock.writeLock();
        // cout << "6.write_lock acq" << endl;

        if (srv->canHandle(*job)) {
            // cout << "hi2" << endl;
            srv->assignJob(*job, startOfScheduling);
            this_thread::sleep_for(chrono::milliseconds(15));
            // cout << "7.write_unlock bef acq" << endl;
            srv->lock.writeUnlock();
            // cout << "7.write_unlock acq" << endl;

            job->schedulingEndTime = currTime();
            serverGreedy = srv;
            ////cout << "hi3" << endl;

            break;
        } else {
            // cout << "8.write_unlock bef acq" << endl;
            srv->lock.writeUnlock();
            // cout << "8.write_unlock acq" << endl;
        }
    }

    if (!serverGreedy) {
        return scheduleJob(job, servers, k, maxRetries, retries + 1, activeJobs, jobsMutex);
    }

    schedulingTime += (job->schedulingEndTime - job->queueEntryTime);

    // updateAvg(currTime() - startOfScheduling);
    {
        // cout << "9.unique_lock jobMutex bef acq" << endl;
        lock_guard<mutex> lock(jobsMutex);
        // cout << "9.unique_lock jobMutex acq" << endl;

        activeJobs.push({job->endTime, *job});
    }
    // cout << "9.unique_lock jobMutex released" << endl;

    return serverGreedy;
}

void workerThread(vector<shared_ptr<Job>> jobs, Servers &servers, int k, int maxRetries,
                  priority_queue<pair<int64_t, Job>, vector<pair<int64_t, Job>>, greater<>> &activeJobs,
                  mutex &jobsMutex) {
    for (auto &job : jobs) {
        int64_t currentTime = currTime();
        if (currentTime < job->queueEntryTime) {
            this_thread::sleep_for(chrono::microseconds(job->queueEntryTime - currentTime));
        }
        // Schedule the job
        scheduleJob(job, servers, k, maxRetries, 0, activeJobs, jobsMutex);
        scheduledJobCounter++;
    }
}

void cleanupThread(Servers &servers,
                   priority_queue<pair<int64_t, Job>, vector<pair<int64_t, Job>>, greater<>> &activeJobs,
                   mutex &jobsMutex, bool &done) {
    // cout << "done :" << done  << endl;
    while (!done || !activeJobs.empty()) {
        {
            // cout << "10.unique_lock jobMutex bef acq" << endl;
            unique_lock<mutex> lock(jobsMutex);
            // cout << "10.unique_lock jobMutex bef acq" << endl;

            if (activeJobs.empty()) {
                lock.unlock();
                this_thread::sleep_for(chrono::milliseconds(10));
                continue;
            }
            // least endtime one is taken
            auto [endTime, job] = activeJobs.top();
            int64_t currentTime = currTime();
            if (endTime > currentTime) {
                lock.unlock();
                this_thread::sleep_for(chrono::microseconds(endTime - currentTime));
                continue;
            }

            activeJobs.pop();
            lock.unlock();

            shared_ptr<Server> server;
            {
                // cout << "11.shared_lock mapMutex bef acq" << endl;
                shared_lock<shared_mutex> lock(servers.mapMutex);
                // cout << "11.shared_lock mapMutex acq" << endl;

                auto it = servers.activeServerMap.find(job.serverId);
                if (it == servers.activeServerMap.end())
                    continue;
                server = it->second;
            }

            // cout << "13.write_lock bef acq" << endl;
            server->lock.writeLock();
            // cout << "13.write_lock acq" << endl;

            server->releaseJob(job);
            bool shouldRemove = server->endTime <= currTime() && server->isIdle();

            // cout << "14.write_unlock bef acq" << endl;
            server->lock.writeUnlock();
            // cout << "14.write_unlock acq" << endl;

            if (shouldRemove) {
                cost += (server->endTime - server->startTime);
                // cout << "12.unique_lock mapMutex bef acq" << endl;
                lock_guard<shared_mutex> lock(servers.mapMutex);
                // cout << "12.unique_lock mapMutex acq" << endl;

                servers.activeServerMap.erase(job.serverId);
                if (servers.activeServerMap.size() == 0) {
                    // done = true;
                }
            }
        }
    }
}

int main() {
    Config cfg = loadConfig("config.txt");

    int64_t baseTime = currTime();       // The "simulation start time"
    int64_t simulationDuration = 600000; // e.g., simulate 0.1 minute total

    // Random generator setup
    mt19937 gen(42);
    uniform_int_distribution<int64_t> jobArrivalTimeDist(0, simulationDuration);
    uniform_int_distribution<> duration(250000000, 300000000); // Jobs last 1s to 5s
    // uniform_int_distribution<> slackTimeDist(2500000, 3000000); // Allow jobs to have start-by within 2s to 8s after arrival
    uniform_int_distribution<> resourceGen(1, 50);

    int d = cfg.d;
    vector<int> capacities(d, 1000);
    Servers servers(capacities);

    vector<shared_ptr<Job>> jobList;
    for (int i = 0; i < cfg.numJobs; ++i) {
        int64_t entryTime = baseTime; // All jobs have same queueEntryTime
        // int64_t startByTime = entryTime + slackTimeDist(gen); // must start within 2-8s after arrival

        vector<int> resources;
        for (int j = 0; j < d; ++j) {
            resources.push_back(resourceGen(gen));
        }

        jobList.push_back(make_shared<Job>(i, entryTime, duration(gen), resources));
    }

    // Statically assign jobs to threads
    vector<vector<shared_ptr<Job>>> threadJobs(cfg.numThreads);
    int jobsPerThread = (cfg.numJobs + cfg.numThreads - 1) / cfg.numThreads; // Ceiling division
    for (int i = 0; i < cfg.numJobs; ++i) {
        int threadId = i / jobsPerThread;
        if (threadId < cfg.numThreads) {
            threadJobs[threadId].push_back(jobList[i]);
        }
    }

    priority_queue<pair<int64_t, Job>, vector<pair<int64_t, Job>>, greater<>> activeJobs;
    mutex jobsMutex;
    bool done = false;

    int64_t responseStartTime = currTime();

    // Launch worker threads with their job lists
    vector<thread> threads;
    for (int i = 0; i < cfg.numThreads; ++i) {
        threads.emplace_back(workerThread, threadJobs[i], ref(servers), cfg.k, cfg.maxRetries,
                             ref(activeJobs), ref(jobsMutex));
    }

    // Launch cleanup thread
    thread cleanup(cleanupThread, ref(servers), ref(activeJobs), ref(jobsMutex), ref(done));

    // Signal cleanup thread to finish
    {
        // cout << "15.unique_lock jobsMutex bef acq" << endl;
        lock_guard<mutex> lock(jobsMutex);
        // cout << "15.unique_lock jobsMutex acq" << endl;

        done = true;
    }

    // Wait for all worker threads to complete
    for (auto &th : threads) {
        th.join();
    }

    // cout << "after done = true" << endl;

    cleanup.join();
    // cout << "after cleanup join" << endl;

    for (auto &server : servers.activeServerMap) {
        cost += ((server.second)->endTime - (server.second)->startTime);
    }

    int64_t responseEndTime = currTime();

    // Output job details
    for (int i = 0; i < jobList.size(); i++) {
        cout << "Queue entry time:" << jobList[i]->queueEntryTime
             << " duration:" << jobList[i]->duration
             << " schedulingEndTime:" << jobList[i]->schedulingEndTime << endl;
    }
    ofstream outFile("job_data.csv");
    outFile << "JobID,QueueEntryTime,Duration,SchedulingEndTime\n";
    for (const auto &job : jobList) {
        outFile << job->jobId << ","
                << job->queueEntryTime << ","
                << job->duration << ","
                << job->schedulingEndTime << "\n";
    }
    outFile.close();

    cout << "\nAll jobs scheduled.\n";
    cout << "Total time to run: " << responseEndTime - responseStartTime << endl;
    cout << "Scheduling Time: " << schedulingTime << endl;
    cout << "server count: " << serverCount << endl;
    cout << "total cost: " << cost << endl;
    cout << "act servers:" << servers.activeServerMap.size() << endl;
    cout << "scheduledJobCounter: " << scheduledJobCounter << endl;
    return 0;
}