// Refactored Multithreaded Job Scheduling System
#include <iostream>
#include <vector>
#include <queue>
#include <map>
#include <thread>
#include <mutex>
#include <shared_mutex>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <algorithm>
#include <memory>
#include <random>
#include <fstream>
#include <iomanip>
#include <math.h>

using namespace std;

int64_t currTime() {
    return chrono::duration_cast<chrono::microseconds>(
               chrono::steady_clock::now().time_since_epoch())
        .count();
}

struct Config {
    int numThreads;
    int numJobs;
    int d;  //number of resources
    int k;
    int maxRetries;
};

Config loadConfig(const string& filename) {
    Config cfg;
    ifstream infile(filename);
    infile >> cfg.numThreads >> cfg.numJobs >> cfg.d >> cfg.k >> cfg.maxRetries;
    return cfg;
}
/*
class FairLock {
public:
    shared_mutex rwLock;
    void readLock() { rwLock.lock_shared(); }
    void readUnlock() { rwLock.unlock_shared(); }
    void writeLock() { rwLock.lock(); }
    void writeUnlock() { rwLock.unlock(); }
};
*/
class Job {
public:
    int jobId;
    int serverId;
    //int64_t startByTime;
    int64_t duration;
    int64_t endTime;
    int64_t queueEntryTime;
    int64_t schedulingEndTime;

    vector<int> resourceRequired;

    Job(int id, int64_t queueEntryTime, int64_t dur, const vector<int>& res)
        : jobId(id), serverId(-1), queueEntryTime(queueEntryTime), duration(dur), endTime(0), resourceRequired(res) {}
    bool operator<(const Job& other) const {
        return endTime < other.endTime;
    }
};

class Server {
public:
    int serverId;
    int64_t startTime, endTime;
    vector<int> resourceAvailable;
    vector<int> capacity;
    //FairLock lock;
    vector<shared_ptr<Job>> assignedJobs;

    Server(int id, const vector<int>& cap) : serverId(id), startTime(INT64_MAX), endTime(0), capacity(cap), resourceAvailable(cap) {}

    bool canHandle(const Job& job) {
        for (size_t i = 0; i < job.resourceRequired.size(); ++i) {
            if (resourceAvailable[i] < job.resourceRequired[i]) return false;
        }
        return true;
    }

    void assignJob(Job& job, int64_t currentTime) {
        startTime = min(startTime, currentTime);
        job.endTime = currentTime + job.duration;
        endTime = max(endTime, job.endTime);
        job.serverId = serverId;
        for (size_t i = 0; i < job.resourceRequired.size(); ++i) {
            resourceAvailable[i] -= job.resourceRequired[i];
        }
        assignedJobs.push_back(make_shared<Job>(job));
    }

    void releaseJob(const Job& job) {
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
    int serverCounter{0};
    vector<int> commonCapacity;
    //mutable shared_mutex mapMutex;

    Servers(const vector<int>& capacity) : commonCapacity(capacity) {}

    shared_ptr<Server> createServer() {
        int id = serverCounter++;
        auto server = make_shared<Server>(id, commonCapacity);
        activeServerMap[id] = server;
        return server;
    }
};

//mutex avgLock, createServerMutex;
//atomic<int> counter(0);
int64_t schedulingTime(0);
int64_t cost(0);
int serverCount(0);

void cleanup(Servers& servers,
        priority_queue<pair<int64_t, Job>, vector<pair<int64_t, Job>>, greater<>>& activeJobs) {

    //while the endtime of job is less than currTime it is removed from activelist and server is updated
    while (!activeJobs.empty() && activeJobs.top().first < currTime()) {

        auto [endTime, job] = activeJobs.top();
        activeJobs.pop();
        shared_ptr<Server> server;
        {
            auto it = servers.activeServerMap.find(job.serverId);
            if (it == servers.activeServerMap.end()) continue;
                server = it->second;
        }
        server->releaseJob(job);
        cout << "job " << job.jobId << " is removed" << endl;

        bool shouldRemove = server->endTime <= currTime() && server->isIdle();

        if (shouldRemove) {
            cost += (server->endTime - server->startTime);
            servers.activeServerMap.erase(job.serverId);
            cout << "server " << job.serverId << " is removed" << endl;
        }

    }
}



shared_ptr<Server> scheduleJob(shared_ptr<Job> job, Servers& servers, priority_queue<pair<int64_t, Job>, vector<pair<int64_t, Job>>, greater<>>& activeJobs) {
    
    //cleans all jobs done before it and schedules job
    cleanup(servers, activeJobs);

    int64_t startOfScheduling = currTime();
    cout << "job" << job->jobId << "->startOfScheduling" << startOfScheduling << endl;
    shared_ptr<Server> serverGreedy = nullptr;

    {
        //shared_lock<shared_mutex> lock(servers.mapMutex);
        for (auto& [id, srv] : servers.activeServerMap) {
            //srv->lock.readLock();
            if(srv->canHandle(*job)){
                if(serverGreedy == nullptr || serverGreedy->endTime < srv->endTime){
                    serverGreedy = srv;
                }
            }
            this_thread::sleep_for(chrono::milliseconds(5));
            //srv->lock.readUnlock();
        }
    }
    
    if(serverGreedy == nullptr){
        //lock_guard<shared_mutex> lock(servers.mapMutex);
        auto newServer = servers.createServer();
        this_thread::sleep_for(chrono::milliseconds(25));
        //newServer->lock.writeLock();
        newServer->assignJob(*job, startOfScheduling);
        this_thread::sleep_for(chrono::milliseconds(15));
        //newServer->lock.writeUnlock();
        serverCount++;
        job->schedulingEndTime = currTime();
        serverGreedy = newServer;
    }
    else{
        //serverGreedy->lock.writeLock();
        serverGreedy->assignJob(*job, startOfScheduling);
        this_thread::sleep_for(chrono::milliseconds(15));
        //serverGreedy->lock.writeUnlock();
        job->schedulingEndTime = currTime();
    }
    
    schedulingTime += (job->schedulingEndTime - job->queueEntryTime);

    //lock_guard<mutex> lock(jobsMutex);
    activeJobs.push({ job->endTime, *job });
    
    return serverGreedy;
}



int main() {
    Config cfg = loadConfig("config.txt");

    int64_t baseTime = currTime();  // The "simulation start time"
    int64_t simulationDuration = 600000; // e.g., simulate 0.1 minute total

    // Random generator setup
    mt19937 gen(42);
    uniform_int_distribution<int64_t> jobArrivalTimeDist(0, simulationDuration);
    uniform_int_distribution<> duration(250000000, 300000000);  // Jobs last 1s to 5s
    //uniform_int_distribution<> slackTimeDist(2500000, 3000000); // Allow jobs to have start-by within 2s to 8s after their arrival
    uniform_int_distribution<> resourceGen(1, 50);

    int d = cfg.d;
    vector<int> capacities(d, 1000);
    Servers servers(capacities);

    vector<shared_ptr<Job>> jobList;
    for (int i = 0; i < cfg.numJobs; ++i) {
        int64_t arrivalOffset = jobArrivalTimeDist(gen);
        //int64_t entryTime = baseTime + arrivalOffset;
        int64_t entryTime = baseTime;
        //int64_t startByTime = entryTime + slackTimeDist(gen);  // must start within 2-8s after arrival

        vector<int> resources;
        for (int j = 0; j < d; ++j) {
            resources.push_back(resourceGen(gen));
        }

        cout << "Job " << i 
            << " entryTime:" << entryTime 
            << endl;

        jobList.push_back(make_shared<Job>(i, entryTime, duration(gen), resources));
    }

    condition_variable cv;
    bool done = false;

    priority_queue<pair<int64_t, Job>, vector<pair<int64_t, Job>>, greater<>> activeJobs;
    
    int64_t responseStartTime = currTime();

    for(int i=0; i< jobList.size(); i++){
        auto& job = jobList[i];
        scheduleJob(job, servers, activeJobs);
    }
    
    for(auto& server : servers.activeServerMap){
        cost += ((server.second)->endTime - (server.second)->startTime);
    }

    int64_t responseEndTime = currTime();

    for(int i=0;i<jobList.size();i++){
        cout << "Queue entry time:" << jobList[i]->queueEntryTime   << " duration:"<< jobList[i]->duration << " schedulingEndTime:" << jobList[i]->schedulingEndTime << endl;
    }
    ofstream outFile("job_data.csv");
    outFile << "JobID,QueueEntryTime,StartByTime,Duration,SchedulingEndTime\n";
    for (const auto& job : jobList) {
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

    /*
    for (const auto& [id, srv] : servers.activeServerMap) {
        cout << "Server " << id << " handled " << srv->assignedJobs.size() << " jobs:\n";
        for (const auto& job : srv->assignedJobs) {
            cout << "  Job(start: " << job->startByTime
                 << ", end: " << job->endTime
                 << ", duration: " << job->duration << ")\n";
        }
    }
    */

    return 0;
}
