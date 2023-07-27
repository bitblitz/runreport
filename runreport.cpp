/*

*/
#include <map>
#include <set>
#include <list>
#include <cmath>
#include <ctime>
#include <deque>
#include <queue>
#include <stack>
#include <string>
#include <bitset>
#include <cstdio>
#include <limits>
#include <vector>
#include <climits>
#include <cstring>
#include <cstdlib>
#include <fstream>
#include <numeric>
#include <sstream>
#include <iostream>
#include <algorithm>
#include <unordered_map>
#include <assert.h>
#include <string_view>
#include <functional>
#include <iomanip> 
#include <format>

using namespace std;

// error exit and end program on malformed input
void malformed_exit()
{
    cout << "Malformed Input" << endl;
    exit(1);
}

// assumes base 10, and only positive numbers
int safe_stoi(const string& s)
{
    if (s.find_first_not_of("0123456789") != string::npos)
    {
        malformed_exit();
    }
    return stoi(s);
}

// turn # of seconds into an HH:MM:SS string
string format_hms(int seconds)
{
    int hours = seconds / 3600;
    int min = (seconds % 3600) / 60;
    seconds = seconds % 60;
    return format("{:02}:{:02}:{:02}", hours, min, seconds);
}

struct JobInfo
{
    JobInfo(const vector<string>& rawData)
    {
        if (rawData.size() != 3)
        {
            malformed_exit();
        }
        m_jobId = safe_stoi(rawData[0]);
        m_runtime = safe_stoi(rawData[1]);
        m_nextJobId = safe_stoi(rawData[2]);
    }

    int m_jobId;
    int m_runtime;
    int m_nextJobId;
};

class Chain
{
public:
    Chain(int jobId, int runtime, int nextJobId) :
        m_firstJobId(jobId),
        m_lastJobId(jobId),
        m_nextJobId(nextJobId),
        m_jobCount(1),
        m_totalRuntime(runtime)
    {
    }

    void add(int jobId, int runtime, int next)
    {
        assert(jobId == m_nextJobId);
        m_totalRuntime += runtime;
        m_jobCount++;
        m_lastJobId = jobId;
        m_nextJobId = next;
    }

    void printChainReport()
    {
        cout << "start_job:" << m_firstJobId << endl;
        cout << "last_job:" << m_lastJobId << endl;
        cout << "number_of_jobs:" << m_jobCount << endl;
        cout << "job_chain_runtime:" << format_hms(m_totalRuntime)  << endl;
        cout << "average_job_time:" << format_hms(m_totalRuntime / 2) << endl;
        cout << "-" << endl;
   }

    int NextJobId() const
    {
        return m_nextJobId;
    }

    int FirstJobId() const
    {
        return m_firstJobId;
    }

private:
    int m_firstJobId;
    int m_lastJobId;
    int m_nextJobId;
    int m_jobCount;
    int m_totalRuntime;
};

vector<string> splitString(const string& s, const string& delim)
{
    string::size_type start = 0;
    string::size_type next = 0;
    vector<string> result;

    do
    {
        next = s.find(delim, start);
        if (next != string::npos)
        {
            string w = s.substr(start, next - start);
            result.push_back(w);
            start = next + 1;
        }
        else
        {
            result.push_back(s.substr(start));
        }
    } while (next != string::npos);
    return result;
}


const string EXPECTED_HEADER = "#job_id,runtime_in_seconds,next_job_id";

// Set implementation runs about 1 second slower for 1 million records, but uses about half memory of 
// the Map implementation in debug mode. Set is still more memory efficient in release possibly due to 
// one less pointer dereference.  Numbers from VS debugging.
// perf for ~1 million rows.  With and without formatting because that formatting takes up a lot of the total time.
//
//       Debug              Release             Debug (no format)       Release (no format) 
// MAP   44301ms / 106MB    11446 ms / 54MB     23559ms / 106MB         2230ms / 54 MB
// SET   45129ms / 62MB     11550 ms / 36MB     16475ms / 62MB          2224ms / 35 MB
#define PROCESS_AS_MAPS 0

#if PROCESS_AS_MAPS
// Map-based implementation
// 
// indexed by next
static unordered_map<int, Chain*> open_chains;
// indexed by first
static map<int, Chain*> closed_chains;

// add Chain* to map index by given id
// terminate with malformed input if a dupe
template<typename mapType,
    typename keyType = mapType::keyType,
    typename dataType = mapType::mappedType>
void verify_add(mapType& map, keyType key, dataType data)
{
    if (map.find(key) == map.end())
    {
        map.insert(make_pair(key, data));
    }
    else
    {
        malformed_exit();
    }
}

void ProcessItemIntoMaps(JobInfo& ji)
{
    auto it = open_chains.find(ji.m_jobId);
    if (it == open_chains.end())
    {
        // new chain
        Chain* newNode = new Chain(ji.m_jobId, ji.m_runtime, ji.m_nextJobId);
        if (newNode->NextJobId() == 0)
        {
            // chain is complete, just put in output assuming no dupes
            // this will also exit with malformed_exit if it is a duplicate jobid
            //printf("new closed chain: first %d, next %d\n", ji.m_jobId, ji.m_nextJobId);
            verify_add(closed_chains, ji.m_jobId, newNode);
        }
        else
        {
            // add to open chains, will exit if this represents a duplicate
            // first node, which should be impossible due to find above.
            //printf("new open chain: first %d, next %d\n", ji.m_jobId, ji.m_nextJobId);
            verify_add(open_chains, ji.m_nextJobId, newNode);
        }
    }
    else
    {
        // chain is open, extend it, and if closed, move it.
        Chain* chain = it->second;
        it->second->add(ji.m_jobId, ji.m_runtime, ji.m_nextJobId);
        if (ji.m_nextJobId == 0)
        {
            // move to closed chains, indexed on first
            //printf("closing chain: first %d\n", chain->FirstJobId());
            verify_add(closed_chains, chain->FirstJobId(), chain);
            // pointer is now owned by closed chains, remove from open
            open_chains.erase(it);
        }
        else
        {
            // reinsert into open chains based on new next id
            open_chains.erase(it);
            //printf("update open chain: first %d, next %d\n", chain->FirstJobId(), chain->NextJobId());
            verify_add(open_chains, chain->NextJobId(), chain);
        }
    }
}

void OutputMaps()
{
    // all chains should be closed
    if (!open_chains.empty())
    {
        malformed_exit();
    }
    cout << "-" << endl;
    for (auto&& chain : closed_chains)
    {
        chain.second->printChainReport();
        delete chain.second;
    }
}
#else // PROCESS_AS_MAPS

function<bool(const Chain& left, const Chain& right)> CompareChainsByNext =
[](const Chain& left, const Chain& right) {
    return left.NextJobId() < right.NextJobId();
};

function<bool(const Chain& left, const Chain& right)> CompareChainsByFirst =
[](const Chain& left, const Chain& right) {
    return left.FirstJobId() < right.FirstJobId();
};

set<Chain, decltype(CompareChainsByNext)> open_chains(CompareChainsByNext);
set<Chain, decltype(CompareChainsByFirst)> closed_chains(CompareChainsByFirst);

// implement based on set
void ProcessItemIntoSets(JobInfo& ji)
{
    // find the chain to connect this to
    Chain searchChain(0, 0, ji.m_jobId);
    // extract set node, update key and readd it
    auto it = open_chains.find(searchChain);
    if (it == open_chains.end() && ji.m_nextJobId == 0)
    {
        // new single node, add direclty to closed_chains
        //closed_chains.emplace(ji.m_jobId, ji.)
        closed_chains.insert(Chain(ji.m_jobId, ji.m_runtime, ji.m_nextJobId));
    }
    else if (it == open_chains.end())
    {
        // new open chain
        open_chains.insert(Chain(ji.m_jobId, ji.m_runtime, ji.m_nextJobId));
    }
    else if (ji.m_nextJobId != 0)
    {
        // update open chain
        auto currentNode = open_chains.extract(it);
        currentNode.value().add(ji.m_jobId, ji.m_runtime, ji.m_nextJobId);
        open_chains.insert(currentNode.value());
    }
    else
    {
        // close chain
        auto currentNode = open_chains.extract(it);
        currentNode.value().add(ji.m_jobId, ji.m_runtime, ji.m_nextJobId);
        closed_chains.insert(currentNode.value());
    }
}

void OutputSets()
{
    if (!open_chains.empty())
    {
        malformed_exit();
    }

    // print closed chains
    cout << "-" << endl;
    for (auto c : closed_chains)
    {
        c.printChainReport();
    }
}
#endif    


int runReport(istream& in)
{
    /* Enter your code here. Read input from STDIN. Print output to STDOUT */
    string raw_line;
    getline(in, raw_line);

    // first line must be a known header
    if (raw_line != EXPECTED_HEADER)
    {
        malformed_exit();
    }
    
    while (in && !in.eof())
    {
        getline(in, raw_line);
        if (raw_line.empty() )
            continue;

        // split line on commas
        // environment couldn't find ranges header, split manually
        //constexpr string_view comma_split{","};
        //ranges::split_view extractor{ raw_line, comma_split };
        auto split = splitString(raw_line, ",");

        // job info validates the contents of the line
        // if it's not valid, it dies with malformed_exit();
        JobInfo ji(split);

// implement based on dual maps
#if PROCESS_AS_MAPS
        ProcessItemIntoMaps(ji);
#else
        ProcessItemIntoSets(ji);
#endif
    }

#if PROCESS_AS_MAPS
    OutputMaps();
#else
    OutputSets();
#endif


return 0;
}

int main()
{
    //auto start = chrono::steady_clock::now();
    //cerr << "Start Time: " << std::time(NULL) << endl;
    auto result = runReport(cin);

    //auto end = chrono::steady_clock::now();
    //auto elapsed_ms = chrono::duration_cast<std::chrono::milliseconds>(end - start);
    //cerr << format("Elapsed: {}ms\n", elapsed_ms.count());
    return result;
}
