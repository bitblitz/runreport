// make_test_data.cpp : This file contains the 'main' function. Program execution begins and ends there.
//

#include <iostream>
#include <random>
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
#include <unordered_set>
#include <assert.h>
#include <string_view>
#include <functional>
#include <iomanip> 
#include <stdint.h>
#include <format>

using namespace std;

const string header="#job_id,runtime_in_seconds,next_job_id";
const uint32_t numberOfJobs = 100; // number of jobs created before remaining jobs are closed
const uint32_t maxJobId = 0x7FFFFFFF;
int randint(int min, int max)
{
    // RAND_MAX is too small on windows, make a 32 bit number
    uint32_t rand32 = 0;
    for (int i = 1; i <= 4; i++)
    {
        rand32 = (rand32 << 8) + (rand() & 0xFF);
    }
    return min + (rand32 % (max - min + 1));
}

int pickRandNotIn(unordered_set<int>& set)
{
    int r = 0;
    do
    {
        r = randint(1, maxJobId);
    } while (set.contains(r));
    return r;
}

int main()
{
    srand((unsigned) time(0));
    // all the nextJobIds that haven't been closed yet.
	int numberOfJobs = 1;
	// 10 to million, by powers of 10
	for (int filenum = 1; filenum <= 6; filenum++)
	{
		numberOfJobs *= 10;
		string fileName = format("cases{}.txt", numberOfJobs);
		fstream outFile = fstream(fileName, std::ios::out | std::ios::trunc);
		outFile << header << endl;
		vector<int> openchains;
		unordered_set<int> alljobids;
		openchains.reserve(numberOfJobs);
		alljobids.reserve(numberOfJobs * 2);

		for (int i = 0; i < numberOfJobs; i++)
		{
			switch (randint(1, 4))
			{
			case 1: // new single node chain
			{
				int jobId = pickRandNotIn(alljobids);
				alljobids.insert(jobId);
				int nextJobId = 0;
				int duration = randint(1, 1000);
				outFile << format("{:d},{:d},{:d}\n", jobId, duration, nextJobId);
				break;
			}
			case 2: // extend open chain
				if (!openchains.empty())
				{
					int n = randint(0, (int)openchains.size() - 1);
					int jobId = openchains[n];
					int nextJobId = pickRandNotIn(alljobids);
					alljobids.insert(nextJobId);
					openchains[n] = nextJobId;
					int duration = randint(1, 1000);
					outFile << format("{:d},{:d},{:d}\n", jobId, duration, nextJobId);
					break;
				}
				// else fallthrough
				__fallthrough;
			case 3: // close open chain
				if (!openchains.empty())
				{
					int n = randint(0, (int)openchains.size() - 1);
					int jobId = openchains[n];
					int nextJobId = 0;
					openchains.erase(openchains.begin() + n);
					int duration = randint(1, 1000);
					outFile << format("{:d},{:d},{:d}\n", jobId, duration, nextJobId);
					break;
				}

				// else fallthrough
				__fallthrough;

			case 4: // open new chain
			{
				int jobId = pickRandNotIn(alljobids);
				alljobids.insert(jobId);
				int nextJobId = pickRandNotIn(alljobids);
				alljobids.insert(nextJobId);
				openchains.push_back(nextJobId);
				int duration = randint(1, 1000);
				outFile << format("{:d},{:d},{:d}\n", jobId, duration, nextJobId);
			}
			break;
			}
		}
		// close remaining open chains
		for (int jobId : openchains)
		{
			int nextJobId = 0;
			int duration = randint(1, 1000);
			outFile << format("{:d},{:d},{:d}\n", jobId, duration, nextJobId);
		}
	}
}

// Run program: Ctrl + F5 or Debug > Start Without Debugging menu
// Debug program: F5 or Debug > Start Debugging menu

// Tips for Getting Started: 
//   1. Use the Solution Explorer window to add/manage files
//   2. Use the Team Explorer window to connect to source control
//   3. Use the Output window to see build output and other messages
//   4. Use the Error List window to view errors
//   5. Go to Project > Add New Item to create new code files, or Project > Add Existing Item to add existing code files to the project
//   6. In the future, to open this project again, go to File > Open > Project and select the .sln file
