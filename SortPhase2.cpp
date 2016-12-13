/**
 *
 * Distributed Bucket Sort by Moving Storage to Compute
 *
 * During the Phase I of the sorting, the input turples are distributed across a total number of N disks, under folders
 * /disk0/input, /disk1/input, /disk2/input, /disk3/input, etc.
 *
 * The objective of the Phase I sorting is to read all the input turples from N disks, map them to the right partition, 
 * and distribute them to the right bucket. Each disk represents a partition, inside each partition there are multiple 
 * buckets. So, the output of the Phase I sorting will be distributed under folders /disk0/intermediate/bucket0,
 * /disk0/intermediate/bucket1, /disk0/intermediate/bucket2, /disk1/intermediate/bucket0, /disk1/intermediate/bucket1,
 * /disk1/intermediate/bucket2, etc.
 *
 * In phase I we only do partition and bucket mapping, without any sorting.
 *
 * After the Phase I sorting, the disks are un-mounted and detached from the nodes, and are attached to the destination 
 * nodes again. For example, disk0 from all nodes are attached to node0, disk1 from all nodes are attached to node1. as 
 * a result, each node now becomes a partition, with only data belonging to this partition. The input turples are distributed
 * across a total number of N disks, under folders /disk0/intermediate/bucket0, /disk0/intermediate/bucket1, 
 * /disk0/intermediate/bucket2, /disk1/intermediate/bucket0, /disk1/intermediate/bucket1, /disk1/intermediate/bucket2, etc.
 *
 * During the Phase II of the sorting, we read from each bucket and perfrom a merge sort for the bucket. The input for each
 * bucket includes /disk0/intermediate/bucket0, /disk1/intermediate/bucket0, /disk2/intermediate/bucket0, etc. The output for 
 * each bucket will be /disk0/output/, /disk1/output/, /disk2/output, etc.
 *
 * Because the disks are not in a RAID configuration, it is important to read randomly from each disk to avoid skewness in 
 * disk read / pattern.
 *
 */


#include <string.h>
#include <cstring>
#include <unistd.h>
#include <stdio.h>
#include <netdb.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <dirent.h>
#include <netinet/in.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <iomanip>
#include <queue>
#include <strings.h>
#include <stdlib.h>
#include <string>
#include <time.h>
#include <vector>
#include <algorithm>
#include <math.h>
#include <pthread.h>
#include <fcntl.h>
#include <malloc.h>
using namespace std;


class SortRecord
{
	public:
	char m_data[100];
	bool operator<(const SortRecord& other) const
	{

		if (memcmp(m_data, other.m_data, 10) < 0) 
			return false;
		else
			return true;
	}
};

class SortRecordQueue
{
	public:
	std::priority_queue<SortRecord> q;
	pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
	
	void push(SortRecord sr)
	{
		pthread_mutex_lock(&mutex);
		q.push(sr);
		pthread_mutex_unlock(&mutex);
	}
	SortRecord top(){return q.top();}
	void pop()      {q.pop();}
	size_t size()   {return q.size();}
	bool empty()    {return q.empty();}
};

time_t current_time;
char message[1024];
pthread_mutex_t lock;
void *phase_2_thread(void *args);
struct phase_2_thread_args
{
	std::vector<int> *buckets;
	int total_nodes;
};
void save_queue_to_file_buffer_io(std::priority_queue<SortRecord> *q, char* filename);
void save_queue_to_file_direct_io(std::priority_queue<SortRecord> *q, char* filename);
void log_progress(char* msg);


/**
 *
 * args[1] = total nodes
 * args[2] = buckets per partition
 * args[3] = total threads
 * args[4] = node id
 *
 */

int main(int argc, char* argv[])
{    
	time_t current_time;
	// number of partitions equals to the number of nodes, also equals to the number of disks per node
	int total_nodes		= atoi(argv[1]);	
	int total_disks		= total_nodes;
	int total_partitions= total_nodes;
	int buckets_per_partition = atoi(argv[2]);
	int total_buckets	= total_nodes * buckets_per_partition;
	int bucket_hash_bar = (ceil) (65536 / total_buckets);
	int partition_hash_bar = (ceil) (total_buckets / total_partitions);
	int total_threads	= atoi(argv[3]);
	int node_id			= atoi(argv[4]);
	
	// Create a vector with a list of buckets that should be processed on this node (partition)
	int i;
	std::vector<int> buckets;
	int start = node_id * buckets_per_partition;
	int end = start + buckets_per_partition;
	for (i=start; i<end; i++)
	{
		buckets.push_back(i);
	}
	
	
	// Now, launch N threads to save the data to disk, N = cpu_cores
	pthread_t 	sort_threads[total_threads];
	struct 		phase_2_thread_args sort_args;
	sort_args.buckets = &buckets;
	sort_args.total_nodes = total_nodes;
	for (i=0; i<total_threads; i++)
	{
		pthread_create(&sort_threads[i], NULL, phase_2_thread, (void*) &sort_args); 
	}
	for (i=0; i<total_threads; i++)
	{
		pthread_join(sort_threads[i], NULL);
	}
}


void *phase_2_thread(void *args)
{
	struct phase_2_thread_args *myArgs = (struct phase_2_thread_args*) args;
	std::vector<int> *buckets = myArgs->buckets;
	int total_nodes = myArgs->total_nodes;

	int i, bucket_id;
	bool next_bucket = true;
	pthread_t         self = pthread_self();	
	while (next_bucket)
	{
		pthread_mutex_lock(&lock);
	    if (buckets->empty())
	    {
	    	next_bucket = false;
	    }
	    if (next_bucket)
	    {
			bucket_id = buckets->back();
			buckets->pop_back();
		}
		pthread_mutex_unlock(&lock);
	
	   	if (next_bucket)
	   	{
			// Look into the input folders, create a list (std:vector) with the filename of all the input files
			std::vector<std::string> files;
			for (i=0; i<total_nodes; i++)
			{
				DIR *dpdf;
				struct dirent *epdf;
				char folder[200], temp[1024];
				sprintf(folder, "/disk%04d/intermediate/bucket%04d", i, bucket_id);
				dpdf = opendir(folder);
				// Get a list of the files in the work folder, sorted by filename
				if (dpdf != NULL)
				{
					// Load data from all the files in the temporary directory
					while (epdf = readdir(dpdf))
					{
						if (epdf->d_type != DT_DIR)
						{
							sprintf(temp, "%s/%s", folder, epdf->d_name);
							std::string filename = temp;
							files.push_back(filename);
							cout << self << "\t" << filename << "\n";
						}
					}
				}
			}
			
			// Reshuffle the list to create a random list to achieve a balance of read and write across all disks.
			std::random_shuffle(files.begin(), files.end());
	   	
			// Sort all the inputs in the bucket
			std::priority_queue<SortRecord> q;
			while (!files.empty())
			{
				std::string file = files.back();
				files.pop_back();
				ifstream in(file.c_str(), ifstream::in | ios::binary);
				if (in)	// the file was open successfully
				{
					// Get file size
					in.seekg(0, std::ios::end);
					int size = in.tellg();
					// Create a buffer as large as the file itself
					char * buffer = new char[in.tellg()];
					// Go back to the beginning of the file and read the whole thing
					in.seekg(0, std::ios::beg);
					in.read(buffer, size);
					// Close the file
					in.close();
			
					// We know that each record is 100 bytes 
					int count = size / 100;
					int i=0, j=0, start = 0;
					char record[100];
					for (i=0; i<count; i++)
					{
						start = 100*i;
						// Create a SortRecord
						SortRecord sr;
						memcpy(sr.m_data, buffer+start, 100);
						q.push(sr);
					}
			
					// free the memory being used by the file buffer
					delete[] buffer;					
				}
			}

			// write sorted result into output file
			int disk_id = bucket_id % total_nodes;
			char output[1024];
			sprintf(output, "/disk%04d/output%04d", disk_id, bucket_id);	
			save_queue_to_file_direct_io(&q, output);
			// delete q 
			q = std::priority_queue<SortRecord>();
		}
	}
}


/**
 *
 * Save the content in a queue to an output file using BufferIO.
 *
 */

void save_queue_to_file_buffer_io(std::priority_queue<SortRecord> *q, char* filename)
{
	if (!q->empty())	// only create a file when there are records to write
	{
		sprintf(message, "BufferIO writing to file %s.", filename);
		log_progress(message);

		std::ofstream outfile (filename,std::ofstream::binary);
		while(!q->empty()) 
		{
			outfile.write (q->top().m_data, 100);
			q->pop();
		}
		outfile.close();

		sprintf(message, "BufferIO closing file %s.", filename);
		log_progress(message);
	}
}


/**
 *
 * Save the content in a queue to an output file using DirectIO.
 *
 */

void save_queue_to_file_direct_io(std::priority_queue<SortRecord> *q, char* filename)
{
	int i, j, base;
	char* buffer = new char[51200];	// 512 records = 51200 bytes

	// Use DirectIO to save data in 512 record blocks
	if (q->size() >= 512)
	{
		char filename_1[1024];
		sprintf(filename_1, "%s-1", filename);
		sprintf(message, "DirectIO writing to file %s.", filename_1);
		log_progress(message);

		int fd = open(filename_1, O_RDWR | O_CREAT | O_DIRECT | O_TRUNC, 0644);
		// DirectIO buffer, 512 is block size, 51200 is buffer size for 512 records
		void *temp = memalign(512, 51200);
		while (q->size() >=512)
		{
			base = 0;
			for (i=0; i<512; i++)	// Get 512 records at a time
			{
				base = i * 100;
				memcpy(buffer+base, q->top().m_data, 100);
				q->pop();
			}
			memcpy(temp, buffer, 51200);
			write(fd, temp, 51200);
		}
		free(temp);
		close(fd);

		sprintf(message, "DirectIO closing file %s.", filename_1);
		log_progress(message);
	}

	// Use BufferIO to save the rese data
	if (!q->empty())
	{
		char filename_2[1024];
		sprintf(filename_2, "%s-2", filename);
		sprintf(message, "BufferIO writing to file %s.", filename_2);
		log_progress(message);
		std::ofstream outfile (filename_2,std::ofstream::binary);
		i = 0;
		while(!q->empty()) 
		{
			base = i * 100;
			memcpy(buffer+base, q->top().m_data, 100);
			q->pop();
			i++;
		}
		outfile.write (buffer, i*100);
		outfile.close();
		sprintf(message, "BufferIO closing file %s.", filename_2);
		log_progress(message);
	}
	
	delete[] buffer;
}

void log_progress(char* msg)
{
	time(&current_time);
	cout << current_time << "\t" << msg << "\n";
}
