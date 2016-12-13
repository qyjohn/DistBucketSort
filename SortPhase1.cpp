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
using namespace std;


class IntermediateBuffer
{
	public:
	int partition_id = 0;
	int bucket_id = 0;
	int file_counter   = 0;
	int record_counter = 0;
	int buffer_size=1000000;

	char work_folder[1024];
	char filename[1024];
	char *buffer;
	pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

	void initialize(int partition, int bucket)
	{
		partition_id = partition;
		bucket_id = bucket;
		sprintf(work_folder, "/disk%04d/intermediate/bucket%04d", partition, bucket);
		buffer = new char[100*buffer_size];
	}
	
	void add_record(char* data)
	{
		pthread_mutex_lock(&mutex);
		memcpy(buffer+100*record_counter, data, 100);
		record_counter++;
		
		if (record_counter == buffer_size)
		{
			sprintf(filename, "%s/%04d", work_folder, file_counter);
			std::ofstream outfile(filename,std::ofstream::binary);
			outfile.write (buffer, 100*buffer_size);
			outfile.close();

			file_counter++;
			record_counter = 0;
		}
		
		pthread_mutex_unlock(&mutex);
	}
	
	void final_save_buffer()
	{
		if (record_counter > 0)
		{
			sprintf(filename, "%s/%04d", work_folder, file_counter);
			std::ofstream outfile(filename,std::ofstream::binary);
			outfile.write (buffer, 100*record_counter);
			outfile.close();

			delete[] buffer;
		}
	}
};

pthread_mutex_t lock;
void *phase_1_thread(void *args);
struct phase_1_thread_args
{
	std::vector<std::string> *files;
	std::vector<IntermediateBuffer> *buckets;
	int total_buckets;
	int bucket_hash_bar;
};

/**
 *
 * args[1] = total nodes
 * args[2] = buckets per partition
 * args[3] = total threads
 * args[4] = node id
 *
 **/
 
int main(int argc, char* argv[])
{    
	// number of partitions equals to the number of nodes, also equals to the number of disks per node
	int total_nodes		= atoi(argv[1]);	
	int total_disks		= total_nodes;
	int total_partitions= total_nodes;
	int buckets_per_partition = atoi(argv[2]);
	int total_buckets	= total_nodes * buckets_per_partition;
	int bucket_hash_bar = (ceil) (65536 / total_buckets);
	int partition_hash_bar = (ceil) (total_buckets / total_partitions);
	int total_threads	= atoi(argv[3]);
	

	// Initialize all the buckets
	int i, partition;
	IntermediateBuffer buckets[total_buckets];
	std::vector<IntermediateBuffer> buckets_v;
	for (i=0; i<total_buckets; i++)
	{
		partition = (floor) (i / partition_hash_bar);
		if (partition >= total_nodes)
		{
			partition = total_nodes - 1;
		} 
		buckets[i].initialize(partition, i);
		buckets_v.push_back(buckets[i]);
	}
	
	// Look into the input folders, create a list (std:vector) with the filename of all the input files
	std::vector<std::string> files;
	for (i=0; i<total_disks; i++)
	{
		DIR *dpdf;
		struct dirent *epdf;
		char folder[200], temp[1024];
		sprintf(folder, "/disk%04d/input", i);
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
				}
			}
		}
	}
	
	// Reshuffle the list to create a random list to achieve a balance of read and write across all disks.
	std::random_shuffle(files.begin(), files.end());
	
	
	// Now, launch N threads to save the data to disk, N = cpu_cores
	pthread_t 	sort_threads[total_threads];
	struct 		phase_1_thread_args sort_args;
	sort_args.files = &files;
	sort_args.buckets = &buckets_v;
	sort_args.total_buckets = total_buckets;
	sort_args.bucket_hash_bar = bucket_hash_bar;
	for (i=0; i<total_threads; i++)
	{
		pthread_create(&sort_threads[i], NULL, phase_1_thread, (void*) &sort_args); 
	}
	for (i=0; i<total_threads; i++)
	{
		pthread_join(sort_threads[i], NULL);
	}
}


void *phase_1_thread(void *args)
{
	struct phase_1_thread_args *myArgs = (struct phase_1_thread_args*) args;
	std::vector<std::string> *files = myArgs->files;
	std::vector<IntermediateBuffer> *buckets   = myArgs->buckets;
	int total_buckets = myArgs->total_buckets;
	int bucket_hash_bar = myArgs->bucket_hash_bar;

	std::string file;
	bool next_file = true;
	pthread_t         self = pthread_self();	
	while (next_file)
	{
		pthread_mutex_lock(&lock);
	    if (files->empty())
	    {
	    	next_file = false;
	    }
	    
	    if (next_file)
	    {
			file = files->back();
			files->pop_back();
		}
		pthread_mutex_unlock(&lock);
	 
	   	if (next_file)
	   	{
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
				int i;
				int count = size / 100;
				char record[100];
				for (i=0; i<count; i++)
				{
					int start = 100*i;
					int key = (unsigned char) buffer[start] * 256;
					int target = floor(key/bucket_hash_bar);
					{
						if (target >= total_buckets)
						{
							target = total_buckets - 1;
						}
					}
					memcpy(record, buffer+start, 100);
					buckets->at(target).add_record(record);
				}
				
				delete[] buffer;
			}
		}
	}
}
