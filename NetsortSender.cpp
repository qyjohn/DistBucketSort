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
	int record_counter = 0;
	int buffer_size=1000;

	int socket;
	char *buffer;
	pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

	void set_socket(int soc)
	{
		socket = soc;
		buffer = new char[100*buffer_size];
	}
	
	void add_record(char* data)
	{
		pthread_mutex_lock(&mutex);
		memcpy(buffer+100*record_counter, data, 100);
		record_counter++;
		
		if (record_counter == buffer_size)
		{
			write(socket, buffer, 100*buffer_size);
			record_counter = 0;
		}
		
		pthread_mutex_unlock(&mutex);
	}
	
	void final_send_buffer()
	{
		if (record_counter > 0)
		{
			write(socket, buffer, 100*record_counter);
			delete[] buffer;
		}
		string exit_string = "EXIT";
		write(socket, exit_string.c_str(), 4);
	}
};

std::vector<int> sockets;
std::vector<IntermediateBuffer> buckets;

int open_connection(char* host, int port)
{
	int listenFd;
	struct sockaddr_in svrAdd;
	struct hostent *server;

	//create client skt
	listenFd = socket(AF_INET, SOCK_STREAM, 0);    
	if(listenFd < 0)
	{
		cerr << "Cannot open socket" << endl;
		return 0;
	}
    
	// check target host
	server = gethostbyname(host);    
	if(server == NULL)
	{
		cerr << "Host does not exist" << endl;
		return 0;
	}
    
	// some necessary preparation
	bzero((char *) &svrAdd, sizeof(svrAdd));
	svrAdd.sin_family = AF_INET;
	bcopy((char *) server -> h_addr, (char *) &svrAdd.sin_addr.s_addr, server -> h_length);
	svrAdd.sin_port = htons(port);
    
	// create the connection
	int checker = connect(listenFd,(struct sockaddr *) &svrAdd, sizeof(svrAdd));    
	if (checker < 0)
	{
		cerr << "Cannot connect!" << endl;
		return 0;
	}
	else
	{
		return listenFd;
	}
}

void initialize(int port)
{
	// Get a list of SortServer from servers.cfg
	// This is a global configuration file for all Sender
	std::ifstream conf_file("servers.cfg");
	char server[1024];
	while (conf_file.getline(server, 1024))
	{
		int socket = open_connection(server, port);
		sockets.push_back(socket);
		IntermediateBuffer bucket;
		bucket.set_socket(socket);
		buckets.push_back(bucket);
	}
}


pthread_mutex_t lock;
void *phase_1_thread(void *args);
struct phase_1_thread_args
{
	std::vector<std::string> *files;
	std::vector<IntermediateBuffer> *buckets;
	int total_nodes;
	int node_hash_bar;
};

/**
 *
 * args[1] = total nodes
 * args[2] = total threads
 * args[3] = node id
 * args[4] = port number
 *
 **/
 
int main(int argc, char* argv[])
{    
	// number of partitions equals to the number of nodes, also equals to the number of disks per node
	int total_nodes		= atoi(argv[1]);	
	int node_hash_bar = (ceil) (65536 / total_nodes);
	int total_threads	= atoi(argv[2]);
	int node_id	= atoi(argv[3]);
	int port = atoi(argv[4]);


	// Initialize all the sockets and buckets
	initialize(port);
	
	// Create a list (std:vector) with the filename of all the input files
	std::vector<std::string> files;
	DIR *dpdf;
	struct dirent *epdf;
	char folder[200], temp[1024];
	sprintf(folder, "/NetSort/Input");
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
	
	
	// Now, launch N threads to send the data over the network
	int i;
	pthread_t 	sort_threads[total_threads];
	struct 		phase_1_thread_args sort_args;
	sort_args.files = &files;
	sort_args.buckets = &buckets;
	sort_args.total_nodes = total_nodes;
	sort_args.node_hash_bar = node_hash_bar;
	for (i=0; i<total_threads; i++)
	{
		pthread_create(&sort_threads[i], NULL, phase_1_thread, (void*) &sort_args); 
	}
	for (i=0; i<total_threads; i++)
	{
		pthread_join(sort_threads[i], NULL);
	}

	// Flush all the remaining data in buffer
	for (i=0; i<total_nodes; i++)
	{
		buckets.at(i).final_send_buffer();
	}
}


void *phase_1_thread(void *args)
{
	struct phase_1_thread_args *myArgs = (struct phase_1_thread_args*) args;
	std::vector<std::string> *files = myArgs->files;
	std::vector<IntermediateBuffer> *buckets   = myArgs->buckets;
	int total_nodes = myArgs->total_nodes;
	int node_hash_bar = myArgs->node_hash_bar;

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
					int target = floor(key/node_hash_bar);
					{
						if (target >= total_nodes)
						{
							target = total_nodes - 1;
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
