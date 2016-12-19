/*
 *  MultiServerV5 <total_nodes> <node_id> <server_port> <partition_factor> <in_memory_factor> <io_mode> <work_folder>
 *
 */

#include <string.h>
#include <unistd.h>
#include <dirent.h>
#include <stdio.h>
#include <netdb.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <iostream>
#include <fstream>
#include <strings.h>
#include <stdlib.h>
#include <string>
#include <pthread.h>
#include <queue>
#include <thread>
#include <fcntl.h>
#include <malloc.h>
#include <math.h>

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

void *save_intermediate_data_thread(void *args);
struct save_intermediate_thread_args
{
	char filename[1024];
	char *buffer;
	int  size;
};

class IntermediateBuffer
{
	public:
	int partition_id = 0;
	int buffer_size=500000; // 100 Byte x 500,000 = 50 MB to flush to disk, 
	int record_counter = 0;
	int file_counter   = 0;
	char filename[1024];
	char *buffer;
	pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

	void initialize(int id)
	{
		partition_id = id;
		buffer = new char[100*buffer_size];
	}
	
	void add_record(char* data)
	{
		pthread_mutex_lock(&mutex);
		memcpy(buffer+100*record_counter, data, 100);
		record_counter++;
		
		if (record_counter == buffer_size)
		{
			sprintf(filename, "/NetSort/Intermediate/Bucket%04d/%04d", partition_id, file_counter);
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
			sprintf(filename, "/NetSort/Intermediate/Bucket%04d/%04d", partition_id, file_counter);
			std::ofstream outfile(filename,std::ofstream::binary);
			outfile.write (buffer, 100*record_counter);
			outfile.close();

			delete[] buffer;
		}
	}
};

void log_progress(char* msg);
int  create_server(int port);
void *server_thread(void *args);
void *sort_thread(void *args);
void *save_data_thread(void *args);
bool test_exit(char* test);
void save_queue_to_file_buffer_io(std::priority_queue<SortRecord> *q, char* filename);
void save_queue_to_file_direct_io(std::priority_queue<SortRecord> *q, char* filename);
void merge_temp_files_and_save(char* dir, char* output_file, int io_mode);
void load_temp_file_to_queue(std::priority_queue<SortRecord> *q, char* filename);
pthread_mutex_t lock;
void *phase_2_thread(void *args);
struct phase_2_thread_args
{
	std::vector<int> *buckets;
	int node_id;
};

time_t current_time;
char message[1024];

struct server_thread_args
{
	// Total number of clients sending data
	int total_nodes;
	// Node name
	int node_id;
	// Server port
	int port_no;
	// CPU cores and total batches
	int cpu_cores;
	// Local data partition
	int buckets;
	// IO mode, 0 - DirectIO, 1 - BufferIO
	int io_mode;
	// Hash bar for each partition
	int hash_bar;
	// Lower range for this particilar server thread
	int lower_range;
	//
	std::vector<SortRecordQueue> *sr_queues;
	std::vector<IntermediateBuffer> *sr_buffers;
};

struct sort_thread_args
{
	int socket, sender_id;
	struct server_thread_args *server_args;
};


struct save_data_thread_args
{
	int thread_id;	
	int io_mode;
	int cpu_cores;
	int partition_factor;
	int in_memory_factor;
	char *work_folder;
	SortRecordQueue *queue;
};





int main(int argc, char* argv[])
{
	int i=0;
	int total_nodes      = atoi(argv[1]); // the number of nodes in the cluster
	int node_id          = atoi(argv[2]); // the id of this node
	int port_no          = atoi(argv[3]); // server port
	int buckets_per_node = atoi(argv[4]); // Number of buckets per node
	int io_mode          = atoi(argv[5]); // 0 is DirectIO, 1 is BufferIO
	int total_threads	 = atoi(argv[6]); // Nubmer of threads in the final sorting step
	
	int hash_bar = floor (65536 / (total_nodes * buckets_per_node));
	int lower_range = floor(65535 * node_id / total_nodes);

	// Now, create the IntermediateBuffer for intermediate data set
	IntermediateBuffer buffers[buckets_per_node];
	std::vector<IntermediateBuffer> sr_buffers;
	for (i=0; i<buckets_per_node; i++)
	{
		buffers[i].initialize(i);
		sr_buffers.push_back(buffers[i]);
	} 

	cout << "1\n";
	// Launch the server daemon
	pthread_t sort_server_thread;
	struct server_thread_args args;
	args.total_nodes = total_nodes;
	args.node_id     = node_id;
	args.port_no     = port_no;
	args.buckets  	 = buckets_per_node;
	args.io_mode     = io_mode;
	args.lower_range = lower_range;
	args.hash_bar    = hash_bar;
	args.sr_buffers  = &sr_buffers; 


	// Create the server thread and wait for the server thread to exit
	pthread_create(&sort_server_thread, NULL, server_thread, (void*) &args); 
	pthread_join(sort_server_thread, NULL);
	
	// Flush all the intermediate data to disk
	for (i=0; i<buckets_per_node; i++)
	{
		sr_buffers[i].final_save_buffer();
	} 

	// Create a vector with a list of buckets that should be processed on this node (partition)
	std::vector<int> buckets;
	for (i=0; i<buckets_per_node; i++)
	{
		buckets.push_back(i);
	}

	// Now, launch N threads to save the data to disk, N = cpu_cores
	pthread_t 	sort_threads[total_threads];
	struct 		phase_2_thread_args sort_args;
	sort_args.buckets = &buckets;
	sort_args.node_id = node_id;
	for (i=0; i<total_threads; i++)
	{
		pthread_create(&sort_threads[i], NULL, phase_2_thread, (void*) &sort_args); 
	}
	for (i=0; i<total_threads; i++)
	{
		pthread_join(sort_threads[i], NULL);
	}	
}

/**
 *
 * UsydSort - SortServer
 *
 * main() method
 * 1. create server socket and wait for client (Sender) connections
 * 2. for each client, create a pthread with a queue to handle incoming data
 * 3. wait for all threads to complete
 * 4. merge all the queues, save the merged and sorted data into a file
 *
 */

void *server_thread(void *args)
{
	int i, j;
	struct server_thread_args *myArgs = (struct server_thread_args*) args;
	int total_nodes   = myArgs->total_nodes;
	int node_id       = myArgs->node_id;
	int port_no       = myArgs->port_no;
	int cpu_cores     = myArgs->cpu_cores;
	int buckets    = myArgs->buckets;
	int io_mode       = myArgs->io_mode;
    
	// Create the server socket
	sprintf(message, "Launching sort server daemon on port %d.", port_no);
	log_progress(message);
	int listenFd = create_server(port_no);
	{
		if (listenFd <=0)	exit(1);
	}

	struct sockaddr_in clntAdd[total_nodes];
	pthread_t          senderThreads[total_nodes];
	socklen_t          len[total_nodes];
	int                clientSocket[total_nodes];
	struct sort_thread_args	sortArgs[total_nodes];

	sprintf(message, "Waiting for %d sender nodes to connect.", total_nodes);
	log_progress(message);
	// Wait for all Sender connections
	// For each Sender, create a pthread with a dedicated queue to handle the incoming data
	for (i=0; i<total_nodes; i++)
	{
		//this is where client connects. svr will hang in this mode until client conn
		len[i] = sizeof(clntAdd[i]);
		clientSocket[i] = accept(listenFd, (struct sockaddr *)&clntAdd[i], &len[i]);
		if (clientSocket[i] < 0)
		{
			cerr << "Cannot accept connection" << endl;
			return 0;
		}
        
		// Create a new thread to handle the input
		sortArgs[i].server_args = myArgs;
		sortArgs[i].socket      = clientSocket[i];
		sortArgs[i].sender_id   = i;
		pthread_create(&senderThreads[i], NULL, sort_thread, (void*) &sortArgs[i]); 
	}
    
	// Wait for all sender theads to exit
	for(i=0; i<total_nodes; i++)
	{
		pthread_join(senderThreads[i], NULL);
	}
}

/**
 *
 * Create the server socket to accept incoming Sender connections
 *
 */

int create_server(int port)
{
	// check port range
	if((port > 65535) || (port < 2000))
	{
		cerr << "Please enter a port number between 2000 - 65535" << endl;
		return 0;
	}
	    
	//create socket
	int listenFd = socket(AF_INET, SOCK_STREAM, 0);	    
	if(listenFd < 0)
	{
		cerr << "Cannot open socket" << endl;
		return 0;
	}
	    
	// initialization
	struct sockaddr_in svrAdd;
	bzero((char*) &svrAdd, sizeof(svrAdd));	    
	svrAdd.sin_family = AF_INET;
	svrAdd.sin_addr.s_addr = INADDR_ANY;
	svrAdd.sin_port = htons(port);
	    
	//bind socket
	if(bind(listenFd, (struct sockaddr *)&svrAdd, sizeof(svrAdd)) < 0)
	{
		cerr << "Cannot bind" << endl;
		return 0;
	}
	    
	// listen on socket
	listen(listenFd, 5);
	return listenFd;
}

/**
 *
 * A sorting thread, handling the data from one particular Sender.
 * The incoming data is pushed into a std::priority_queue.
 *
 */

void *sort_thread (void *args)
{
	struct sort_thread_args *myArgs = (struct sort_thread_args*) args;
	struct server_thread_args *server_args = myArgs->server_args;
	int sock      = myArgs->socket;	// the socket
	int sender_id = myArgs->sender_id;
	int cpu_cores   = server_args->cpu_cores;
	int hash_bar    = server_args->hash_bar;
	int lower_range = server_args->lower_range;
	int buckets  = server_args->buckets;
	std::vector<SortRecordQueue> *sr_queues = server_args->sr_queues;
	std::vector<IntermediateBuffer> *sr_buffers = server_args->sr_buffers;

	// Print out debug message
	sprintf(message, "Sender %04d is now connected.", sender_id);
	log_progress(message);
	
	// read 1000 records at a time, requiring 1000 x 100 = 100000 bytes for the buffer
	int buffer_count = 1000;
	int buffer_size = 100*buffer_count;
	char buffer[buffer_size], temp[100];
	int i=0, j=0, size=0, count=0, marker=0, buffer_start=0, buffer_left=0;
    
	// Read while the client is still connected. The client side will disconnect from the server
	// when the data transfer is completed.
	while(true)
	{
		// Attempt to receive [buffer_size] bytes each time, unless the client disconnects
		size=recv(sock, buffer, buffer_size, MSG_WAITALL);

		// Check the number of records
		if (size == buffer_size)
		{
			count = buffer_count;
		}
		else
		{
			count = floor(size / 100);
		}

		// Create individual records
		for (i=0; i<count; i++)
		{
			int key = (unsigned char) buffer[100*i] * 256;
			int partition = floor ((key - lower_range) / hash_bar);
			if (partition >= buckets)
			{
				partition = buckets - 1;
			}
			// Push the data into inermediate queue
			memcpy(temp, buffer + 100*i, 100);
			sr_buffers->at(partition).add_record(temp);
		}

		// Check exit signal
		marker = size % 100;
		if (marker == 4)
		{
			if (test_exit(buffer+size-4)) break;
		}	
	}

	// Print out debug message
	sprintf(message, "Sender %04d is now disconnected successfully.", sender_id);
	log_progress(message);
	close(sock);
}


/**
 *
 * Checking the EXIT signal from a Sender.
 *
 */

bool test_exit(char* test)
{
	if ((test[0] == 'E') && (test[1] == 'X') &&(test[2] == 'I') && (test[3] == 'T'))
		return true;
	else
		return false;
}


void *phase_2_thread(void *args)
{
	struct phase_2_thread_args *myArgs = (struct phase_2_thread_args*) args;
	std::vector<int> *buckets = myArgs->buckets;
	int node_id = myArgs->node_id;

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
			DIR *dpdf;
			struct dirent *epdf;
			char folder[200], temp[1024];
			sprintf(folder, "/NetSort/Intermediate/Bucket%04d", bucket_id);
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
					// Delete the file to save space
					std::remove(file.c_str()); 
			
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
			char output[1024];
			sprintf(output, "/NetSort/Output/%04d-%04d", node_id, bucket_id);	
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
