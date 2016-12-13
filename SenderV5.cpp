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
#include <math.h>
using namespace std;

int port = 2000;
int totalServers = 0;
float hashBar = 0;
int BufferSize = 1000;
char** SendBuffers;
int SendBufferCounters[65535];
int SortServers[65535];


int is_regular_file(char *path)
{
	struct stat path_stat;
	stat(path, &path_stat);
	return S_ISREG(path_stat.st_mode);
}

int is_directory(char *path) 
{
	struct stat statbuf;
	if (stat(path, &statbuf) != 0)
		return 0;
	return S_ISDIR(statbuf.st_mode);
}

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

void initialize()
{
	// Get a list of SortServer from servers.cfg
	// This is a global configuration file for all Sender
	std::ifstream conf_file("servers.cfg");
	char server[1024];
	while (conf_file.getline(server, 1024))
	{
		int socket = open_connection(server, port);
		SortServers[totalServers] = socket;
		cout << "Server: " << server << "\tPort: " << port << "\tSocket: " << SortServers[totalServers] << "\n";
		totalServers++;
	}
	SendBuffers = new char * [totalServers];
	for (int i=0; i<totalServers; i++)
	{
		SendBuffers[i] = new char[100*BufferSize];
		SendBufferCounters[i] = 0;
	}
}

int send_file(const char* filename)
{
	// create an ifstream from the file
	int key, target;	
	char record[100];

	ifstream in(filename, ifstream::in | ios::binary);
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
			int target = floor(key/hashBar);
			{
				if (target >= totalServers)
				{
					target = totalServers - 1;
				}
			}
			memcpy(SendBuffers[target] + 100*SendBufferCounters[target], buffer+start, 100);
			SendBufferCounters[target] = SendBufferCounters[target] + 1;
			if (SendBufferCounters[target] == BufferSize)	// Buffer full
			{
				write(SortServers[target], SendBuffers[target], 100*BufferSize);
				SendBufferCounters[target] = 0;
			}
		}
		
		// Send all the remaining content in buffer
		for (i=0; i<totalServers; i++)
		{
			if (SendBufferCounters[i] > 0)
			{
				write(SortServers[i], SendBuffers[i], 100*SendBufferCounters[i]);
				SendBufferCounters[i] = 0;
			}
		}

		// free the memory being used by the file buffer
		delete[] buffer;
	}
}

int send_exit_signal()
{
	// Send EXIT signal
	string exit_string = "EXIT";
	for (int i=0; i< totalServers; i++)
	{
		write(SortServers[i], exit_string.c_str(), 4);
	}
}

int main(int argc, char* argv[])
{    
	time_t current_time;
	port = atoi(argv[2]);

	// Get a list of SortServer from servers.cfg and create socket connections
	initialize();
	hashBar = 65536 / totalServers;
	cout << "Total Servers: " << totalServers << "\n";
	cout << "Hash Bar: " << hashBar << "\n";	

	std::vector<std::string> files;
	// Check if argv[1] is a file or a folder
	if (is_directory(argv[1]))
	{
		DIR *dpdf;
		struct dirent *epdf;
		dpdf = opendir(argv[1]);
		char temp[1024];
		// Get a list of the files in the work folder, sorted by filename
		if (dpdf != NULL)
		{
			// Load data from all the files in the temporary directory
			while (epdf = readdir(dpdf))
			{
				if (epdf->d_type != DT_DIR)
				{
					sprintf(temp, "%s/%s", argv[1], epdf->d_name);
					std::string filename = temp;
					files.push_back(filename);
				}
			}
		}
	
	}
	// Otherwise check if argv[1] is a regular file
	else if (is_regular_file(argv[1]))
	{
		// Get a list of data files to send, this list is in a text file with the filename in argv[1]
		std::ifstream conf_file(argv[1]);
		char line[1024];
		// Send the data to SortServer, one file by one file
		while (conf_file.getline(line, 1024))
		{
			std::string filename = line;
			files.push_back(filename);
		}
	}
	
	std::make_heap(files.begin(), files.end());
	std::sort_heap(files.begin(), files.end());

	// Send files
	int count = files.size();
	for (int i=0; i<count; i++)
	{
		time(&current_time);
		cout << current_time << " File: " << files[i] << "\n";
		send_file(files[i].c_str());
	}
	
	// Send EXIT signal to SortServer
	send_exit_signal();
	time(&current_time);
	cout << current_time << " Done\n";
}


