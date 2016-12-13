
/**
 *
 * A simple utility to record timestamp and system load.
 *
 */
 
import java.lang.management.*;
import javax.management.*;
import java.io.*;
import java.net.*;
import java.sql.*;
import java.util.*;

public class LoadMonitor extends Thread
{	
	String node_ip, test_id;
	Connection db_connection;
	Properties prop = new Properties();
	
	public LoadMonitor()
	{
		try
		{
			test_id = "Test";
			node_ip = "" + InetAddress.getLocalHost().getHostAddress();
			FileInputStream input = new FileInputStream("config.properties");
			prop.load(input);
			
			// Create a connection to database
			Class.forName("com.mysql.jdbc.Driver");
			String conn_string = "jdbc:mysql://" + prop.getProperty("db_hostname") + "/" + prop.getProperty("db_database") 
					+ "?user=" + prop.getProperty("db_username") + "&password=" + prop.getProperty("db_password");
			db_connection = DriverManager.getConnection(conn_string);
		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}				
	}
	
	
    public static String[] sysUsage (int seconds) throws Exception 
    {          
    	Runtime runtime = Runtime.getRuntime();
        BufferedReader reader = null;
        StringTokenizer st;
        String  command, infoLine, loadString = "0.00";
		String[] load = new String[8]; 	// num of threads, %usr, %sys, %iowait, read KB, write KB, eth0 tx bytes, eth0 rx bytes

        try 
        {
			// Concurrent threads
			OperatingSystemMXBean mbean = ManagementFactory.getOperatingSystemMXBean();
			load[0] = "" + mbean.getSystemLoadAverage();
        
			// Parsing CPU load
            command = "/bin/bash bin/cpustat.sh";
            reader = new BufferedReader(new InputStreamReader(runtime.exec(command).getInputStream()));
            infoLine = reader.readLine().trim();
            st = new StringTokenizer(infoLine, " ");
            load[1] = st.nextToken();
            load[2] = st.nextToken();
            load[3] = st.nextToken();
			
			// Parsing disk read and write in KB
            command = "/bin/bash bin/diskstat.sh";
            reader = new BufferedReader(new InputStreamReader(runtime.exec(command).getInputStream()));
            infoLine = reader.readLine().trim();
            st = new StringTokenizer(infoLine, " ");
            load[4] = st.nextToken();
            load[5] = st.nextToken();
				        
	        // Pasing eth0 RX and TX
	        String line;
			InputStream fis = new FileInputStream("/sys/class/net/eth0/statistics/rx_bytes");
			InputStreamReader isr = new InputStreamReader(fis);
			BufferedReader br = new BufferedReader(isr);
			if ((line = br.readLine()) != null) 
			{
				load[6] = line;
			}
			fis.close();

			fis = new FileInputStream("/sys/class/net/eth0/statistics/tx_bytes");
			isr = new InputStreamReader(fis);
			br = new BufferedReader(isr);
			if ((line = br.readLine()) != null) 
			{
				load[7] = line;
			}
			fis.close();

        } catch (Exception e)
        {
	        System.out.println(e.getMessage());
	        e.printStackTrace();
        }    
        
        return load;
    }

	
	public void log_stats(String[] load)
	{
		try
		{
			// Assemble SQL statement
			String sql = "INSERT INTO node_stats (test_id, node, threads, cpu_usr, cpu_sys, cpu_iowait, disk_read_kb, disk_write_kb, net_tx_b, net_rx_b) VALUES ("
				+ "'" + test_id + "', " + "'" + node_ip + "', " 
				+ load[0] + ", " + load[1] + ", " + load[2] + ", " + load[3] + ", " 
				+ load[4] + ", " + load[5] + ", " + load[6] + ", " + load[7] + ")";
				
			Statement statement = db_connection.createStatement();
			statement.executeUpdate(sql);			
		} catch (Exception e)
		{
	        System.out.println(e.getMessage());
	        e.printStackTrace();			
		}
	}
	
	
	public void run()
	{
		int	   period = 1; // default 1 second
		int    sleep = 1000;
		long   unixTime;
		String[] load;
		
		while (true)
		{
			try
			{
				unixTime = System.currentTimeMillis() / 1000L;
				load = LoadMonitor.sysUsage(period);
				log_stats(load);
//				Thread.sleep(sleep);
			} catch (Exception e)
			{
				System.out.println(e.getMessage());
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String[] args)
	{
		LoadMonitor lm = new LoadMonitor();
		lm.start();
	}
	
}
