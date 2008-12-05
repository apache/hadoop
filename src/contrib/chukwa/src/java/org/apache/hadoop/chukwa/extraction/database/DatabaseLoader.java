package org.apache.hadoop.chukwa.extraction.database;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.chukwa.conf.ChukwaConfiguration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class DatabaseLoader
{

	static HashMap<String, String> hashDatasources = new HashMap<String, String>();
	static ChukwaConfiguration conf = null;
	static FileSystem fs = null;
	
	private static Log log = LogFactory.getLog(DatabaseLoader.class);

	/**
	 * @param args
	 * @throws URISyntaxException
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException,
			URISyntaxException
	{
		//FIXME quick implementation to be able to load data into database
		
		System.out.println("Input directory:" + args[0]);

		
		for(int i=1;i<args.length;i++)
		{
			hashDatasources.put(args[i], "");
		}
		
		conf = new ChukwaConfiguration();
		fs = FileSystem.get(conf);
		Path demuxDir = new Path(args[0]);
		FileStatus fstat = fs.getFileStatus(demuxDir);

		if (!fstat.isDir())
		{
			throw new IOException(args[0] + " is not a directory!");
		} 
		else
		{
			// cluster Directory
			FileStatus[] clusterDirectories = fs.listStatus(demuxDir);
			for (FileStatus clusterDirectory : clusterDirectories)
			{
				FileStatus[] datasourceDirectories = fs.listStatus(clusterDirectory.getPath());
				
				String directoryName = null;
				for (FileStatus datasourceDirectory : datasourceDirectories)
				{
					directoryName = datasourceDirectory.getPath().getName();
					if (directoryName.equals("_log") || (!hashDatasources.containsKey(directoryName)))
					{
						log.info("Skipping this directory:" + directoryName );
						continue;
					}
					try
					{
						processDS(clusterDirectory.getPath().getName(),datasourceDirectory.getPath());
					}
					catch(Exception e)
					{
						e.printStackTrace();
						log.warn("Exception in DatabaseLoader:" ,e);
					}
				}
			}
			
			System.exit(0);
		}
	}
		
	static void processDS(String cluster, Path datasourcePath) throws IOException
	{
		Path srcDir = datasourcePath;
		FileStatus fstat = fs.getFileStatus(srcDir);

		if (!fstat.isDir())
		{
			throw new IOException(datasourcePath.getName() + " is not a directory!");
		} else
		{
			FileStatus[] datasourceDirectories = fs.listStatus(srcDir,new EventFileFilter());
			for (FileStatus datasourceDirectory : datasourceDirectories)
			{
				String dataSource = datasourceDirectory.getPath().getName();
				dataSource = dataSource.substring(0,dataSource.indexOf('_'));
				
				
				// Need to rename if we want todo some processing in para.
				//
				// Maybe the file has already been processed by another loader
				if (fs.exists(datasourceDirectory.getPath()))
				{
					
					log.info("Processing: " + datasourceDirectory.getPath().getName());
					
					try
					{
						MetricDataLoader mdl = new MetricDataLoader(cluster);
						mdl.process(datasourceDirectory.getPath());
					} catch (SQLException e)
					{
						e.printStackTrace();
						log.warn("SQLException in MetricDataLoader:" ,e);
					} catch (URISyntaxException e)
					{
						e.printStackTrace();
						log.warn("Exception in MetricDataLoader:" ,e);
					}
					
					log.info("Processed: " + datasourceDirectory.getPath().getName());
				}
			} // End for(FileStatus datasourceDirectory :datasourceDirectories)
		} // End Else
	}
}

class EventFileFilter implements PathFilter
{
	  public boolean accept(Path path) 
	  {
	    return (path.toString().endsWith(".evt"));
	  }
}
