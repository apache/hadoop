package org.apache.hadoop.chukwa.extraction.database;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.chukwa.conf.ChukwaConfiguration;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class DatabaseLoader
{

	private static Log log = LogFactory.getLog(DatabaseLoader.class);

	/**
	 * @param args
	 * @throws URISyntaxException
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException,
			URISyntaxException
	{
		System.out.println("Input directory:" + args[0]);

		ChukwaConfiguration conf = new ChukwaConfiguration();
		String fsName = conf.get("writer.hdfs.filesystem");
		FileSystem fs = FileSystem.get(new URI(fsName), conf);

		Path srcDir = new Path(args[0]);
		FileStatus fstat = fs.getFileStatus(srcDir);

		if (!fstat.isDir())
		{
			throw new IOException(args[0] + " is not a directory!");
		} else
		{
			FileStatus[] datasourceDirectories = fs.listStatus(srcDir,new EventFileFilter());
			for (FileStatus datasourceDirectory : datasourceDirectories)
			{

				// rename working file
				String databaseInputFilename = datasourceDirectory.getPath()
						.getName();
				
				Path inProgressdatabaseInputFilePath = new Path(databaseInputFilename
						+ "." + System.currentTimeMillis() + ".pgs");
				
				// Maybe the file has already been processed by another loader
				if (fs.exists(datasourceDirectory.getPath()))
				{
					fs.rename(datasourceDirectory.getPath(), inProgressdatabaseInputFilePath);

					SequenceFile.Reader r = new SequenceFile.Reader(fs,datasourceDirectory.getPath(), conf);
					Text key = new Text();
					ChukwaRecord record = new ChukwaRecord();
					try
					{
						while (r.next(key, record))
						{
							System.out.println(record.getValue(DatabaseHelper.sqlField));
						} // End while(r.next(key, databaseRecord) )
					} // end Try
					catch (Exception e)
					{
						log.error("Unable to insert data into database"
								+ e.getMessage());
						e.printStackTrace();
					} 
					
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