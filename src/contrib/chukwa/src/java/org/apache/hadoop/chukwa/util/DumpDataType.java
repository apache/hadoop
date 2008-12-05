package org.apache.hadoop.chukwa.util;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.chukwa.ChukwaArchiveKey;
import org.apache.hadoop.chukwa.ChunkImpl;
import org.apache.hadoop.chukwa.conf.ChukwaConfiguration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;

public class DumpDataType
{

	/**
	 * @param args
	 * @throws URISyntaxException 
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException, URISyntaxException
	{
		System.err.println("Input file:" + args[0]);
		System.err.println("DataType:" + args[1]);
		System.err.println("Source:" + args[2]);
		
		ChukwaConfiguration conf = new ChukwaConfiguration();
		String fsName = conf.get("writer.hdfs.filesystem");
		FileSystem fs = FileSystem.get(new URI(fsName), conf);

		SequenceFile.Reader r = 
			new SequenceFile.Reader(fs,new Path(args[0]), conf);
		
		ChukwaArchiveKey key = new ChukwaArchiveKey();
		ChunkImpl chunk = ChunkImpl.getBlankChunk();
		try
		{
			while (r.next(key, chunk))
			{
				if (args[1].equalsIgnoreCase(chunk.getDataType()))
				{
					if (args[2].equalsIgnoreCase("ALL") || args[2].equalsIgnoreCase(chunk.getSource()))
					{
						System.out.print(new String(chunk.getData()));
					}	
				}
				
			}
		} 
		catch (Exception e)
		{
			e.printStackTrace();
		} 
	

	}

}
