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

public class DumpArchive
{

	/**
	 * @param args
	 * @throws URISyntaxException 
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException, URISyntaxException
	{
		System.out.println("Input file:" + args[0]);

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
				System.out.println("\nTimePartition: " + key.getTimePartition());
				System.out.println("DataType: " + key.getDataType());
				System.out.println("StreamName: " + key.getStreamName());
				System.out.println("SeqId: " + key.getSeqId());
				System.out.println("\t\t =============== ");
				
				System.out.println("Cluster : " + chunk.getTags());
				System.out.println("DataType : " + chunk.getDataType());
				System.out.println("Source : " + chunk.getSource());
				System.out.println("Application : " + chunk.getApplication());
				System.out.println("SeqID : " + chunk.getSeqID());
				System.out.println("Data : " + new String(chunk.getData()));
			}
		} 
		catch (Exception e)
		{
			e.printStackTrace();
		} 
	

	}

}
