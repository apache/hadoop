package org.apache.hadoop.chukwa.validationframework.interceptor;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.chukwa.Chunk;

public class ChunkDumper
{
	static public String testRepositoryDumpDir = "/tmp/chukwaDump/";
	static HashMap<String, DataOutputStream> hash = new HashMap<String, DataOutputStream>();
	
	public static void dump(String component,Chunk chunk)
	{
		
		String fileName = chunk.getApplication();
		
		if (!hash.containsKey(component + "-" +fileName))
		{
			File directory = new File(testRepositoryDumpDir+ "/"+ component );
			if (!directory.exists())
			{
				directory.mkdirs();
			}
			String name = fileName;
			if (fileName.indexOf("/") >= 0)
			{
				name = fileName.substring(fileName.lastIndexOf("/"));
			}
			name += ".bin";
			
			synchronized(name.intern())
			{
				System.out.println("FileName [" + name + "]");
				try
				{
					DataOutputStream  dos=new DataOutputStream(new FileOutputStream(new File(testRepositoryDumpDir+ "/"+ component + "/" + name)));
					System.out.println("Writing to [" + testRepositoryDumpDir+ "/"+ component + "/" + name + "]");
					hash.put(component + "-" +fileName, dos);
				} catch (FileNotFoundException e)
				{
					e.printStackTrace();
				}   
			}
		}
		String key = component + "-" +fileName;
		synchronized(key.intern())
		{
			DataOutputStream dos = hash.get(key);
			try
			{
				chunk.write(dos);
				dos.flush();
			}
			catch (IOException e)
			{
				e.printStackTrace();
			}
		}
	}
	
	static void close()
	{
		Iterator<String> it = hash.keySet().iterator();
		while(it.hasNext())
		{
			String key = it.next();
			DataOutputStream dos = hash.get(key);
			try{ dos.close();} 
			catch (Exception e)
			{ e.printStackTrace();}
		}
	}
}
