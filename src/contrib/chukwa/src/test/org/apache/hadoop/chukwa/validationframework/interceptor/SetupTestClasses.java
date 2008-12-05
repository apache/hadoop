package org.apache.hadoop.chukwa.validationframework.interceptor;

import java.lang.reflect.Field;

import org.apache.hadoop.chukwa.datacollection.ChunkQueue;
import org.apache.hadoop.chukwa.datacollection.DataFactory;

public class SetupTestClasses
{
	public static void setupClasses() throws Throwable
	{
		setupChunkQueueInterceptor();
	}
	
	static protected void setupChunkQueueInterceptor() throws Throwable
	{
		DataFactory da = DataFactory.getInstance();
		ChunkQueue chunkQueue = da.getEventQueue();
		
		final Field fields[] = DataFactory.class.getDeclaredFields();
	    for (int i = 0; i < fields.length; ++i) 
	    {
	      if ("chunkQueue".equals(fields[i].getName())) 
	      {
	        Field f = fields[i];
	        f.setAccessible(true);
	        ChunkQueue ci = new ChunkQueueInterceptor(chunkQueue);
	        f.set(da, ci);
	        System.out.println("Adding QueueInterceptor");
	        break;
	      }
	    }
	}
}
