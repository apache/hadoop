package org.apache.hadoop.chukwa.validationframework.interceptor;

import java.util.List;

import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.datacollection.ChunkQueue;

public class ChunkQueueInterceptor implements
		org.apache.hadoop.chukwa.datacollection.ChunkQueue
{
	private ChunkQueue defaultQueue = null;
	
	public ChunkQueueInterceptor(ChunkQueue defaultQueue)
	{
		this.defaultQueue = defaultQueue;
	}

	@Override
	public void add(Chunk chunk) throws InterruptedException
	{
		ChunkDumper.dump("adaptor", chunk);
		defaultQueue.add(chunk);
	}

	@Override
	public void collect(List<Chunk> chunks, int count)
			throws InterruptedException
	{
		defaultQueue.collect(chunks, count);
		for(Chunk chunk: chunks)
			{ ChunkDumper.dump("sender", chunk);}
	}

	@Override
	public int size()
	{
		return defaultQueue.size();
	}

}
