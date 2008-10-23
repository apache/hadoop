package org.apache.hadoop.chukwa;

import java.io.IOException;

import junit.framework.TestCase;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;

public class ChunkImplTest extends TestCase
{
	public void testVersion()
	{
		ChunkBuilder cb = new ChunkBuilder();
		cb.addRecord("foo".getBytes());
		cb.addRecord("bar".getBytes());
		cb.addRecord("baz".getBytes());
		Chunk c = cb.getChunk();
		DataOutputBuffer ob = new DataOutputBuffer(c.getSerializedSizeEstimate());
		try
		{
			c.write(ob);
			DataInputBuffer ib = new DataInputBuffer();
			ib.reset(ob.getData(), c.getSerializedSizeEstimate());
			int version = ib.readInt();
			assertEquals(version,ChunkImpl.PROTOCOL_VERSION);
		} 
		catch (IOException e)
		{
			e.printStackTrace();
			fail("Should nor raise any exception"); 
		}
	}

	public void testWrongVersion()
	{
		ChunkBuilder cb = new ChunkBuilder();
		cb.addRecord("foo".getBytes());
		cb.addRecord("bar".getBytes());
		cb.addRecord("baz".getBytes());
		Chunk c = cb.getChunk();
		DataOutputBuffer ob = new DataOutputBuffer(c.getSerializedSizeEstimate());
		try
		{
			c.write(ob);
			DataInputBuffer ib = new DataInputBuffer();
			ib.reset(ob.getData(), c.getSerializedSizeEstimate());
			//change current chunkImpl version
			ChunkImpl.PROTOCOL_VERSION = ChunkImpl.PROTOCOL_VERSION+1;
			ChunkImpl.read(ib);
			fail("Should have raised an IOexception"); 
		} 
		catch (IOException e)
		{
			// right behavior, do nothing
		}
	}
}
