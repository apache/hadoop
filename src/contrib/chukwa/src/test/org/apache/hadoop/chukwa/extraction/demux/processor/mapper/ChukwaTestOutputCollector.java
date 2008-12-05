package org.apache.hadoop.chukwa.extraction.demux.processor.mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.mapred.OutputCollector;

public class ChukwaTestOutputCollector<K,V> implements OutputCollector<K,V>
{
	public HashMap<K, V> data = new HashMap<K, V>();
	
	public void collect(K key, V value) throws IOException
	{
		data.put(key, value);
	}

	@Override
	public String toString()
	{
		Iterator<K> it = data.keySet().iterator();
		K key = null;
		V value = null;
		StringBuilder sb = new StringBuilder();
		
		while(it.hasNext())
		{
			key = it.next();
			value = data.get(key);
			sb.append("Key[").append(key).append("] value[").append(value).append("]\n");
		}
		return sb.toString();
	}

	
}
