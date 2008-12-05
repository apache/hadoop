package org.apache.hadoop.chukwa.extraction.demux.processor.reducer;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class IdentityReducer implements ReduceProcessor
{

	@Override
	public String getDataType()
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void process(ChukwaRecordKey key, Iterator<ChukwaRecord> values,
			OutputCollector<ChukwaRecordKey, ChukwaRecord> output,
			Reporter reporter)
	{
		while(values.hasNext())
		{
			try
			{
				output.collect(key, values.next());
			} 
			catch (IOException e)
			{
				e.printStackTrace();
			}
		}
	}

}
