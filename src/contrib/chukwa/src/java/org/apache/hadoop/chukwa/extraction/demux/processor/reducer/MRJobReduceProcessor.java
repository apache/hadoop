package org.apache.hadoop.chukwa.extraction.demux.processor.reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.chukwa.extraction.engine.Record;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

public class MRJobReduceProcessor implements ReduceProcessor
{
	static Logger log = Logger.getLogger(MRJobReduceProcessor.class);
	@Override
	public String getDataType()
	{
		return MRJobReduceProcessor.class.getName();
	}

	@Override
	public void process(ChukwaRecordKey key, Iterator<ChukwaRecord> values,
			OutputCollector<ChukwaRecordKey, ChukwaRecord> output,
			Reporter reporter)
	{
		try
		{
			HashMap<String, String> data = new HashMap<String, String>();
			
			ChukwaRecord record = null;
			String[] fields = null;
			while(values.hasNext())
			{
				record = values.next();
				fields = record.getFields();
				for(String field: fields)
				{
					data.put(field, record.getValue(field));
				}
			}
			
			//Extract initial time: SUBMIT_TIME
			long initTime = Long.parseLong(data.get("SUBMIT_TIME"));
			
			// Extract HodId
			// maybe use a regex to extract this and load it from configuration
			// JOBCONF="/user/xxx/mapredsystem/563976.xxx.yyy.com/job_200809062051_0001/job.xml"
			String jobConf = data.get("JOBCONF");
			int idx = jobConf.indexOf("mapredsystem/");
			idx += 13;
			int idx2 = jobConf.indexOf(".", idx);
			data.put("HodId", jobConf.substring(idx, idx2)); 
			
			ChukwaRecordKey newKey = new ChukwaRecordKey();
			newKey.setKey(""+initTime);
			newKey.setReduceType("MRJob");
			
			ChukwaRecord newRecord = new ChukwaRecord();
			newRecord.add(Record.tagsField, record.getValue(Record.tagsField));
			newRecord.setTime(initTime);
			newRecord.add(Record.tagsField, record.getValue(Record.tagsField));
			Iterator<String> it = data.keySet().iterator();
			while(it.hasNext())
			{
				String field = it.next();
				newRecord.add(field, data.get(field));
			}

			output.collect(newKey, newRecord);
		}
		catch (IOException e)
		{
			log.warn("Unable to collect output in JobLogHistoryReduceProcessor [" + key + "]", e);
			e.printStackTrace();
		}

	}

}
