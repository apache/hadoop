package org.apache.hadoop.chukwa.extraction.demux.processor.mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

public class TsProcessor extends AbstractProcessor
{
	static Logger log = Logger.getLogger(TsProcessor.class);
	private SimpleDateFormat sdf = null;
	
	public TsProcessor()
	{
		//TODO move that to config
		sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
	}

	@Override
	protected void parse(String recordEntry,
			OutputCollector<ChukwaRecordKey, ChukwaRecord> output,
			Reporter reporter)
	 throws Throwable
	{
		try
		{
			String dStr = recordEntry.substring(0, 23);
			Date d = sdf.parse(dStr);
			ChukwaRecord record = new ChukwaRecord();
			this.buildGenericRecord(record, recordEntry, d.getTime(), chunk.getDataType());
			output.collect(key, record);
		} 
		catch (ParseException e)
		{
			log.warn("Unable to parse the date in DefaultProcessor ["
					+ recordEntry + "]", e);
			e.printStackTrace();
			throw e;
		}
		catch (IOException e)
		{
			log.warn("Unable to collect output in DefaultProcessor ["
					+ recordEntry + "]", e);
			e.printStackTrace();
			throw e;
		}
			
	}

}
