package org.apache.hadoop.chukwa.extraction.demux.processor.mapper;

import java.io.IOException;

import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

public class DefaultProcessor extends AbstractProcessor
{
	static Logger log = Logger.getLogger(DefaultProcessor.class);
	@Override
	protected void parse(String recordEntry,
			OutputCollector<ChukwaRecordKey, ChukwaRecord> output,
			Reporter reporter)
	{
		try
		{
			ChukwaRecord record = new ChukwaRecord();
			this.buildGenericRecord(record, recordEntry, archiveKey.getTimePartition(), chunk.getDataType());
			output.collect(key, record);
		} 
		catch (IOException e)
		{
			log.warn("Unable to collect output in DefaultProcessor ["
					+ recordEntry + "]", e);
			e.printStackTrace();
		}
	}
}
