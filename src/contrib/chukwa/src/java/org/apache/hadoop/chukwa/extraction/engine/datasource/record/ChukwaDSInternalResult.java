package org.apache.hadoop.chukwa.extraction.engine.datasource.record;

import java.util.List;

import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.chukwa.extraction.engine.Record;

public class ChukwaDSInternalResult
{
	List<Record> records = null;
	String day = null;
	int hour = 0;
	int rawIndex = 0;
	int spill = 1;
	long position = -1;
	long currentTs = -1;
	
	String fileName = null;
	
	ChukwaRecordKey key = null;
}
