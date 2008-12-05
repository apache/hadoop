/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.chukwa.extraction.demux.processor.reducer;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.chukwa.extraction.engine.Record;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

public class JobLogHistoryReduceProcessor implements ReduceProcessor
{
	static Logger log = Logger.getLogger(JobLogHistoryReduceProcessor.class);
	@Override
	public String getDataType()
	{
		return this.getClass().getName();
	}

	@Override
	public void process(ChukwaRecordKey key, 
						Iterator<ChukwaRecord> values,
						OutputCollector<ChukwaRecordKey, ChukwaRecord> output,
						Reporter reporter)
	{
		try
		{
			String action = key.getKey();
			int count = 0;
			
			ChukwaRecord record = null;
			while(values.hasNext())
			{
				record = values.next();
				if (record.containsField("START_TIME"))
				{
					count++;
				}
				else
				{
					count --;
				}
			}
			ChukwaRecordKey newKey = new ChukwaRecordKey();
			newKey.setKey(""+record.getTime());
			newKey.setReduceType("MSSRGraph");
			ChukwaRecord newRecord = new ChukwaRecord();
			newRecord.add(Record.tagsField, record.getValue(Record.tagsField));
			newRecord.setTime(record.getTime());
			newRecord.add("count", "" + count);
			newRecord.add("JOBID", record.getValue("JOBID"));
			if (action.indexOf("JobLogHist/Map/") >= 0)
			{
				newRecord.add("type", "MAP");
			}
			else if (action.indexOf("JobLogHist/SHUFFLE/") >= 0)
			{
				newRecord.add("type", "SHUFFLE");
			}
			else if (action.indexOf("JobLogHist/SORT/") >= 0)
			{
				newRecord.add("type", "SORT");
			}
			else if (action.indexOf("JobLogHist/REDUCE/") >= 0)
			{
				newRecord.add("type", "REDUCE");
			}
				
			output.collect(newKey, newRecord);
		} catch (IOException e)
		{
			log.warn("Unable to collect output in JobLogHistoryReduceProcessor [" + key + "]", e);
			e.printStackTrace();
		}

	}

}
