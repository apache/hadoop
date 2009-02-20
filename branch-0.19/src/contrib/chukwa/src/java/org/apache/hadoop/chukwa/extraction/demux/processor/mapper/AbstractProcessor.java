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

package org.apache.hadoop.chukwa.extraction.demux.processor.mapper;

import java.util.Calendar;

import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.Record;
import org.apache.hadoop.chukwa.util.RecordConstants;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public abstract class  AbstractProcessor implements ChunkProcessor
{
	Calendar calendar = Calendar.getInstance();
	Chunk chunk = null;
	byte[] bytes;
	int[] recordOffsets ;
	int currentPos = 0;
	int startOffset = 0;
	Text key = new Text();
	
	public AbstractProcessor()
	{}
	
	protected abstract void parse(String recordEntry, OutputCollector<Text, ChukwaRecord> output, Reporter reporter);
	
	
	public void process(Chunk chunk,OutputCollector<Text, ChukwaRecord> output, Reporter reporter)	{
		reset(chunk);
		while (hasNext()) {
			parse(nextLine(), output, reporter);
		}
	}
	
	
	protected void buildGenericRecord(ChukwaRecord record, String body,long timestamp,String dataSource)	{
		calendar.setTimeInMillis( timestamp);
		String fileName = dataSource + "/" + dataSource + new java.text.SimpleDateFormat("_yyyy_MM_dd_HH").format(calendar.getTime());
		int minutes = calendar.get(Calendar.MINUTE);
		int dec = minutes/10;
		fileName += "_" + dec ;
		
		int m = minutes - (dec*10);
		if (m < 5) { 
		  fileName += "0.evt";
		} else {
		  fileName += "5.evt";
		}

		record.setTime(timestamp);
		record.add(Record.rawField, body);
		record.add(Record.dataSourceField, dataSource);
		record.add(Record.destinationField, fileName);
		record.add(Record.sourceField, chunk.getSource());
		record.add(Record.streamNameField, chunk.getStreamName());
		record.add(Record.typeField, chunk.getDataType());
	}

	
	protected void reset(Chunk chunk)	{
		this.chunk = chunk;
		this.bytes = chunk.getData();
		this.recordOffsets = chunk.getRecordOffsets();
		currentPos = 0;
		startOffset = 0;
	}
	
	protected boolean hasNext() {
		return (currentPos < recordOffsets.length);
	}
	
	protected String nextLine()	{
		String log = new String(bytes,startOffset,(recordOffsets[currentPos]-startOffset));
		startOffset = recordOffsets[currentPos] + 1;
		currentPos ++;
		return RecordConstants.recoverRecordSeparators("\n", log);
	}
}
