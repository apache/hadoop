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

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;



public class HadoopLogProcessor extends AbstractProcessor
{
	static Logger log = Logger.getLogger(HadoopLogProcessor.class);
	
	private static final String recordType = "HadoopLog";
	private static final String nameNodeType = "NameNode";
	private static final String dataNodeType = "DataNode";
	private static final String auditType = "Audit";
	private SimpleDateFormat sdf = null;
	
	
	public HadoopLogProcessor()
	{
		//TODO move that to config
		sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
	}
	
	@Override
	public void parse(String recordEntry, OutputCollector<ChukwaRecordKey, ChukwaRecord> output,
			Reporter reporter)
	 throws Throwable
	{
			try
			{
				String dStr = recordEntry.substring(0, 23);
				Date d = sdf.parse(dStr);
				ChukwaRecord record = new ChukwaRecord();

				if (this.chunk.getStreamName().indexOf("datanode") > 0) {
					buildGenericRecord(record,recordEntry,d.getTime(),dataNodeType);
				} else if (this.chunk.getStreamName().indexOf("namenode") > 0) {
					buildGenericRecord(record,recordEntry,d.getTime(),nameNodeType);
				} else if (this.chunk.getStreamName().indexOf("audit") > 0) {
					buildGenericRecord(record,recordEntry,d.getTime(),auditType);
				} else {
					buildGenericRecord(record,recordEntry,d.getTime(),recordType);
				}
				
				
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

	public String getDataType()
	{
		return HadoopLogProcessor.recordType;
	}

}
