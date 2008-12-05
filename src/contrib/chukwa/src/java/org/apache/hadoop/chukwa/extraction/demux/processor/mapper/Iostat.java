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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

public class Iostat extends AbstractProcessor
{
	static Logger log = Logger.getLogger(Iostat.class);
	public final String recordType = this.getClass().getName();
	
	private static String regex="([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2},[0-9]{3}) (.*?) (.*?): (.*?) \\((.*?)\\)";
	private static Pattern p = null;
	
	private Matcher matcher = null;
	private SimpleDateFormat sdf = null;

	public Iostat()
	{
		//TODO move that to config
		sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		p = Pattern.compile(regex);
	}

	@Override
	protected void parse(String recordEntry, OutputCollector<ChukwaRecordKey, ChukwaRecord> output,
			Reporter reporter)
  throws Throwable
	{
		
		log.debug("Iostat record: [" + recordEntry + "] type[" + chunk.getDataType() + "]");
		int i = 0;

		matcher=p.matcher(recordEntry);
		while (matcher.find())
		{
			log.debug("Iostat Processor Matches");
			
			try
			{
				Date d = sdf.parse( matcher.group(1).trim());
				

				
				String[] lines = recordEntry.split("\n");
				String[] headers = null;
				for(int skip=0;skip<2;skip++) {
				    i++;
					while ( i < lines.length && lines[i].indexOf("avg-cpu")<0) {
					    // Skip the first output because the numbers are averaged from system boot up
					    log.debug("skip line:"+lines[i]);
					    i++;					
				    }
				}
				while (i < lines.length)
				{
					ChukwaRecord record = null;
					
					if(lines[i].indexOf("avg-cpu")>=0 || lines[i].indexOf("Device")>=0) {
						headers = parseHeader(lines[i]);
						i++;
					}
					String data[] = parseData(lines[i]);
					if(headers[0].equals("avg-cpu:")) {
						log.debug("Matched CPU-Utilization");
						record = new ChukwaRecord();
					  key = new ChukwaRecordKey();
					  buildGenericRecord(record, null, d.getTime(), "SystemMetrics");
					} else if(headers[0].equals("Device:")) {
						log.debug("Matched Iostat");
						record = new ChukwaRecord();
						key = new ChukwaRecordKey();
					  buildGenericRecord(record, null, d.getTime(), "SystemMetrics");
					} else {
						log.debug("No match:"+headers[0]);
					}
					if(record!=null) {
						int j=0;
						log.debug("Data Length: " + data.length);
			            while(j<data.length) {
			            	log.debug("header:"+headers[j]+" data:"+data[j]);
			            	if(!headers[j].equals("avg-cpu:")) {
			            		record.add(headers[j],data[j]);
			            	}
						  j++;
			            }
			            record.setTime(d.getTime());
			            if(data.length>3) {
						    output.collect(key, record);
			            }
					}
					i++;
				}
				// End of parsing
			} catch (Exception e)
			{
				e.printStackTrace();
				throw e;
			}
		}
	}
	
	public String[] parseHeader(String header) {
		String[] headers = header.split("\\s+");
		return headers;
	}

	public String[] parseData(String dataLine) {
		String[] data = dataLine.split("\\s+");
		return data;
	}

	public String getDataType() {
		return recordType;
	}
}