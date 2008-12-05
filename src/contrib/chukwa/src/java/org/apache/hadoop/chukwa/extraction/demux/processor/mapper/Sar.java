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

public class Sar extends AbstractProcessor
{
	static Logger log = Logger.getLogger(Sar.class);
	public final String recordType = this.getClass().getName();

	private static String regex="([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2},[0-9]{3}) (.*?) (.*?): (.*?) \\((.*?)\\)";
	private static Pattern p = null;
	
	private Matcher matcher = null;
	private SimpleDateFormat sdf = null;

	public Sar()
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
		
		log.debug("Sar record: [" + recordEntry + "] type[" + chunk.getDataType() + "]");
		int i = 0;
		
//		String logLevel = null;
//		String className = null;
		
		matcher=p.matcher(recordEntry);
		while (matcher.find())
		{
			log.debug("Sar Processor Matches");
			
			try
			{
				Date d = sdf.parse( matcher.group(1).trim());
				
//				logLevel = matcher.group(2);
//				className = matcher.group(3);
				
				//TODO create a more specific key structure
				// part of ChukwaArchiveKey + record index if needed
				key.setKey("" + d.getTime());
				
				String[] lines = recordEntry.split("\n");
				
				
				String[] headers = null;
				while(i < (lines.length-1) && lines[i+1].indexOf("Average:")<0) {
					// Skip to the average lines
					log.debug("skip:"+lines[i]);
				    i++;
				}
				while (i < lines.length)
				{
					ChukwaRecord record = null;
					if(lines[i].equals("")) {
						i++;
						headers = parseHeader(lines[i]);
						i++;
					}
					String data[] = parseData(lines[i]);
					
					//FIXME please validate this
					if(headers[1].equals("IFACE") && headers[2].equals("rxpck/s")) {
						log.debug("Matched Sar-Network");
						
						record = new ChukwaRecord();
						key = new ChukwaRecordKey();
						this.buildGenericRecord(record, null,d.getTime(), "SystemMetrics");
					} else if(headers[1].equals("IFACE") && headers[2].equals("rxerr/s")) {
						log.debug("Matched Sar-Network");
						
						record = new ChukwaRecord();
						key = new ChukwaRecordKey();
						this.buildGenericRecord(record, null,d.getTime(), "SystemMetrics");
					} else if(headers[1].equals("kbmemfree")) {
						log.debug("Matched Sar-Memory");
						
						record = new ChukwaRecord();
						key = new ChukwaRecordKey();
						this.buildGenericRecord(record, null,d.getTime(), "SystemMetrics");
					} else if(headers[1].equals("totsck")) {
						log.debug("Matched Sar-NetworkSockets");
						
						record = new ChukwaRecord();
						key = new ChukwaRecordKey();
						this.buildGenericRecord(record, null,d.getTime(), "SystemMetrics");
					} else if(headers[1].equals("runq-sz")) {
						log.debug("Matched Sar-LoadAverage");
						
						record = new ChukwaRecord();
						key = new ChukwaRecordKey();
						this.buildGenericRecord(record, null,d.getTime(), "SystemMetrics");
					} else {
						log.debug("No match:"+headers[1]+" "+headers[2]);
					}
					if(record!=null) {
						int j=0;
						
						log.debug("Data Length: " + data.length);
	                    while(j<data.length) {
	                    	log.debug("header:"+headers[j]+" data:"+data[j]);
	                    	if(!headers[j].equals("Average:")) {
	                    		record.add(headers[j],data[j]);
	                    	}
						    j++;
	                    }			
	                   
						output.collect(key, record);
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