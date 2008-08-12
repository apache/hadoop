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

import org.apache.hadoop.chukwa.extraction.database.DatabaseHelper;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.Record;
import org.apache.hadoop.io.Text;
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
		sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
		p = Pattern.compile(regex);
	}

	@Override
	protected void parse(String recordEntry, OutputCollector<Text, ChukwaRecord> output,
			Reporter reporter)
	{
		
		log.debug("Iostat record: [" + recordEntry + "] type[" + chunk.getDataType() + "]");
		StringBuilder sb = new StringBuilder(); 	 
		int i = 0;
		
		String logLevel = null;
		String className = null;
		String body = null;
		
		matcher=p.matcher(recordEntry);
		while (matcher.find())
		{
			log.debug("Iostat Processor Matches");
			
			try
			{
				Date d = sdf.parse( matcher.group(1).trim());
				
				logLevel = matcher.group(2);
				className = matcher.group(3);
				String hostname = matcher.group(5);
				
				//TODO create a more specific key structure
				// part of ChukwaArchiveKey + record index if needed
				key.set("" + d.getTime());
				
				String[] lines = recordEntry.split("\n");
				int skip=0;
				i++;
				String[] headers = null;
				while (skip<2 && i < lines.length) {
					// Skip the first output because the numbers are averaged from system boot up
					if(lines[i].indexOf("avg-cpu:")>0) {
						skip++;
					}
					i++;					
				}
				while (i < lines.length)
				{
					DatabaseHelper databaseRecord = null;
					if(lines[i].equals("")) {
						i++;
						headers = parseHeader(lines[i]);
						i++;
					}
					String data[] = parseData(lines[i]);
					if(headers[0].equals("avg-cpu:")) {
						log.debug("Matched CPU-Utilization");
						databaseRecord = new DatabaseHelper("system");
					} else if(headers[0].equals("Device:")) {
						log.debug("Matched Iostat");
						databaseRecord = new DatabaseHelper("system");	
					} else {
						log.debug("No match:"+headers[0]);
					}
					if(databaseRecord!=null) {
						int j=0;
						log.debug("Data Length: " + data.length);
	                    while(j<data.length) {
	                    	log.debug("header:"+headers[j]+" data:"+data[j]);
	                    	if(!headers[j].equals("avg-cpu:")) {
						        databaseRecord.add(d.getTime(),headers[j],data[j]);
	                    	}
						    j++;
	                    }						
						//Output Sar info to database
						output.collect(key, databaseRecord.buildChukwaRecord());
					}
					i++;
				}
				// End of parsing
			} catch (Exception e)
			{
				e.printStackTrace();
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