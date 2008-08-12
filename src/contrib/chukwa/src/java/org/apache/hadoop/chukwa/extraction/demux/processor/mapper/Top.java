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
import java.util.Set;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.chukwa.extraction.database.DatabaseHelper;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.Record;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;
import java.util.HashMap;

public class Top extends AbstractProcessor
{
	static Logger log = Logger.getLogger(Top.class);
	public final String recordType = this.getClass().getName();

	private static String regex="([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2},[0-9]{3}) (.*?) (.*?): ";
	private static Pattern p = null;
	
	private Matcher matcher = null;
	private SimpleDateFormat sdf = null;

	public Top()
	{
		//TODO move that to config
		sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
		p = Pattern.compile(regex);
	}

	@Override
	protected void parse(String recordEntry, OutputCollector<Text, ChukwaRecord> output,
			Reporter reporter)
	{
		
		log.info("Top record: [" + recordEntry + "] type[" + chunk.getDataType() + "]");
		StringBuilder sb = new StringBuilder(); 	 
		
		String logLevel = null;
		String className = null;
		String body = null;
		
		matcher=p.matcher(recordEntry);
		while (matcher.find())
		{
			log.info("Top Processor Matches");
			
			try
			{
				Date d = sdf.parse( matcher.group(1).trim());
				
				logLevel = matcher.group(2);
				className = matcher.group(3);
				
				//TODO create a more specific key structure
				// part of ChukwaArchiveKey + record index if needed
				key.set("" + d.getTime());
				String[] lines = recordEntry.split("\n");
				int i = 0;
				String summaryString = "";
				while(!lines[i].equals("")) {
					summaryString = summaryString + lines[i] + "\n";
				    i++;
				}
				i++;
				String[] headers = lines[i].split("\\s+");
				HashMap<String, String>summary = parseSummary(summaryString);
				DatabaseHelper databaseRecord = new DatabaseHelper("system");
				Iterator<String> ki = summary.keySet().iterator();
				while(ki.hasNext()) {
					String key = ki.next();
				    databaseRecord.add(d.getTime(),key, summary.get(key));
				}
				output.collect(key, databaseRecord.buildChukwaRecord());
				while (i < lines.length)
				{
					databaseRecord = null;
					String data[] = lines[i].split("\\s+",headers.length);
					if(lines[i].indexOf("PID USER")<0) {
						databaseRecord = new DatabaseHelper("system");	
					}
					if(databaseRecord!=null) {
						int j=0;
						log.debug("Data Length: " + data.length);
	                    while(j<data.length-1) {
	                    	if(headers[0].equals("")) {
		                    	log.debug("header:"+headers[j+1]+" data:"+data[j+1]);
	                    		databaseRecord.add(d.getTime(),headers[j+1],data[j+1]);
	                    	} else {
		                    	log.debug("header:"+headers[j+1]+" data:"+data[j]);
							    databaseRecord.add(d.getTime(),headers[j+1],data[j]);
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
	
	public HashMap<String, String> parseSummary(String header) {
		HashMap<String, String> keyValues = new HashMap<String, String>();
		String[] headers = header.split("\n");
		int i=0;
		Pattern p = Pattern.compile("top - (.*?) up (.*?),\\s+(\\d+) users");
		Matcher matcher = p.matcher(headers[0]);
		if(matcher.find()) {
            keyValues.put("uptime",matcher.group(2));
            keyValues.put("users",matcher.group(3));
		}
		p = Pattern.compile("Tasks:\\s+(\\d+) total,\\s+(\\d+) running,\\s+(\\d+) sleeping,\\s+(\\d+) stopped,\\s+(\\d+) zombie");
		matcher = p.matcher(headers[1]);
		if(matcher.find()) {
            keyValues.put("tasks_total",matcher.group(1));
            keyValues.put("tasks_running",matcher.group(2));
            keyValues.put("tasks_sleeping",matcher.group(3));
            keyValues.put("tasks_stopped",matcher.group(4));
            keyValues.put("tasks_zombie",matcher.group(5));
		}
		p = Pattern.compile("Cpu\\(s\\):\\s+(.*?)% us,\\s+(.*?)% sy,\\s+(.*?)% ni,\\s+(.*?)% id,\\s+(.*?)% wa,\\s+(.*?)% hi,\\s+(.*?)% si");
		matcher = p.matcher(headers[2]);
		if(matcher.find()) {
            keyValues.put("cpu_user%",matcher.group(1));
            keyValues.put("cpu_sys%",matcher.group(2));
            keyValues.put("cpu_nice%",matcher.group(3));
            keyValues.put("cpu_wait%",matcher.group(4));
            keyValues.put("cpu_hi%",matcher.group(5));
            keyValues.put("cpu_si%",matcher.group(6));
		}
		p = Pattern.compile("Mem:\\s+(.*?)k total,\\s+(.*?)k used,\\s+(.*?)k free,\\s+(.*?)k buffers");
		matcher = p.matcher(headers[3]);
		if(matcher.find()) {
			keyValues.put("mem_total",matcher.group(1));
			keyValues.put("mem_used",matcher.group(2));
			keyValues.put("mem_free",matcher.group(3));
			keyValues.put("mem_buffers",matcher.group(4));
		}
		p = Pattern.compile("Swap:\\s+(.*?)k total,\\s+(.*?)k used,\\s+(.*?)k free,\\s+(.*?)k cached");
		matcher = p.matcher(headers[4]);
		if(matcher.find()) {
			keyValues.put("swap_total",matcher.group(1));
			keyValues.put("swap_used",matcher.group(2));
			keyValues.put("swap_free",matcher.group(3));
			keyValues.put("swap_cached",matcher.group(4));
		}
		Iterator<String> ki = keyValues.keySet().iterator();
		while(ki.hasNext()) {
			String key = ki.next();
			log.info(key+":"+keyValues.get(key));
		}
		return keyValues;
	}

	public String getDataType() {
		return recordType;
	}
}