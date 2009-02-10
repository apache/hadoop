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
import java.util.HashMap;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

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
		sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		p = Pattern.compile(regex);
	}

	@Override
	protected void parse(String recordEntry, OutputCollector<ChukwaRecordKey, ChukwaRecord> output,
			Reporter reporter)
	 throws Throwable
	{
		
		log.debug("Top record: [" + recordEntry + "] type[" + chunk.getDataType() + "]");
		
		
		matcher=p.matcher(recordEntry);
		while (matcher.find())
		{
			log.debug("Top Processor Matches");
			
			try
			{
				Date d = sdf.parse( matcher.group(1).trim());

				ChukwaRecord record = new ChukwaRecord();
				String[] lines = recordEntry.split("\n");
				int i = 0;
				if(lines.length<2) {
					return;
				}
				String summaryString = "";
				while(!lines[i].equals("")) {
					summaryString = summaryString + lines[i] + "\n";
				    i++;
				}
				i++;
				record = new ChukwaRecord();
				key = new ChukwaRecordKey();
				parseSummary(record,summaryString);
				this.buildGenericRecord(record, null, d.getTime(), "SystemMetrics");
				output.collect(key, record);
				
				StringBuffer buffer = new StringBuffer();
				//FIXME please validate this
				while (i < lines.length) {
					record = null;
					buffer.append(lines[i]+"\n");
					i++;
					
				}
				record = new ChukwaRecord();
				key = new ChukwaRecordKey();
				this.buildGenericRecord(record, buffer.toString(), d.getTime(), "Top");
				//Output Top info to database
				output.collect(key, record);

				// End of parsing
			} catch (Exception e)
			{
				e.printStackTrace();
				throw e;
			}
		}
	}
	
	public void parseSummary(ChukwaRecord record,String header) {
		HashMap<String, Object> keyValues = new HashMap<String, Object>();
		String[] headers = header.split("\n");
		Pattern p = Pattern.compile("top - (.*?) up (.*?),\\s+(\\d+) users");
		Matcher matcher = p.matcher(headers[0]);
		if(matcher.find()) {
            record.add("uptime",matcher.group(2));
            record.add("users",matcher.group(3));
		}
		p = Pattern.compile("Tasks:\\s+(\\d+) total,\\s+(\\d+) running,\\s+(\\d+) sleeping,\\s+(\\d+) stopped,\\s+(\\d+) zombie");
		matcher = p.matcher(headers[1]);
		if(matcher.find()) {
			record.add("tasks_total",matcher.group(1));
			record.add("tasks_running",matcher.group(2));
			record.add("tasks_sleeping",matcher.group(3));
			record.add("tasks_stopped",matcher.group(4));
			record.add("tasks_zombie",matcher.group(5));
		}
		p = Pattern.compile("Cpu\\(s\\):\\s*(.*?)%\\s*us,\\s*(.*?)%\\s*sy,\\s*(.*?)%\\s*ni,\\s*(.*?)%\\s*id,\\s*(.*?)%\\s*wa,\\s*(.*?)%\\s*hi,\\s*(.*?)%\\s*si");
		matcher = p.matcher(headers[2]);
		if(matcher.find()) {
			record.add("cpu_user%",matcher.group(1));
			record.add("cpu_sys%",matcher.group(2));
			record.add("cpu_nice%",matcher.group(3));
			record.add("cpu_wait%",matcher.group(4));
			record.add("cpu_hi%",matcher.group(5));
			record.add("cpu_si%",matcher.group(6));
		}
		p = Pattern.compile("Mem:\\s+(.*?)k total,\\s+(.*?)k used,\\s+(.*?)k free,\\s+(.*?)k buffers");
		matcher = p.matcher(headers[3]);
		if(matcher.find()) {
			record.add("mem_total",matcher.group(1));
			record.add("mem_used",matcher.group(2));
			record.add("mem_free",matcher.group(3));
			record.add("mem_buffers",matcher.group(4));
		}
		p = Pattern.compile("Swap:\\s+(.*?)k total,\\s+(.*?)k used,\\s+(.*?)k free,\\s+(.*?)k cached");
		matcher = p.matcher(headers[4]);
		if(matcher.find()) {
			record.add("swap_total",matcher.group(1));
			record.add("swap_used",matcher.group(2));
			record.add("swap_free",matcher.group(3));
			record.add("swap_cached",matcher.group(4));
		}
		Iterator<String> ki = keyValues.keySet().iterator();
		while(ki.hasNext()) {
			String key = ki.next();
			log.debug(key+":"+keyValues.get(key));
		}
	}

	public String getDataType() {
		return recordType;
	}
}
