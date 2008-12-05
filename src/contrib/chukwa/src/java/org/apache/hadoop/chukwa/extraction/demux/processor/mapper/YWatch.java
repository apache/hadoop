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
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

public class YWatch extends AbstractProcessor
{
	static Logger log = Logger.getLogger(YWatch.class);

	private static final String ywatchType = "YWatch";
	
	private static String regex= null;
	
	private static Pattern p = null;
	
	private Matcher matcher = null;

	public YWatch()
	{
		//TODO move that to config
		regex="([0-9]{4}\\-[0-9]{2}\\-[0-9]{2} [0-9]{2}\\:[0-9]{2}:[0-9]{2},[0-9]{3}) (INFO|DEBUG|ERROR|WARN) (.*?): (.*)";
		p = Pattern.compile(regex);
		matcher = p.matcher("-");
	}

	@SuppressWarnings("unchecked")
	@Override
	protected void parse(String recordEntry, OutputCollector<ChukwaRecordKey, ChukwaRecord> output,
			Reporter reporter)
	 throws Throwable
	{
		if (log.isDebugEnabled())
		{
			log.debug("YWatchProcessor record: [" + recordEntry + "] type[" + chunk.getDataType() + "]");
		}
		
		matcher.reset(recordEntry);
		if (matcher.matches())
		{
			log.info("YWatchProcessor Matches");
			
			try
			{
				String body = matcher.group(4);
				
				try
				{
					JSONObject json = new JSONObject(body);

					String poller = json.getString("poller");
					String host = json.getString("host");
					String metricName = json.getString("metricName");
					
					// Data
					JSONObject jsonData = json.getJSONObject("data").getJSONObject("data");
		
					String jsonTs = null;
					long ts = Long.parseLong(jsonTs);
					
					String jsonValue = null;
					Iterator<String> it = jsonData.keys();
					
					ChukwaRecord record = null;
					
					while(it.hasNext())
					{
						jsonTs = it.next();
						jsonValue = jsonData.getString(jsonTs);
						
						record = new ChukwaRecord();
						key = new ChukwaRecordKey();
						this.buildGenericRecord(record, null, ts, "Ywatch");
						record.add("poller", poller);
						record.add("host", host);
						record.add("metricName", metricName);
						record.add("value", jsonValue);
						output.collect(key, record);
						log.info("YWatchProcessor output 1 metric");
					}
					
				} 
				catch (IOException e)
				{
					log.warn("Unable to collect output in YWatchProcessor [" + recordEntry + "]", e);
					e.printStackTrace();
				}
				catch (JSONException e)
				{
					e.printStackTrace();
					log.warn("Wrong format in YWatchProcessor [" + recordEntry + "]", e);
				}
				
			}
			catch(Exception e)
			{
				e.printStackTrace();
				throw e;
			}
		}
	}

	public String getDataType()
	{
		return YWatch.ywatchType;
	}
}
