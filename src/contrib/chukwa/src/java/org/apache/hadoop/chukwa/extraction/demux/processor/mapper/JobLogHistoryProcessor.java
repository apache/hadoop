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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.Record;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

public class JobLogHistoryProcessor extends AbstractProcessor
{
	static Logger log = Logger.getLogger(JobLogHistoryProcessor.class);

	private static final String recordType = "JobLogHistory";
	private static final String jobEntryTypeField = "entryType";
	
	private static String regex = null;
	private static String internalRegex = null;
	private static Pattern p = null;
	private static Pattern ip = null;
	
	private Matcher matcher = null;
	private Matcher internalMatcher = null;
	
	private SimpleDateFormat sdf = null;

	public JobLogHistoryProcessor()
	{
		sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
		
		internalRegex = "(.*?)=\"(.*?)\"(.*)";
		regex = "([0-9]{4}\\-[0-9]{2}\\-[0-9]{2} [0-9]{2}\\:[0-9]{2}:[0-9]{2},[0-9]{3}) (INFO|DEBUG|ERROR|WARN) (.*?): (.*)";
		
		ip = Pattern.compile(internalRegex);
		p = Pattern.compile(regex);
		
		internalMatcher = ip.matcher("-");
		matcher = p.matcher("-");
	}

	@Override
	protected void parse(String recordEntry,
			OutputCollector<Text, ChukwaRecord> output, Reporter reporter)
	{

		String logLevel = null;
		String className = null;
		String body = null;

		log.info("JobLogHistoryProcessor record: [" + recordEntry + "] type["
				+ chunk.getDataType() + "]");
		
		
		matcher.reset(recordEntry);
		if (matcher.matches())
		{
			log.info("JobLogHistoryProcessor Matches");

			try
			{
				Date d = sdf.parse(matcher.group(1).trim());
				ChukwaRecord record = new ChukwaRecord();

				logLevel = matcher.group(2);
				className = matcher.group(3);
				body = matcher.group(4);

				key.set("" + d.getTime());
				
				record.add(Record.logLevelField, logLevel);
				record.add(Record.classField, className);
				
				int firstSep = body.indexOf(" ");
				String logEntryType = body.substring(0,firstSep);
				record.add(jobEntryTypeField, logEntryType);
				
				internalMatcher.reset(body.substring(firstSep));
				String fieldName = null;
				String fieldValue = null;
				while (internalMatcher.matches())
				{
					fieldName = internalMatcher.group(1).trim();
					fieldValue = internalMatcher.group(2).trim();
					record.add(fieldName, fieldValue);
					log.info("JobLogHistoryProcessor Add field: [" + fieldName + "][" + fieldValue +"]" );
					internalMatcher.reset(internalMatcher.group(3));
				}

				buildGenericRecord(record, body, d.getTime(), JobLogHistoryProcessor.recordType);
				output.collect(key, record);
				log.info("JobLogHistoryProcessor outputing a record ============>>>>>");
			} 
			catch (IOException e)
			{
				e.printStackTrace();
			} 
			catch (ParseException e)
			{
				e.printStackTrace();
			}

		}
	}

	public String getDataType()
	{
		return JobLogHistoryProcessor.recordType;
	}

	public static void main(String[] args)
	{
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
		
		internalRegex = "(.*?)=\"(.*?)\"(.*)";
		regex = "([0-9]{4}\\-[0-9]{2}\\-[0-9]{2} [0-9]{2}\\:[0-9]{2}:[0-9]{2},[0-9]{3}) (INFO|DEBUG|ERROR|WARN) (.*?): (.*)";
		
		ip = Pattern.compile(internalRegex);
		p = Pattern.compile(regex);
		
		Matcher internalMatcher = ip.matcher("-");
		Matcher matcher = p.matcher("-");
	
		String log = "2008-07-28 23:30:38,865 INFO org.apache.hadoop.chukwa.ChukwaJobHistory: Task TASKID=\"task_200807282329_0001_m_000000\" TASK_TYPE=\"MAP\" START_TIME=\"1217287838862\" SPLITS=\"/default-rack/somehost3.example.com,/default-rack/somehost2.example.com,/default-rack/somehost.example.com\"";
		 matcher.reset(log);
		
		if (matcher.matches())
		{
			System.out.println("JobLogHistoryProcessor Matches");

			try
			{
				Date d = sdf.parse(matcher.group(1).trim());
				System.out.println(d);
				ChukwaRecord record = new ChukwaRecord();

				String logLevel = matcher.group(2);
				String className = matcher.group(3);
				String body = matcher.group(4);

				System.out.println(matcher.group(1));
				System.out.println(matcher.group(2));
				System.out.println(matcher.group(3));
				System.out.println(matcher.group(4));
				
				System.out.println(body); 
				
				record.add(Record.logLevelField, logLevel);
				record.add(Record.classField, className);
				
				int firstSep = body.indexOf(" ");
				System.out.println(firstSep);
				String logEntryType = body.substring(0,firstSep);
				record.add(jobEntryTypeField, logEntryType);
				
				internalMatcher.reset(body.substring(firstSep));
				String fieldName = null;
				String fieldValue = null;
				while (internalMatcher.matches())
				{
					fieldName = internalMatcher.group(1).trim();
					fieldValue = internalMatcher.group(2).trim();
					record.add(fieldName, fieldValue);
					System.out.println("JobLogHistoryProcessor Add field: [" + fieldName + "][" + fieldValue +"]" );
					internalMatcher.reset(internalMatcher.group(3));
				}

				
				System.out.println("JobLogHistoryProcessor outputing a record ============>>>>>");
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
		}
	}
}
