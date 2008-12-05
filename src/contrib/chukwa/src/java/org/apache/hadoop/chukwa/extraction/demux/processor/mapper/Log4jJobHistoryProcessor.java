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
import java.util.Hashtable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.chukwa.extraction.engine.Record;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

public class Log4jJobHistoryProcessor extends AbstractProcessor
{
	static Logger log = Logger.getLogger(Log4jJobHistoryProcessor.class);

	private static final String recordType = "JobLogHistory";
	private static String internalRegex = null;
	private static Pattern ip = null;

	private Matcher internalMatcher = null;

	public Log4jJobHistoryProcessor()
	{
		internalRegex = "(.*?)=\"(.*?)\"(.*)([\\n])?";
		ip = Pattern.compile(internalRegex);
		internalMatcher = ip.matcher("-");
	}

	@Override
	protected void parse(String recordEntry,
			OutputCollector<ChukwaRecordKey, ChukwaRecord> output,
			Reporter reporter)
	 throws Throwable
	{

//		log.info("JobLogHistoryProcessor record: [" + recordEntry + "] type["
//				+ chunk.getDataType() + "]");

		try
		{

			//String dStr = recordEntry.substring(0, 23);
			int start = 24;
			int idx = recordEntry.indexOf(' ', start);
			// String level = recordEntry.substring(start, idx);
			start = idx + 1;
			idx = recordEntry.indexOf(' ', start);
			// String className = recordEntry.substring(start, idx-1);
			String body = recordEntry.substring(idx + 1);

			Hashtable<String, String> keys = new Hashtable<String, String>();
			ChukwaRecord record = null;
		
			int firstSep = body.indexOf(" ");
			keys.put("RECORD_TYPE", body.substring(0, firstSep));
//			log.info("JobLogHistoryProcessor Add field: [RECORD_TYPE]["
//					+ keys.get("RECORD_TYPE") + "]");

			body = body.substring(firstSep);

			internalMatcher.reset(body);

//			 String fieldName = null;
//			 String fieldValue = null;

			while (internalMatcher.matches())
			{

				keys.put(internalMatcher.group(1).trim(), internalMatcher
						.group(2).trim());

				// TODO Remove debug info before production
//				 fieldName = internalMatcher.group(1).trim();
//				 fieldValue = internalMatcher.group(2).trim();
//				 log.info("JobLogHistoryProcessor Add field: [" + fieldName +
//				 "][" + fieldValue +"]" );
//				 log.info("EOL : [" + internalMatcher.group(3) + "]" );
				internalMatcher.reset(internalMatcher.group(3));
			} 

			if (!keys.containsKey("JOBID"))
			{
				// Extract JobID from taskID
				// JOBID  = "job_200804210403_0005"
				// TASKID = "tip_200804210403_0005_m_000018"
				String jobId = keys.get("TASKID");
				int idx1 = jobId.indexOf('_',0);
				int idx2 = jobId.indexOf('_', idx1+1);
				idx2 = jobId.indexOf('_', idx2+1);
				keys.put("JOBID","job" + jobId.substring(idx1,idx2));
//				log.info("JobLogHistoryProcessor Add field: [JOBID]["
//						+ keys.get("JOBID") + "]");
			}
			
			// if (keys.get("RECORD_TYPE").equalsIgnoreCase("Job") &&
			// keys.containsKey("SUBMIT_TIME"))
			// {
			// // Job JOBID="job_200804210403_0005" JOBNAME="MY_JOB"
			// USER="userxxx"
			// // SUBMIT_TIME="1208760436751"
			// JOBCONF="/mapredsystem/xxx.yyy.com/job_200804210403_0005/job.xml"
			//					
			//					
			// }
			// else if (keys.get("RECORD_TYPE").equalsIgnoreCase("Job") &&
			// keys.containsKey("LAUNCH_TIME"))
			// {
			// // Job JOBID="job_200804210403_0005" LAUNCH_TIME="1208760437110"
			// TOTAL_MAPS="5912" TOTAL_REDUCES="739"
			//					
			// }
			// else if (keys.get("RECORD_TYPE").equalsIgnoreCase("Job") &&
			// keys.containsKey("FINISH_TIME"))
			// {
			// // Job JOBID="job_200804210403_0005" FINISH_TIME="1208760906816"
			// JOB_STATUS="SUCCESS" FINISHED_MAPS="5912" FINISHED_REDUCES="739"
			// FAILED_MAPS="0" FAILED_REDUCES="0"
			// // COUNTERS="File Systems.Local bytes read:1735053407244,File
			// Systems.Local bytes written:2610106384012,File Systems.HDFS bytes
			// read:801605644910,File Systems.HDFS bytes written:44135800,
			// // Job Counters .Launched map tasks:5912,Job Counters .Launched
			// reduce tasks:739,Job Counters .Data-local map tasks:5573,Job
			// Counters .Rack-local map tasks:316,Map-Reduce Framework.
			// // Map input records:9410696067,Map-Reduce Framework.Map output
			// records:9410696067,Map-Reduce Framework.Map input
			// bytes:801599188816,Map-Reduce Framework.Map output
			// bytes:784427968116,
			// // Map-Reduce Framework.Combine input records:0,Map-Reduce
			// Framework.Combine output records:0,Map-Reduce Framework.Reduce
			// input groups:477265,Map-Reduce Framework.Reduce input
			// records:739000,
			// // Map-Reduce Framework.Reduce output records:739000"
			//					
			// }
			// else
			if (keys.get("RECORD_TYPE").equalsIgnoreCase("MapAttempt")
					&& keys.containsKey("START_TIME"))
			{
				// MapAttempt TASK_TYPE="MAP"
				// TASKID="tip_200804210403_0005_m_000018"
				// TASK_ATTEMPT_ID="task_200804210403_0005_m_000018_0"
				// START_TIME="1208760437531"
				// HOSTNAME="tracker_xxx.yyy.com:xxx.yyy.com/xxx.xxx.xxx.xxx:53734"

				key = new ChukwaRecordKey();
				key.setKey("JobLogHist/Map/" + keys.get("JOBID") + "/" + keys.get("START_TIME"));
				key.setReduceType("JobLogHistoryReduceProcessor");
				record = new ChukwaRecord();
				record.setTime(Long.parseLong(keys.get("START_TIME")));
				record.add("JOBID",keys.get("JOBID"));
				record.add("START_TIME", keys.get("START_TIME"));
				record.add(Record.tagsField, chunk.getTags());
//				log.info("JobLogHist/Map/S");
				output.collect(key, record);

			} else if (keys.get("RECORD_TYPE").equalsIgnoreCase("MapAttempt")
					&& keys.containsKey("FINISH_TIME"))
			{
				// MapAttempt TASK_TYPE="MAP"
				// TASKID="tip_200804210403_0005_m_005494"
				// TASK_ATTEMPT_ID="task_200804210403_0005_m_005494_0"
				// TASK_STATUS="SUCCESS"
				// FINISH_TIME="1208760624124"
				// HOSTNAME="tracker_xxxx.yyyy.com:xxx.yyy.com/xxx.xxx.xxx.xxx:55491"

				key = new ChukwaRecordKey();
				key.setKey("JobLogHist/Map/" + keys.get("JOBID") + "/" + keys.get("FINISH_TIME"));
				key.setReduceType("JobLogHistoryReduceProcessor");
				record = new ChukwaRecord();
				record.setTime(Long.parseLong(keys.get("FINISH_TIME")));
				record.add("JOBID",keys.get("JOBID"));
				record.add("FINISH_TIME", keys.get("FINISH_TIME"));
				record.add(Record.tagsField, chunk.getTags());
//				log.info("JobLogHist/Map/E");
				output.collect(key, record);
			}

			else if (keys.get("RECORD_TYPE").equalsIgnoreCase("ReduceAttempt")
					&& keys.containsKey("START_TIME"))
			{
				// ReduceAttempt TASK_TYPE="REDUCE"
				// TASKID="tip_200804210403_0005_r_000138"
				// TASK_ATTEMPT_ID="task_200804210403_0005_r_000138_0"
				// START_TIME="1208760454885"
				// HOSTNAME="tracker_xxxx.yyyy.com:xxx.yyy.com/xxx.xxx.xxx.xxx:51947"

				key = new ChukwaRecordKey();
				key.setKey("JobLogHist/SHUFFLE/" + keys.get("JOBID") + "/" + keys.get("START_TIME"));
				key.setReduceType("JobLogHistoryReduceProcessor");
				record = new ChukwaRecord();
				record.setTime(Long.parseLong(keys.get("START_TIME")));
				record.add("JOBID",keys.get("JOBID"));
				record.add("START_TIME", keys.get("START_TIME"));
				record.add(Record.tagsField, chunk.getTags());
//				log.info("JobLogHist/SHUFFLE/S");
				output.collect(key, record);

			} else if (keys.get("RECORD_TYPE")
					.equalsIgnoreCase("ReduceAttempt")
					&& keys.containsKey("FINISH_TIME"))
			{
				// ReduceAttempt TASK_TYPE="REDUCE"
				// TASKID="tip_200804210403_0005_r_000138"
				// TASK_ATTEMPT_ID="task_200804210403_0005_r_000138_0"
				// TASK_STATUS="SUCCESS" SHUFFLE_FINISHED="1208760787167"
				// SORT_FINISHED="1208760787354" FINISH_TIME="1208760802395"
				// HOSTNAME="tracker__xxxx.yyyy.com:xxx.yyy.com/xxx.xxx.xxx.xxx:51947"

				key = new ChukwaRecordKey();
				key.setKey("JobLogHist/SHUFFLE/" + keys.get("JOBID") + "/" + keys.get("SHUFFLE_FINISHED"));
				key.setReduceType("JobLogHistoryReduceProcessor");
				record = new ChukwaRecord();
				record.setTime(Long.parseLong(keys.get("SHUFFLE_FINISHED")));
				record.add("JOBID",keys.get("JOBID"));
				record.add("SHUFFLE_FINISHED", keys.get("SHUFFLE_FINISHED"));
				record.add(Record.tagsField, chunk.getTags());
//				log.info("JobLogHist/SHUFFLE/E");
				output.collect(key, record);

				// SORT
				key = new ChukwaRecordKey();
				key.setKey("JobLogHist/SORT/" + keys.get("JOBID") + "/" + keys.get("SHUFFLE_FINISHED"));
				key.setReduceType("JobLogHistoryReduceProcessor");
				record = new ChukwaRecord();
				record.setTime(Long.parseLong(keys.get("SHUFFLE_FINISHED")));
				record.add("JOBID",keys.get("JOBID"));
				record.add("START_TIME", keys.get("SHUFFLE_FINISHED"));
				record.add(Record.tagsField, chunk.getTags());
//				log.info("JobLogHist/SORT/S");
				output.collect(key, record);

				key = new ChukwaRecordKey();
				key.setKey("JobLogHist/SORT/" + keys.get("JOBID") + "/" + keys.get("SORT_FINISHED"));
				key.setReduceType("JobLogHistoryReduceProcessor");
				record = new ChukwaRecord();
				record.setTime(Long.parseLong(keys.get("SORT_FINISHED")));
				record.add("JOBID",keys.get("JOBID"));
				record.add("SORT_FINISHED", keys.get("SORT_FINISHED"));
				record.add(Record.tagsField, chunk.getTags());
//				log.info("JobLogHist/SORT/E");
				output.collect(key, record);

				// Reduce
				key = new ChukwaRecordKey();
				key.setKey("JobLogHist/REDUCE/" + keys.get("JOBID") + "/" + keys.get("SORT_FINISHED"));
				key.setReduceType("JobLogHistoryReduceProcessor");
				record = new ChukwaRecord();
				record.setTime(Long.parseLong(keys.get("SORT_FINISHED")));
				record.add("JOBID",keys.get("JOBID"));
				record.add("START_TIME", keys.get("SORT_FINISHED"));
				record.add(Record.tagsField, chunk.getTags());
//				log.info("JobLogHist/REDUCE/S");
				output.collect(key, record);

				key = new ChukwaRecordKey();
				key.setKey("JobLogHist/REDUCE/" + keys.get("JOBID") + "/" + keys.get("FINISH_TIME"));
				key.setReduceType("JobLogHistoryReduceProcessor");
				record = new ChukwaRecord();
				record.setTime(Long.parseLong(keys.get("SORT_FINISHED")));
				record.add("JOBID",keys.get("JOBID"));
				record.add("FINISH_TIME", keys.get("SORT_FINISHED"));
				record.add(Record.tagsField, chunk.getTags());
//				log.info("JobLogHist/REDUCE/E");
				output.collect(key, record);

			} else if (keys.get("RECORD_TYPE").equalsIgnoreCase("Job")
					&& keys.containsKey("COUNTERS"))
			{
				// Job JOBID="job_200804210403_0005" FINISH_TIME="1208760906816"
				// JOB_STATUS="SUCCESS" FINISHED_MAPS="5912"
				// FINISHED_REDUCES="739" FAILED_MAPS="0" FAILED_REDUCES="0"
				// COUNTERS="File Systems.Local bytes read:1735053407244,File
				// Systems.Local bytes written:2610106384012,File Systems.HDFS
				// bytes read:801605644910,File Systems.HDFS bytes
				// written:44135800,
				// Job Counters .Launched map tasks:5912,Job Counters .Launched
				// reduce tasks:739,Job Counters .Data-local map tasks:5573,Job
				// Counters .Rack-local map tasks:316,Map-Reduce Framework.
				// Map input records:9410696067,Map-Reduce Framework.Map output
				// records:9410696067,Map-Reduce Framework.Map input
				// bytes:801599188816,Map-Reduce Framework.Map output
				// bytes:784427968116,
				// Map-Reduce Framework.Combine input records:0,Map-Reduce
				// Framework.Combine output records:0,Map-Reduce
				// Framework.Reduce input groups:477265,Map-Reduce
				// Framework.Reduce input records:739000,
				// Map-Reduce Framework.Reduce output records:739000"

				record = new ChukwaRecord();
				key = new ChukwaRecordKey();
				buildGenericRecord(record, null, Long.parseLong(keys.get("FINISH_TIME")), "MRJobCounters");
				extractCounters(record, keys.get("COUNTERS"));

				String jobId = keys.get("JOBID").replace("_", "").substring(3);
				record.add("JobId", "" + jobId);

				// FIXME validate this when HodId will be available
				if (keys.containsKey("HODID"))
					{ record.add("HodId", keys.get("HODID")); }

//				log.info("MRJobCounters +1");
				output.collect(key, record);
			}

			if (keys.containsKey("TASK_TYPE")
					&& keys.containsKey("COUNTERS")
					&& (keys.get("TASK_TYPE").equalsIgnoreCase("REDUCE") || keys
							.get("TASK_TYPE").equalsIgnoreCase("MAP")))
			{
				// MAP
				// Task TASKID="tip_200804210403_0005_m_000154" TASK_TYPE="MAP"
				// TASK_STATUS="SUCCESS" FINISH_TIME="1208760463883"
				// COUNTERS="File Systems.Local bytes read:159265655,File
				// Systems.Local bytes written:318531310,
				// File Systems.HDFS bytes read:145882417,Map-Reduce
				// Framework.Map input records:1706604,
				// Map-Reduce Framework.Map output records:1706604,Map-Reduce
				// Framework.Map input bytes:145882057,
				// Map-Reduce Framework.Map output bytes:142763253,Map-Reduce
				// Framework.Combine input records:0,Map-Reduce
				// Framework.Combine output records:0"

				// REDUCE
				// Task TASKID="tip_200804210403_0005_r_000524"
				// TASK_TYPE="REDUCE" TASK_STATUS="SUCCESS"
				// FINISH_TIME="1208760877072"
				// COUNTERS="File Systems.Local bytes read:1179319677,File
				// Systems.Local bytes written:1184474889,File Systems.HDFS
				// bytes written:59021,
				// Map-Reduce Framework.Reduce input groups:684,Map-Reduce
				// Framework.Reduce input records:1000,Map-Reduce
				// Framework.Reduce output records:1000"

				record = new ChukwaRecord();
				key = new ChukwaRecordKey();
				buildGenericRecord(record, null, Long.parseLong(keys.get("FINISH_TIME")), "SizeVsFinishTime");
				extractCounters(record, keys.get("COUNTERS"));
				record.add("JOBID",keys.get("JOBID"));
				record.add("TASKID", keys.get("TASKID"));
				record.add("TASK_TYPE", keys.get("TASK_TYPE"));
				
//				log.info("MR_Graph +1");
				output.collect(key, record);

			}
		} 
		catch (IOException e)
		{
			log.warn("Unable to collect output in JobLogHistoryProcessor ["
					+ recordEntry + "]", e);
			e.printStackTrace();
			throw e;
		}

	}

	protected void extractCounters(ChukwaRecord record, String input)
	{

		String[] data = null;
		String[] counters = input.split(",");

		for (String counter : counters)
		{
			data = counter.split(":");
			record.add(data[0].replaceAll(" ", "_").replaceAll("\\.", "_")
					.toUpperCase(), data[1]);
		}
	}

	public String getDataType()
	{
		return Log4jJobHistoryProcessor.recordType;
	}
}
