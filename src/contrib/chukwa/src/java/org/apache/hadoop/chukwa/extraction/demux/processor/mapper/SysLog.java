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
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

public class SysLog extends AbstractProcessor 
{

	static Logger log = Logger.getLogger(SysLog.class);
	private SimpleDateFormat sdf = null;

	public SysLog()
	{
		sdf = new SimpleDateFormat("MMM d HH:mm:ss");
	}
	
	
  @Override
  protected void parse(String recordEntry,
      OutputCollector<ChukwaRecordKey, ChukwaRecord> output, Reporter reporter)
  throws Throwable
  {
		try
		{
			String dStr = recordEntry.substring(0, 15);
			int start = 15;
			int idx = recordEntry.indexOf(' ', start);
			start = idx + 1;
			idx = recordEntry.indexOf(' ', start);
			String body = recordEntry.substring(idx + 1);
			body.replaceAll("\n", "");

			Calendar convertDate = Calendar.getInstance();
			Date d = sdf.parse(dStr);
			int year = convertDate.get(Calendar.YEAR);
			convertDate.setTime(d);
			convertDate.set(Calendar.YEAR, year);
			
			ChukwaRecord record = new ChukwaRecord();
			buildGenericRecord(record,recordEntry,convertDate.getTime().getTime(),"SysLog");
			output.collect(key, record);
		}
		catch (ParseException e)
		{
			e.printStackTrace();
			log.warn("Wrong format in SysLog [" + recordEntry + "]", e);
			throw e;
		}
		catch (IOException e)
		{
			e.printStackTrace();
			log.warn("Unable to collect output in SysLog [" + recordEntry + "]", e);
			throw e;
		}

  }


	public String getDataType()
	{
		return SysLog.class.getName();
	}

}
