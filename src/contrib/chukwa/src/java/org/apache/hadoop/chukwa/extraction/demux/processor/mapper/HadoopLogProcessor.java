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



public class HadoopLogProcessor extends AbstractProcessor
{
	static Logger log = Logger.getLogger(HadoopLogProcessor.class);
	
	private static final String recordType = "HadoopLog";
	private static final String nameNodeType = "NameNode";
	private static final String dataNodeType = "DataNode";
	
	private static String regex= null;
	private static Pattern p = null;
	
	private Matcher matcher = null;
	private SimpleDateFormat sdf = null;
	
	
	public HadoopLogProcessor()
	{
		//TODO move that to config
		sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
		regex="([0-9]{4}\\-[0-9]{2}\\-[0-9]{2} [0-9]{2}\\:[0-9]{2}:[0-9]{2},[0-9]{3}) (INFO|DEBUG|ERROR|WARN) (.*?): ((.*)\n*)*\\z";
		p = Pattern.compile(regex);
		matcher = p.matcher("-");
	}
	
	@Override
	public void parse(String line, OutputCollector<Text, ChukwaRecord> output,
			Reporter reporter)
	{
		log.info("record: [" + line + "] type[" + chunk.getDataType() + "]");
		
		ChukwaRecord record = new ChukwaRecord();
		
		matcher.reset(line);
		if (matcher.matches())
		{
			try
			{
				Date d = sdf.parse( matcher.group(0).trim());
				if (this.chunk.getStreamName().indexOf("datanode") > 0)
				{
					buildGenericRecord(record,line,d.getTime(),dataNodeType);
				}
				else if (this.chunk.getStreamName().indexOf("namenode") > 0)
				{
					buildGenericRecord(record,line,d.getTime(),nameNodeType);
				}
				else
				{
					buildGenericRecord(record,line,d.getTime(),recordType);
				}
				
				key.set("" + d.getTime());
				record.add(Record.logLevelField, "" +matcher.group(2));
				record.add(Record.classField, "" +matcher.group(3));
				record.add(Record.bodyField, "" +matcher.group(4));
				output.collect(key, record);
			}
			catch (ParseException e)
			{
				e.printStackTrace();
			}
			catch (IOException e)
			{
				e.printStackTrace();
			}
		}
		
	}

	public String getDataType()
	{
		return HadoopLogProcessor.recordType;
	}

}
