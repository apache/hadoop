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

package org.apache.hadoop.chukwa.extraction.engine.datasource.record;

import java.io.IOException;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.chukwa.extraction.engine.Record;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;

public class ChukwaSequenceFileParser
{

	public static  List<Record> readData(String cluster,String dataSource,int maxRows,long t1, long t0,
			long maxOffset,String filter,String fileName,FileSystem fs,Configuration conf  ) throws
			MalformedFileFormat
	{
	
		//String source = "NameNode." + fileName;
		List<Record> records = new LinkedList<Record>();
		SequenceFile.Reader r= null;
		
		int lineCount = 0;
		if (filter != null)
			{ filter = filter.toLowerCase();}
		
		try
		{
			
			if (!fs.exists(new Path(fileName)))
			{
				System.out.println("fileName not there!");
				return records;
			}
			System.out.println("NameNodeParser Open [" +fileName + "]");
			
			r= new SequenceFile.Reader(fs, new Path(fileName), conf);
			System.out.println("NameNodeParser Open2 [" +fileName + "]");
			
			long timestamp = 0;
			int listSize = 0;

			long offset = 0;
			
//			HdfsWriter.HdfsWriterKey key = new HdfsWriter.HdfsWriterKey();
			ChukwaRecordKey key = new ChukwaRecordKey();
		    ChukwaRecord record = new ChukwaRecord();
		    
			while(r.next(key, record))
			{	
				lineCount ++;
				
				System.out.println("NameNodeParser Line [" +record.getValue(Record.bodyField) + "]");	
				
				if (record != null)
				{
					timestamp = record.getTime();
					if (timestamp < t0) 
					{
						 System.out.println("Line not in range. Skipping: " +record.getValue(Record.bodyField));
						 System.out.println("Search for: " + new Date(t0) + " is :" + new Date(timestamp));
						 continue;
					} 
					else if ((timestamp < t1) && (offset < maxOffset )) //JB (epochTS < maxDate)
					{
						
						System.out.println("In Range: " + record.getValue(Record.bodyField));
						boolean valid = false;
						
						 if ( (filter == null || filter.equals("") ))
						 {
							 valid = true;
						 }
						 else if ( isValid(record,filter))
						 {
							 valid = true;
						 }
						 
						 if (valid)
						 {
							records.add(record);
							record = new ChukwaRecord();
							listSize = records.size();
							if (listSize > maxRows)
							{
								records.remove(0);
								System.out.println("==========>>>>>REMOVING: " + record.getValue(Record.bodyField));
							}
						 }
						else 
						{
							System.out.println("In Range ==================>>>>>>>>> OUT Regex: " + record.getValue(Record.bodyField));
						}

					}
					else
					{
						 System.out.println("Line out of range. Stopping now: " +record.getValue(Record.bodyField));
						break;
					}
				}

			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			System.out.println("File: " +fileName +" Line count: " + lineCount);
			if (r != null)
			{
				try
				{r.close();} 
				catch (IOException e)
				{}	
			}
			
		}
		return records;
	}
	
	
	protected static boolean isValid(ChukwaRecord record, String filter)
	{
		String[] fields = record.getFields();
		for(String field: fields)
		{
			if ( record.getValue(field).toLowerCase().indexOf(filter) >= 0)
			{
				return true;
			}
		}
		return false;
	}
}
