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
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import java.util.Date;

import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.Record;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

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
			Text key = new Text();

		    ChukwaRecord evt = new ChukwaRecord();
			while(r.next(key, evt))
			{	
				lineCount ++;
				
				System.out.println("NameNodeParser Line [" +evt.getValue(Record.bodyField) + "]");	
				
				if (evt != null)
				{
					timestamp = evt.getTime();
					if (timestamp < t0) 
					{
						 System.out.println("Line not in range. Skipping: " +evt.getValue(Record.bodyField));
						 System.out.println("Search for: " + new Date(t0) + " is :" + new Date(timestamp));
						 continue;
					} 
					else if ((timestamp < t1) && (offset < maxOffset )) //JB (epochTS < maxDate)
					{
						
						System.out.println("In Range: " + evt.getValue(Record.bodyField));
						boolean valid = false;
						
						 if ( (filter == null || filter.equals("") ))
						 {
							 valid = true;
						 }
						 else if (evt.getValue(Record.rawField).toLowerCase().indexOf(filter) > 0)
						   {
 System.out.println("MATCH " +  filter + "===========================>>>>>>>" + evt.getValue(Record.rawField));
							   valid = true;
						   }
						 
						 if (valid)
						 {
							records.add(evt);
evt = new ChukwaRecord();
							listSize = records.size();
							if (listSize > maxRows)
							{
								records.remove(0);
								System.out.println("==========>>>>>REMOVING: " + evt.getValue(Record.bodyField));
							}
						 }
						else 
						{
							System.out.println("In Range ==================>>>>>>>>> OUT Regex: " + evt.getValue(Record.bodyField));
						}

					}
					else
					{
						 System.out.println("Line out of range. Stopping now: " +evt.getValue(Record.bodyField));
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
	
	public static void main(String[] args) throws Throwable
	{
		Configuration conf = new Configuration();

	    FileSystem fs = FileSystem.get(conf);//FileSystem.get(new URI(fsURL), conf);
	    Calendar c = Calendar.getInstance();
	    c.add(Calendar.MONTH, -2);
	    
	    ChukwaSequenceFileParser.readData(	"/tmp/t1", "NameNode",
	    									200, new java.util.Date().getTime(), 
	    									c.getTimeInMillis(), Long.MAX_VALUE, null, 
	    									args[0], fs, conf);
	    
	    SequenceFile.Reader r= new SequenceFile.Reader(fs, new Path(args[0]), conf);
	    Text key = new Text();
	    
	    ChukwaRecord evt = new ChukwaRecord();
	    while(r.next(key, evt))
	    {
	      System.out.println( evt);
	    }
	}
}
