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
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.Record;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;



public class ChukwaFileParser
{
	static final int timestampField = 0;
		
	
	@SuppressWarnings("deprecation")
	public static  List<Record> readData(String cluster,String dataSource,int maxRows,long t1, long t0,
			long maxOffset,String filter,String fileName,FileSystem fs ) throws
			MalformedFileFormat
	{
	
		//String source = "NameNode." + fileName;
		List<Record> records = new LinkedList<Record>();
		FSDataInputStream dataIS = null;
		int lineCount = 0;
		
		try
		{
			
			if (!fs.exists(new Path(fileName)))
			{
				System.out.println("fileName not there!");
				return records;
			}
			System.out.println("NameNodeParser Open [" +fileName + "]");
			
			dataIS = fs.open(new Path(fileName));
			System.out.println("NameNodeParser Open2 [" +fileName + "]");
			
			long timestamp = 0;
			int listSize = 0;
			String line = null;
			String[] data = null;
			long offset = 0;
			
			
			do
			{
				offset = dataIS.getPos();
				
				// Need TODO something here
//				if (offset > maxOffset)
//				{
//					break;
//				}
				
				line = dataIS.readLine();
				lineCount ++;
//				System.out.println("NameNodeParser Line [" +line + "]");	
				if (line != null)
				{
					
					//empty lines
					if (line.length() < 14)
					{
//						System.out.println("NameNodeParser Line < 14! [" +line + "]");
						continue;
					}
//					System.out.println("Line [" +line + "]");
					data = line.split("\t");// Default separator for TextOutputFormat!
					
					try
					{
						timestamp = Long.parseLong(data[timestampField]);
						
					} catch (Exception e)
					{
						e.printStackTrace();
						//throw new MalformedFileFormat(e);
					}
					if (timestamp < t0) 
					{
//						 System.out.println("Line not in range. Skipping: " +line);
//						 System.out.println("Search for: " + new Date(t0) + " is :" + new Date(timestamp));
						 continue;
					} 
					else if ((timestamp < t1) && (offset < maxOffset )) //JB (epochTS < maxDate)
					{
						
//						System.out.println("In Range: " + line);
						boolean valid = false;
						
						 if ( (filter == null || filter.equals("") ))
						 {
							 valid = true;
						 }
						 else if (line.indexOf(filter) > 0)
						   {
							   valid = true;
						   }
						 
						 if (valid)
						 {
//							System.out.println("In Range In Filter: " + line);
							ChukwaRecord record = new ChukwaRecord();
							record.setTime(timestamp);
							record.add("offset", ""+offset);
							record.add(Record.bodyField, data[1]);
							record.add(Record.sourceField, dataSource);
							
							records.add(record);
							listSize = records.size();
							if (listSize > maxRows)
							{
								records.remove(0);
//								System.out.println("==========>>>>>REMOVING: " + e);
							}
						 }
						else 
						{
//							System.out.println("In Range ==================>>>>>>>>> OUT Regex: " + line);
						}

					}
					else
					{
//						 System.out.println("Line out of range. Stopping now: " +line);
						break;
					}
				}

			} while (line != null);			
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			System.out.println("File: " +fileName +" Line count: " + lineCount);
			try
			{dataIS.close();} 
			catch (IOException e)
			{}
		}
		return records;
	}
}
