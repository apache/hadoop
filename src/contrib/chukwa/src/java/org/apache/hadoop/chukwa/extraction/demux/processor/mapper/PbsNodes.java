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

import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

public class PbsNodes extends AbstractProcessor
{
	static Logger log = Logger.getLogger(PbsNodes.class);

	private static final String rawPBSRecordType = "PbsNodes";
	private static final String machinePBSRecordType = "MachinePbsNodes";
	private SimpleDateFormat sdf = null;

	public PbsNodes()
	{
		//TODO move that to config
		sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
	}

	@Override
	protected void parse(String recordEntry, OutputCollector<ChukwaRecordKey, ChukwaRecord> output,
			Reporter reporter)
	 throws Throwable
	{
		
//		log.info("PbsNodeProcessor record: [" + recordEntry + "] type[" + chunk.getDataType() + "]");
		
		

		StringBuilder sb = new StringBuilder(); 	 
		int i = 0;
		String nodeActivityStatus = null;
		StringBuilder sbFreeMachines = new StringBuilder();
		StringBuilder sbUsedMachines = new StringBuilder();
		StringBuilder sbDownMachines = new StringBuilder();
		
		int totalFreeNode = 0;
		int totalUsedNode = 0;
		int totalDownNode = 0;
	
		String body = null;
		ChukwaRecord record = null;
		
		
			try
			{
				
				String dStr = recordEntry.substring(0, 23);
				int start = 24;
				int idx = recordEntry.indexOf(' ', start);
				//String level = recordEntry.substring(start, idx);
				start = idx+1;
				idx = recordEntry.indexOf(' ', start );
				//String className = recordEntry.substring(start, idx-1);
				body = recordEntry.substring(idx+1);
				
				
				Date d = sdf.parse(dStr );
				
				String[] lines = body.split("\n");
				while (i < lines.length)
				{
					while ((i < lines.length) && (lines[i].trim().length() > 0))
					{
						sb.append(lines[i].trim()).append("\n");
						i++;
					}

					 if ( (i< lines.length) && (lines[i].trim().length() > 0) )
					 {
					 throw new PbsInvalidEntry(recordEntry);
					 }

					// Empty line
					i++;

					if (sb.length() > 0)
					{
						body =  sb.toString();
						// Process all entries for a machine
						//System.out.println("=========>>> Record [" + body+ "]");
						
						record = new ChukwaRecord();
						key = new ChukwaRecordKey();
						
						buildGenericRecord(record,null,d.getTime(),machinePBSRecordType);
						parsePbsRecord(body, record);
						
						//Output PbsNode record for 1 machine
						output.collect(key, record);
						//log.info("PbsNodeProcessor output 1 sub-record");
						
						//compute Node Activity information
						nodeActivityStatus = record.getValue("state");
						if (nodeActivityStatus != null)
						{
							if (nodeActivityStatus.equals("free"))
							{
								totalFreeNode ++;
								sbFreeMachines.append(record.getValue("Machine")).append(",");
							}
							else if (nodeActivityStatus.equals("job-exclusive"))
							{
								totalUsedNode ++;
								sbUsedMachines.append(record.getValue("Machine")).append(",");
							}
							else
							{
								totalDownNode ++;
								sbDownMachines.append(record.getValue("Machine")).append(",");
							}					
						}
						sb = new StringBuilder();
					}
				}
				
				// End of parsing

				record = new ChukwaRecord();
				key = new ChukwaRecordKey();
				buildGenericRecord(record,null,d.getTime(),"NodeActivity");
				
				record.setTime(d.getTime());
				record.add("used", ""+totalUsedNode);
				record.add("free", ""+totalFreeNode);
				record.add("down", ""+totalDownNode);
				record.add("usedMachines", sbUsedMachines.toString());
				record.add("freeMachines", sbFreeMachines.toString());
				record.add("downMachines", sbDownMachines.toString());
				
				output.collect(key,record);
				//log.info("PbsNodeProcessor output 1 NodeActivity");					
			}
			catch (ParseException e)
			{
				e.printStackTrace();
				log.warn("Wrong format in PbsNodesProcessor [" + recordEntry + "]", e);
				throw e;
			}
			catch (IOException e)
			{
				log.warn("Unable to collect output in PbsNodesProcessor [" + recordEntry + "]", e);
				e.printStackTrace();
				throw e;
			}
			catch (PbsInvalidEntry e)
			{
				log.warn("Wrong format in PbsNodesProcessor [" + recordEntry + "]", e);
				e.printStackTrace();
				throw e;
			}
		
		

		
	}

	protected static void parsePbsRecord(String recordLine, ChukwaRecord record)
	{
		int i = 0;
		String[] lines = recordLine.split("\n");
		record.add("Machine", lines[0]);
		
		i++;
		String[] data = null;
		while (i < lines.length)
		{
			data = extractFields(lines[i]);	
			record.add(data[0].trim(), data[1].trim());
			if (data[0].trim().equalsIgnoreCase("status"))
			{
				parseStatusField(data[1].trim(), record);
			}
			i++;
		}
	}

	protected static void parseStatusField(String statusField,
			ChukwaRecord record)
	{
		String[] data = null;
		String[] subFields = statusField.trim().split(",");
		for (String subflied : subFields)
		{
			data = extractFields(subflied);
			record.add("status-"+data[0].trim(), data[1].trim());
		}
	}


	static String[] extractFields(String line)
	{
		String[] args = new String[2];
		int index = line.indexOf("=");
		args[0] = line.substring(0,index );
		args[1] = line.substring(index + 1);

		return args;
	}

	public String getDataType()
	{
		return PbsNodes.rawPBSRecordType;
	}
	
}
