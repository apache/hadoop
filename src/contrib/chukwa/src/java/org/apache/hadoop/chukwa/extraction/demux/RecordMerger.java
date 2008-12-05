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

package org.apache.hadoop.chukwa.extraction.demux;

import java.io.IOException;

import org.apache.hadoop.chukwa.conf.ChukwaConfiguration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class RecordMerger extends Thread
{
	static Logger log = Logger.getLogger(RecordMerger.class);
	ChukwaConfiguration conf = null;
	FileSystem fs = null;
	String[] mergeArgs = null;
	Tool tool = null;
	boolean deleteRawData = false;
	
	public RecordMerger(ChukwaConfiguration conf,FileSystem fs,Tool tool,String[] mergeArgs,boolean deleteRawData)
	{
		this.conf = conf;
		this.fs = fs;
		this.tool = tool;
		this.mergeArgs = mergeArgs;
		this.deleteRawData = deleteRawData;
	}
	@Override
	public void run()
	{
		System.out.println("\t Running Merge! : output [" + mergeArgs[1] +"]");
		int res;
		try
		{
			res = ToolRunner.run(conf,tool, mergeArgs);
			System.out.println("MR exit status: " + res);
			if (res == 0)
			{
				writeRecordFile(mergeArgs[1]+"/part-00000",mergeArgs[2],mergeArgs[3]);
				
				// delete input
				if (deleteRawData)
				 { 
					FileUtil.fullyDelete(fs,new Path(mergeArgs[0])); 
					
					Path hours = new Path(mergeArgs[2]) ;
					FileStatus[] hoursOrMinutesFS = fs.listStatus(hours);
					for(FileStatus hourOrMinuteFS : hoursOrMinutesFS)
					{
						String dirName = hourOrMinuteFS.getPath().getName();
						
						try
						{ 
							Integer.parseInt(dirName);
							FileUtil.fullyDelete(fs,new Path(mergeArgs[2] + "/" + dirName)); 
							if (log.isDebugEnabled() )
								{ log.debug("Deleting Hour directory: " + mergeArgs[2] + "/" + dirName); }
						}
						catch(NumberFormatException e) { /* Not an Hour or Minutes directory- Do nothing */ }
					}
				 }
				
				// delete rolling tag
				FileUtil.fullyDelete(fs, new Path(mergeArgs[3]));
				// delete M/R temp directory
				FileUtil.fullyDelete(fs, new Path(mergeArgs[1]));
			}
			else
			{
				throw new RuntimeException("Error in M/R merge operation!");
			}

		} 
		catch (Exception e)
		{
			e.printStackTrace();
			throw new RuntimeException("Error in M/R merge operation!",e);
		}
	}
	
	
	void writeRecordFile(String input,String outputDir,String fileName) throws IOException
	{
		boolean done = false;
		int count = 1;
		Path recordFile = new Path(input);
		do
		{
			Path destDirPath = new Path(outputDir );
			Path destFilePath = new Path(outputDir + "/" + fileName + "." + count + ".evt" );
			
			if (!fs.exists(destDirPath))
			{
				fs.mkdirs(destDirPath);
				log.info(">>>>>>>>>>>> create Dir" + destDirPath);
			}
			
			if (!fs.exists(destFilePath))
			{
				boolean res = fs.rename(recordFile,destFilePath);
				
				if (res == false)
				{
					log.info(">>>>>>>>>>>> Use standard copy rename failded");
					FileUtil.copy(fs,recordFile,fs,destFilePath,false,false,conf);
				}
				done = true;
			}
			else
			{
				log.info("Start MoveToRepository main()");
			}
			count ++;
			// Just put a limit here
			// TODO read from config
			if (count > 1000)
			{
				throw new IOException("too many files in this directory: " + destDirPath);
			}
		} while (!done);
	}
}
