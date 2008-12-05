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

package org.apache.hadoop.chukwa.extraction.archive;

import org.apache.hadoop.chukwa.ChukwaArchiveKey;
import org.apache.hadoop.chukwa.ChunkImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class ChukwaArchiveBuilder extends Configured implements Tool
{
	static Logger log = Logger.getLogger(ChukwaArchiveBuilder.class);

	static int printUsage()
	{
		System.out
				.println("ChuckwaArchiveBuilder <Stream/DataType/Daily/Hourly> <input> <output>");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}
	
	public int run(String[] args) throws Exception
	{
	

		// Make sure there are exactly 3 parameters left.
		if (args.length != 3)
		{
			System.out.println("ERROR: Wrong number of parameters: "
					+ args.length + " instead of 3.");
			return printUsage();
		}
	
		JobConf jobConf = new JobConf(getConf(), ChukwaArchiveBuilder.class);
		jobConf.addResource(new Path("conf/chukwa-demux-conf.xml"));
		
		jobConf.setInputFormat(SequenceFileInputFormat.class);
		
		jobConf.setMapperClass(IdentityMapper.class);
		jobConf.setReducerClass(IdentityReducer.class);
		
		if (args[0].equalsIgnoreCase("Daily"))
		{
			jobConf.setPartitionerClass(ChukwaArchiveDailyPartitioner.class);
			jobConf.setOutputFormat(ChukwaArchiveDailyOutputFormat.class);
			jobConf.setJobName("Chukwa-DailyArchiveBuilder");
		}
		else if (args[0].equalsIgnoreCase("Hourly"))
		{
			jobConf.setJobName("Chukwa-HourlyArchiveBuilder");
			jobConf.setPartitionerClass(ChukwaArchiveHourlyPartitioner.class);
			jobConf.setOutputFormat(ChukwaArchiveHourlyOutputFormat.class);			
		}
		else if (args[0].equalsIgnoreCase("DataType"))
    {
      jobConf.setJobName("Chukwa-HourlyArchiveBuilder-DataType");
      int reduceCount = jobConf.getInt("chukwaArchiveBuilder.reduceCount", 1);
      log.info("Reduce Count:" + reduceCount);
      jobConf.setNumReduceTasks(reduceCount);
      
      jobConf.setPartitionerClass(ChukwaArchiveDataTypePartitioner.class);
      jobConf.setOutputFormat(ChukwaArchiveDataTypeOutputFormat.class);     
    }
		else if (args[0].equalsIgnoreCase("Stream"))
    {
      jobConf.setJobName("Chukwa-HourlyArchiveBuilder-Stream");
      int reduceCount = jobConf.getInt("chukwaArchiveBuilder.reduceCount", 1);
      log.info("Reduce Count:" + reduceCount);
      jobConf.setNumReduceTasks(reduceCount);
      
      jobConf.setPartitionerClass(ChukwaArchiveStreamNamePartitioner.class);
      jobConf.setOutputFormat(ChukwaArchiveStreamNameOutputFormat.class);     
    }
		else
		{
			System.out.println("ERROR: Wrong Time partionning: "
					+ args[0] + " instead of [Stream/DataType/Hourly/Daily].");
			return printUsage();
		}

    
		jobConf.set("mapred.compress.map.output", "true");
		jobConf.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.LzoCodec");
		jobConf.set("mapred.output.compress", "true");
		jobConf.set("mapred.output.compression.type", "BLOCK");
	
		
		
		jobConf.setOutputKeyClass(ChukwaArchiveKey.class);
		jobConf.setOutputValueClass(ChunkImpl.class);
	
		FileInputFormat.setInputPaths(jobConf, args[1]);
		FileOutputFormat.setOutputPath(jobConf, new Path(args[2]));

		JobClient.runJob(jobConf);
		return 0;
	}

	public static void main(String[] args) throws Exception
	{
		int res = ToolRunner.run(new Configuration(),
				new ChukwaArchiveBuilder(), args);
		System.exit(res);
	}
}
