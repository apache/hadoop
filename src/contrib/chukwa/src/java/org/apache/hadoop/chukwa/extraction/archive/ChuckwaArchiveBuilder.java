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

public class ChuckwaArchiveBuilder extends Configured implements Tool
{
	static Logger log = Logger.getLogger(ChuckwaArchiveBuilder.class);

	static int printUsage()
	{
		System.out
				.println("ChuckwaArchiveBuilder <Daily/Hourly> <input> <output>");
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
	
		JobConf conf = new JobConf(getConf(), ChuckwaArchiveBuilder.class);

		
		conf.setInputFormat(SequenceFileInputFormat.class);
		
		conf.setMapperClass(IdentityMapper.class);
		conf.setReducerClass(IdentityReducer.class);
		
		if (args[0].equalsIgnoreCase("Daily"))
		{
			conf.setPartitionerClass(ChukwaArchiveDailyPartitioner.class);
			conf.setOutputFormat(ChukwaArchiveDailyOutputFormat.class);
			conf.setJobName("Chukwa-DailyArchiveBuilder");
		}
		else if (args[0].equalsIgnoreCase("Hourly"))
		{
			conf.setJobName("Chukwa-HourlyArchiveBuilder");
			conf.setPartitionerClass(ChukwaArchiveHourlyPartitioner.class);
			conf.setOutputFormat(ChukwaArchiveHourlyOutputFormat.class);			
		}
		else
		{
			System.out.println("ERROR: Wrong Time partionning: "
					+ args[0] + " instead of [Hourly/Daily].");
			return printUsage();
		}

		
		conf.setOutputKeyClass(ChukwaArchiveKey.class);
		conf.setOutputValueClass(ChunkImpl.class);
				
		//FIXME need compression - read config
		//conf.setCompressMapOutput(true);
		//conf.setMapOutputCompressorClass(LzoCodec.class);
		
		//
		
		FileInputFormat.setInputPaths(conf, args[1]);
		FileOutputFormat.setOutputPath(conf, new Path(args[2]));

		JobClient.runJob(conf);
		return 0;
	}

	public static void main(String[] args) throws Exception
	{
		int res = ToolRunner.run(new Configuration(),
				new ChuckwaArchiveBuilder(), args);
		System.exit(res);
	}
}
