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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.chukwa.ChukwaArchiveKey;
import org.apache.hadoop.chukwa.ChunkImpl;
import org.apache.hadoop.chukwa.extraction.demux.processor.mapper.ProcessorFactory;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class Demux extends Configured implements Tool
{
	static Logger log = Logger.getLogger(Demux.class);
	
	public static class MapClass extends MapReduceBase implements
			Mapper<ChukwaArchiveKey, ChunkImpl , Text, ChukwaRecord>
	{
		
		public void map(ChukwaArchiveKey key, ChunkImpl chunk,
				OutputCollector<Text, ChukwaRecord> output, Reporter reporter)
				throws IOException
		{
			try {
				log.info("Entry: ["+ chunk.getData() + "] EventType: [" + chunk.getDataType() + "]");
				
				ProcessorFactory.getProcessor(chunk.getDataType()).process(chunk, output, reporter);
			} catch(Exception e) {
				e.printStackTrace();
			}
		}
	}

	static int printUsage() {
		System.out
				.println("Demux [-m <maps>] [-r <reduces>] <input> <output>");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	public int run(String[] args) throws Exception
	{
		JobConf conf = new JobConf(getConf(), Demux.class);

		conf.setJobName("Chukwa-Demux");
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setMapperClass(Demux.MapClass.class);
		conf.setPartitionerClass(ChukwaRecordPartitioner.class);
		conf.setReducerClass(IdentityReducer.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(ChukwaRecord.class);
		conf.setOutputFormat(ChukwaRecordOutputFormat.class);
		//
		
		List<String> other_args = new ArrayList<String>();
		for (int i = 0; i < args.length; ++i) {
			try {
				if ("-m".equals(args[i])) {
					conf.setNumMapTasks(Integer.parseInt(args[++i]));
				} else if ("-r".equals(args[i])) {
					conf.setNumReduceTasks(Integer.parseInt(args[++i]));
				} else 	{
					other_args.add(args[i]);
				}
			} catch (NumberFormatException except) {
				System.out.println("ERROR: Integer expected instead of "
						+ args[i]);
				return printUsage();
			} catch (ArrayIndexOutOfBoundsException except) {
				System.out.println("ERROR: Required parameter missing from "
						+ args[i - 1]);
				return printUsage();
			}
		}
		// Make sure there are exactly 2 parameters left.
		if (other_args.size() != 2) {
			System.out.println("ERROR: Wrong number of parameters: "
					+ other_args.size() + " instead of 2.");
			return printUsage();
		}
		
		FileInputFormat.setInputPaths(conf, other_args.get(0));
		FileOutputFormat.setOutputPath(conf, new Path(other_args.get(1)));

		JobClient.runJob(conf);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new Demux(), args);
		System.exit(res);
	}

}
