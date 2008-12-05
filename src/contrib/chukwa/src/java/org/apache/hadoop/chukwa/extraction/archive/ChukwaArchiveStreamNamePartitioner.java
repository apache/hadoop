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
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

public class ChukwaArchiveStreamNamePartitioner<K, V> 
	implements Partitioner<ChukwaArchiveKey,ChunkImpl>
{
	public void configure(JobConf arg0)
	{}

	public int getPartition(ChukwaArchiveKey key,ChunkImpl chunk, int numReduceTasks)
	{
		
		 return ( (chunk.getSource() + "/" + chunk.getStreamName()).hashCode() & Integer.MAX_VALUE) % numReduceTasks;
	}

}
