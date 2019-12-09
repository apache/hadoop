/**
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
package org.apache.hadoop.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A sleep job whose mappers create 1MB buffer for every record.
 */
public class GrowingSleepJob extends SleepJob {
  private static final Logger LOG =
      LoggerFactory.getLogger(GrowingSleepJob.class);

  public static class GrowingSleepMapper extends SleepMapper {
    private final int MB = 1024 * 1024;
    private ArrayList<byte[]> bytes = new ArrayList<>();

    @Override
    public void map(IntWritable key, IntWritable value, Context context)
        throws IOException, InterruptedException {
      super.map(key, value, context);
      long free = Runtime.getRuntime().freeMemory();
      if (free > 32 * MB) {
        LOG.info("Free memory = " + free +
            " bytes. Creating 1 MB on the heap.");
        bytes.add(new byte[MB]);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new GrowingSleepJob(), args);
    System.exit(res);
  }

  @Override
  public Job createJob(int numMapper, int numReducer,
                       long mapSleepTime, int mapSleepCount,
                       long reduceSleepTime, int reduceSleepCount)
      throws IOException {
    Job job = super.createJob(numMapper, numReducer, mapSleepTime,
        mapSleepCount, reduceSleepTime, reduceSleepCount);
    job.setMapperClass(GrowingSleepMapper.class);
    job.setJobName("Growing sleep job");
    return job;
  }
}
