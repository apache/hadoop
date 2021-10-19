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
package org.apache.hadoop.tools.dynamometer.workloadgenerator.audit;

import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * AuditReplayReducer aggregates the returned latency values from
 * {@link AuditReplayMapper} and sums them up by {@link UserCommandKey}, which
 * combines the user's id that ran the command and the type of the command
 * (READ/WRITE).
 */
public class AuditReplayReducer extends Reducer<UserCommandKey,
    CountTimeWritable, UserCommandKey, CountTimeWritable> {

  @Override
  protected void reduce(UserCommandKey key, Iterable<CountTimeWritable> values,
      Context context) throws IOException, InterruptedException {
    long countSum = 0;
    long timeSum = 0;
    for (CountTimeWritable v : values) {
      countSum += v.getCount();
      timeSum += v.getTime();
    }
    context.write(key, new CountTimeWritable(countSum, timeSum));
  }
}
