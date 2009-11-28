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

package org.apache.hadoop.sqoop.testutil;

import java.util.List;

/**
 * Sets common arguments to Sqoop sub-instances for testing.
 */
public final class CommonArgs {

  private CommonArgs() {
  }

  /** Craft a list of arguments that are common to (virtually) all Sqoop programs
   */
  public static void addHadoopFlags(List<String> args) {
    args.add("-D");
    args.add("mapreduce.jobtracker.address=local");
    args.add("-D");
    args.add("mapreduce.job.maps=1");
    args.add("-D");
    args.add("fs.default.name=file:///");
    args.add("-D");
    args.add("jobclient.completion.poll.interval=50");
    args.add("-D");
    args.add("jobclient.progress.monitor.poll.interval=50");
  }
}
