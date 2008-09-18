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

package org.apache.hadoop.streaming;

import org.apache.hadoop.util.ToolRunner;

/** The main entrypoint. Usually invoked with the script bin/hadoopStreaming
 * or bin/hadoop har hadoop-streaming.jar args.
 * It passes all the args to StreamJob which handles all the arguments.
 */
public class HadoopStreaming {

  public static void main(String[] args) throws Exception {
    int returnStatus = 0;
    StreamJob job = new StreamJob();
    returnStatus = ToolRunner.run(job, args);
    if (returnStatus != 0) {
      System.err.println("Streaming Job Failed!");
      System.exit(returnStatus);
    }
  }
}
