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

package org.apache.hadoop.mapred.lib;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class CombineFileSplit extends 
    org.apache.hadoop.mapreduce.lib.input.CombineFileSplit 
    implements InputSplit {

  private JobConf job;

  public CombineFileSplit() {
  }
  
  public CombineFileSplit(JobConf job, Path[] files, long[] start, 
          long[] lengths, String[] locations) {
    super(files, start, lengths, locations);
    this.job = job;
  }

  public CombineFileSplit(JobConf job, Path[] files, long[] lengths) {
    super(files, lengths);
    this.job = job;
  }
  
  /**
   * Copy constructor
   */
  public CombineFileSplit(CombineFileSplit old) throws IOException {
    super(old);
  }

  public JobConf getJob() {
    return job;
  }
}
