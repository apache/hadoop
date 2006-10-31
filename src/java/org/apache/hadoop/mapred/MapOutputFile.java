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

package org.apache.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;

/**
 * Manipulate the working area for the transient store for maps and reduces.
 */ 
class MapOutputFile {

  private JobConf conf;
  
  /** Create a local map output file name.
   * @param mapTaskId a map task id
   * @param partition a reduce partition
   */
  public Path getOutputFile(String mapTaskId, int partition)
    throws IOException {
    return conf.getLocalPath(mapTaskId+"/part-"+partition+".out");
  }

  /** Create a local reduce input file name.
   * @param mapTaskId a map task id
   * @param reduceTaskId a reduce task id
   */
  public Path getInputFile(int mapId, String reduceTaskId)
    throws IOException {
    // TODO *oom* should use a format here
    return conf.getLocalPath(reduceTaskId+"/map_"+mapId+".out");
  }

  /** Removes all of the files related to a task. */
  public void removeAll(String taskId) throws IOException {
    conf.deleteLocalFiles(taskId);
  }

  /** 
   * Removes all contents of temporary storage.  Called upon 
   * startup, to remove any leftovers from previous run.
   */
  public void cleanupStorage() throws IOException {
    conf.deleteLocalFiles();
  }

  public void setConf(Configuration conf) {
    if (conf instanceof JobConf) {
      this.conf = (JobConf) conf;
    } else {
      this.conf = new JobConf(conf);
    }
  }
}
