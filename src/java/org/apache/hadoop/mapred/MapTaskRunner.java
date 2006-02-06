/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.apache.hadoop.conf.*;

import java.io.*;

/** Runs a map task. */
class MapTaskRunner extends TaskRunner {
  private MapOutputFile mapOutputFile;

  public MapTaskRunner(Task task, TaskTracker tracker, Configuration conf) {
    super(task, tracker, conf);
    this.mapOutputFile = new MapOutputFile();
    this.mapOutputFile.setConf(conf);
  }
  
  /** Delete any temporary files from previous failed attempts. */
  public boolean prepare() throws IOException {
    this.mapOutputFile.removeAll(getTask().getTaskId());
    return true;
  }

  /** Delete all of the temporary map output files. */
  public void close() throws IOException {
    LOG.info(getTask()+" done; removing files.");
    this.mapOutputFile.removeAll(getTask().getTaskId());
  }
}
