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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;

/**
 * Manipulate the working area for the transient store for maps and reduces.
 */ 
class MapOutputFile {

  private JobConf conf;
  private JobID jobId;
  
  MapOutputFile() {
  }

  MapOutputFile(JobID jobId) {
    this.jobId = jobId;
  }

  private LocalDirAllocator lDirAlloc = 
                            new LocalDirAllocator("mapred.local.dir");
  
  /** Return the path to local map output file created earlier
   * @param mapTaskId a map task id
   */
  public Path getOutputFile(TaskAttemptID mapTaskId)
    throws IOException {
    return lDirAlloc.getLocalPathToRead(TaskTracker.getIntermediateOutputDir(
                       jobId.toString(), mapTaskId.toString())
                       + "/file.out", conf);
  }

  /** Create a local map output file name.
   * @param mapTaskId a map task id
   * @param size the size of the file
   */
  public Path getOutputFileForWrite(TaskAttemptID mapTaskId, long size)
    throws IOException {
    return lDirAlloc.getLocalPathForWrite(TaskTracker.getIntermediateOutputDir(
                       jobId.toString(), mapTaskId.toString())
                       + "/file.out", size, conf);
  }

  /** Return the path to a local map output index file created earlier
   * @param mapTaskId a map task id
   */
  public Path getOutputIndexFile(TaskAttemptID mapTaskId)
    throws IOException {
    return lDirAlloc.getLocalPathToRead(TaskTracker.getIntermediateOutputDir(
                       jobId.toString(), mapTaskId.toString())
                       + "/file.out.index", conf);
  }

  /** Create a local map output index file name.
   * @param mapTaskId a map task id
   * @param size the size of the file
   */
  public Path getOutputIndexFileForWrite(TaskAttemptID mapTaskId, long size)
    throws IOException {
    return lDirAlloc.getLocalPathForWrite(TaskTracker.getIntermediateOutputDir(
                       jobId.toString(), mapTaskId.toString())
                       + "/file.out.index", 
                       size, conf);
  }

  /** Return a local map spill file created earlier.
   * @param mapTaskId a map task id
   * @param spillNumber the number
   */
  public Path getSpillFile(TaskAttemptID mapTaskId, int spillNumber)
    throws IOException {
    return lDirAlloc.getLocalPathToRead(TaskTracker.getIntermediateOutputDir(
                       jobId.toString(), mapTaskId.toString())
                       + "/spill" 
                       + spillNumber + ".out", conf);
  }

  /** Create a local map spill file name.
   * @param mapTaskId a map task id
   * @param spillNumber the number
   * @param size the size of the file
   */
  public Path getSpillFileForWrite(TaskAttemptID mapTaskId, int spillNumber, 
         long size) throws IOException {
    return lDirAlloc.getLocalPathForWrite(TaskTracker.getIntermediateOutputDir(
                       jobId.toString(), mapTaskId.toString())
                       + "/spill" + 
                       spillNumber + ".out", size, conf);
  }

  /** Return a local map spill index file created earlier
   * @param mapTaskId a map task id
   * @param spillNumber the number
   */
  public Path getSpillIndexFile(TaskAttemptID mapTaskId, int spillNumber)
    throws IOException {
    return lDirAlloc.getLocalPathToRead(TaskTracker.getIntermediateOutputDir(
                       jobId.toString(), mapTaskId.toString())
                       + "/spill" + 
                       spillNumber + ".out.index", conf);
  }

  /** Create a local map spill index file name.
   * @param mapTaskId a map task id
   * @param spillNumber the number
   * @param size the size of the file
   */
  public Path getSpillIndexFileForWrite(TaskAttemptID mapTaskId, int spillNumber,
         long size) throws IOException {
    return lDirAlloc.getLocalPathForWrite(TaskTracker.getIntermediateOutputDir(
                       jobId.toString(), mapTaskId.toString())
                       + "/spill" + spillNumber + 
                       ".out.index", size, conf);
  }

  /** Return a local reduce input file created earlier
   * @param mapTaskId a map task id
   * @param reduceTaskId a reduce task id
   */
  public Path getInputFile(int mapId, TaskAttemptID reduceTaskId)
    throws IOException {
    // TODO *oom* should use a format here
    return lDirAlloc.getLocalPathToRead(TaskTracker.getIntermediateOutputDir(
                       jobId.toString(), reduceTaskId.toString())
                       + "/map_" + mapId + ".out",
                       conf);
  }

  /** Create a local reduce input file name.
   * @param mapTaskId a map task id
   * @param reduceTaskId a reduce task id
   * @param size the size of the file
   */
  public Path getInputFileForWrite(TaskID mapId, TaskAttemptID reduceTaskId, 
                                   long size)
    throws IOException {
    // TODO *oom* should use a format here
    return lDirAlloc.getLocalPathForWrite(TaskTracker.getIntermediateOutputDir(
                       jobId.toString(), reduceTaskId.toString())
                       + "/map_" + mapId.getId() + ".out", 
                       size, conf);
  }

  /** Removes all of the files related to a task. */
  public void removeAll(TaskAttemptID taskId) throws IOException {
    conf.deleteLocalFiles(TaskTracker.getIntermediateOutputDir(
                          jobId.toString(), taskId.toString())
);
  }

  public void setConf(Configuration conf) {
    if (conf instanceof JobConf) {
      this.conf = (JobConf) conf;
    } else {
      this.conf = new JobConf(conf);
    }
  }
  
  public void setJobId(JobID jobId) {
    this.jobId = jobId;
  }

}
