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
package org.apache.hadoop.mapred.nativetask.util;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskID;

@InterfaceAudience.Private
public class LocalJobOutputFiles implements NativeTaskOutput {

  static final String TASKTRACKER_OUTPUT = "output";
  static final String REDUCE_INPUT_FILE_FORMAT_STRING = "%s/map_%d.out";
  static final String SPILL_FILE_FORMAT_STRING = "%s/spill%d.out";
  static final String SPILL_INDEX_FILE_FORMAT_STRING = "%s/spill%d.out.index";
  static final String OUTPUT_FILE_FORMAT_STRING = "%s/file.out";
  static final String OUTPUT_FILE_INDEX_FORMAT_STRING = "%s/file.out.index";

  private JobConf conf;
  private LocalDirAllocator lDirAlloc = new LocalDirAllocator("mapred.local.dir");

  public LocalJobOutputFiles(Configuration conf, String id) {
    this.conf = new JobConf(conf);
  }

  /**
   * Return the path to local map output file created earlier
   */
  public Path getOutputFile() throws IOException {
    String path = String.format(OUTPUT_FILE_FORMAT_STRING, TASKTRACKER_OUTPUT);
    return lDirAlloc.getLocalPathToRead(path, conf);
  }

  /**
   * Create a local map output file name.
   * 
   * @param size the size of the file
   */
  public Path getOutputFileForWrite(long size) throws IOException {
    String path = String.format(OUTPUT_FILE_FORMAT_STRING, TASKTRACKER_OUTPUT);
    return lDirAlloc.getLocalPathForWrite(path, size, conf);
  }

  /**
   * Return the path to a local map output index file created earlier
   */
  public Path getOutputIndexFile() throws IOException {
    String path = String.format(OUTPUT_FILE_INDEX_FORMAT_STRING, TASKTRACKER_OUTPUT);
    return lDirAlloc.getLocalPathToRead(path, conf);
  }

  /**
   * Create a local map output index file name.
   * 
   * @param size the size of the file
   */
  public Path getOutputIndexFileForWrite(long size) throws IOException {
    String path = String.format(OUTPUT_FILE_INDEX_FORMAT_STRING, TASKTRACKER_OUTPUT);
    return lDirAlloc.getLocalPathForWrite(path, size, conf);
  }

  /**
   * Return a local map spill file created earlier.
   * 
   * @param spillNumber the number
   */
  public Path getSpillFile(int spillNumber) throws IOException {
    String path = String.format(SPILL_FILE_FORMAT_STRING, TASKTRACKER_OUTPUT, spillNumber);
    return lDirAlloc.getLocalPathToRead(path, conf);
  }

  /**
   * Create a local map spill file name.
   * 
   * @param spillNumber the number
   * @param size the size of the file
   */
  public Path getSpillFileForWrite(int spillNumber, long size) throws IOException {
    String path = String.format(SPILL_FILE_FORMAT_STRING, TASKTRACKER_OUTPUT, spillNumber);
    return lDirAlloc.getLocalPathForWrite(path, size, conf);
  }

  /**
   * Return a local map spill index file created earlier
   * 
   * @param spillNumber the number
   */
  public Path getSpillIndexFile(int spillNumber) throws IOException {
    String path = String
.format(SPILL_INDEX_FILE_FORMAT_STRING, TASKTRACKER_OUTPUT, spillNumber);
    return lDirAlloc.getLocalPathToRead(path, conf);
  }

  /**
   * Create a local map spill index file name.
   * 
   * @param spillNumber the number
   * @param size the size of the file
   */
  public Path getSpillIndexFileForWrite(int spillNumber, long size) throws IOException {
    String path = String
.format(SPILL_INDEX_FILE_FORMAT_STRING, TASKTRACKER_OUTPUT, spillNumber);
    return lDirAlloc.getLocalPathForWrite(path, size, conf);
  }

  /**
   * Return a local reduce input file created earlier
   * 
   * @param mapId a map task id
   */
  public Path getInputFile(int mapId) throws IOException {
    return lDirAlloc.getLocalPathToRead(
        String.format(REDUCE_INPUT_FILE_FORMAT_STRING, TASKTRACKER_OUTPUT, Integer.valueOf(mapId)),
        conf);
  }

  /**
   * Create a local reduce input file name.
   * 
   * @param mapId a map task id
   * @param size the size of the file
   */
  public Path getInputFileForWrite(TaskID mapId, long size, Configuration conf)
    throws IOException {
    return lDirAlloc.getLocalPathForWrite(
        String.format(REDUCE_INPUT_FILE_FORMAT_STRING, TASKTRACKER_OUTPUT, mapId.getId()), size,
        conf);
  }

  /** Removes all of the files related to a task. */
  public void removeAll() throws IOException {
    conf.deleteLocalFiles(TASKTRACKER_OUTPUT);
  }

  public String getOutputName(int partition) {
    return String.format("part-%05d", partition);
  }

}
