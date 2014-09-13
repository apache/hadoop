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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.TaskID;

/**
 * base class of output files manager.
 */
@InterfaceAudience.Private
public interface NativeTaskOutput {

  /**
   * Return the path to local map output file created earlier
   */
  public Path getOutputFile() throws IOException;

  /**
   * Create a local map output file name.
   * 
   * @param size the size of the file
   */
  public Path getOutputFileForWrite(long size) throws IOException;

  /**
   * Return the path to a local map output index file created earlier
   */
  public Path getOutputIndexFile() throws IOException;

  /**
   * Create a local map output index file name.
   * 
   * @param size the size of the file
   */
  public Path getOutputIndexFileForWrite(long size) throws IOException;

  /**
   * Return a local map spill file created earlier.
   * 
   * @param spillNumber the number
   */
  public Path getSpillFile(int spillNumber) throws IOException;

  /**
   * Create a local map spill file name.
   * 
   * @param spillNumber the number
   * @param size the size of the file
   */
  public Path getSpillFileForWrite(int spillNumber, long size) throws IOException;

  /**
   * Return a local map spill index file created earlier
   * 
   * @param spillNumber the number
   */
  public Path getSpillIndexFile(int spillNumber) throws IOException;

  /**
   * Create a local map spill index file name.
   * 
    r* @param spillNumber the number
   * @param size the size of the file
   */
  public Path getSpillIndexFileForWrite(int spillNumber, long size) throws IOException;

  /**
   * Return a local reduce input file created earlier
   * 
   * @param mapId a map task id
   */
  public Path getInputFile(int mapId) throws IOException;

  /**
   * Create a local reduce input file name.
   * 
   * @param mapId a map task id
   * @param size the size of the file
   */
  public Path getInputFileForWrite(TaskID mapId, long size, Configuration conf) throws IOException;

  /** Removes all of the files related to a task. */
  public void removeAll() throws IOException;

  public String getOutputName(int partition);
}
