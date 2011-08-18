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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.fs.Path;

/**
 * Manipulate the working area for the transient store for maps and reduces.
 *
 * This class is used by map and reduce tasks to identify the directories that
 * they need to write to/read from for intermediate files. The callers of
 * these methods are from child space and see mapreduce.cluster.local.dir as
 * taskTracker/jobCache/jobId/attemptId
 * This class should not be used from TaskTracker space.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public abstract class MapOutputFile implements Configurable {

  private Configuration conf;

  static final String MAP_OUTPUT_FILENAME_STRING = "file.out";
  static final String MAP_OUTPUT_INDEX_SUFFIX_STRING = ".index";
  static final String REDUCE_INPUT_FILE_FORMAT_STRING = "%s/map_%d.out";

  public MapOutputFile() {
  }

  /**
   * Return the path to local map output file created earlier
   *
   * @return path
   * @throws IOException
   */
  public abstract Path getOutputFile() throws IOException;

  /**
   * Create a local map output file name.
   *
   * @param size the size of the file
   * @return path
   * @throws IOException
   */
  public abstract Path getOutputFileForWrite(long size) throws IOException;

  /**
   * Create a local map output file name on the same volume.
   */
  public abstract Path getOutputFileForWriteInVolume(Path existing);

  /**
   * Return the path to a local map output index file created earlier
   *
   * @return path
   * @throws IOException
   */
  public abstract Path getOutputIndexFile() throws IOException;

  /**
   * Create a local map output index file name.
   *
   * @param size the size of the file
   * @return path
   * @throws IOException
   */
  public abstract Path getOutputIndexFileForWrite(long size) throws IOException;

  /**
   * Create a local map output index file name on the same volume.
   */
  public abstract Path getOutputIndexFileForWriteInVolume(Path existing);

  /**
   * Return a local map spill file created earlier.
   *
   * @param spillNumber the number
   * @return path
   * @throws IOException
   */
  public abstract Path getSpillFile(int spillNumber) throws IOException;

  /**
   * Create a local map spill file name.
   *
   * @param spillNumber the number
   * @param size the size of the file
   * @return path
   * @throws IOException
   */
  public abstract Path getSpillFileForWrite(int spillNumber, long size)
      throws IOException;

  /**
   * Return a local map spill index file created earlier
   *
   * @param spillNumber the number
   * @return path
   * @throws IOException
   */
  public abstract Path getSpillIndexFile(int spillNumber) throws IOException;

  /**
   * Create a local map spill index file name.
   *
   * @param spillNumber the number
   * @param size the size of the file
   * @return path
   * @throws IOException
   */
  public abstract Path getSpillIndexFileForWrite(int spillNumber, long size)
      throws IOException;

  /**
   * Return a local reduce input file created earlier
   *
   * @param mapId a map task id
   * @return path
   * @throws IOException
   */
  public abstract Path getInputFile(int mapId) throws IOException;

  /**
   * Create a local reduce input file name.
   *
   * @param mapId a map task id
   * @param size the size of the file
   * @return path
   * @throws IOException
   */
  public abstract Path getInputFileForWrite(
      org.apache.hadoop.mapreduce.TaskID mapId, long size) throws IOException;

  /** Removes all of the files related to a task. */
  public abstract void removeAll() throws IOException;

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

}
