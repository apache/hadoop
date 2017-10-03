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
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRConfig;

/**
 * Manipulate the working area for the transient store for maps and reduces.
 *
 * This class is used by map and reduce tasks to identify the directories that
 * they need to write to/read from for intermediate files. The callers of
 * these methods are from child space.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class YarnOutputFiles extends MapOutputFile {

  private JobConf conf;

  private static final String JOB_OUTPUT_DIR = "output";
  private static final String SPILL_FILE_PATTERN = "%s_spill_%d.out";
  private static final String SPILL_INDEX_FILE_PATTERN = SPILL_FILE_PATTERN
      + ".index";

  public YarnOutputFiles() {
  }

  // assume configured to $localdir/usercache/$user/appcache/$appId
  private LocalDirAllocator lDirAlloc = 
    new LocalDirAllocator(MRConfig.LOCAL_DIR);

  private Path getAttemptOutputDir() {
    return new Path(JOB_OUTPUT_DIR, conf.get(JobContext.TASK_ATTEMPT_ID));
  }
  
  /**
   * Return the path to local map output file created earlier
   * 
   * @return path
   * @throws IOException
   */
  public Path getOutputFile() throws IOException {
    Path attemptOutput =
      new Path(getAttemptOutputDir(), MAP_OUTPUT_FILENAME_STRING);
    return lDirAlloc.getLocalPathToRead(attemptOutput.toString(), conf);
  }

  /**
   * Create a local map output file name.
   * 
   * @param size the size of the file
   * @return path
   * @throws IOException
   */
  public Path getOutputFileForWrite(long size) throws IOException {
    Path attemptOutput = 
      new Path(getAttemptOutputDir(), MAP_OUTPUT_FILENAME_STRING);
    return lDirAlloc.getLocalPathForWrite(attemptOutput.toString(), size, conf);
  }

  /**
   * Create a local map output file name on the same volume.
   */
  public Path getOutputFileForWriteInVolume(Path existing) {
    Path outputDir = new Path(existing.getParent(), JOB_OUTPUT_DIR);
    Path attemptOutputDir = new Path(outputDir,
        conf.get(JobContext.TASK_ATTEMPT_ID));
    return new Path(attemptOutputDir, MAP_OUTPUT_FILENAME_STRING);
  }

  /**
   * Return the path to a local map output index file created earlier
   * 
   * @return path
   * @throws IOException
   */
  public Path getOutputIndexFile() throws IOException {
    Path attemptIndexOutput =
      new Path(getAttemptOutputDir(), MAP_OUTPUT_FILENAME_STRING +
                                      MAP_OUTPUT_INDEX_SUFFIX_STRING);
    return lDirAlloc.getLocalPathToRead(attemptIndexOutput.toString(), conf);
  }

  /**
   * Create a local map output index file name.
   * 
   * @param size the size of the file
   * @return path
   * @throws IOException
   */
  public Path getOutputIndexFileForWrite(long size) throws IOException {
    Path attemptIndexOutput =
      new Path(getAttemptOutputDir(), MAP_OUTPUT_FILENAME_STRING +
                                      MAP_OUTPUT_INDEX_SUFFIX_STRING);
    return lDirAlloc.getLocalPathForWrite(attemptIndexOutput.toString(),
        size, conf);
  }

  /**
   * Create a local map output index file name on the same volume.
   */
  public Path getOutputIndexFileForWriteInVolume(Path existing) {
    Path outputDir = new Path(existing.getParent(), JOB_OUTPUT_DIR);
    Path attemptOutputDir = new Path(outputDir,
        conf.get(JobContext.TASK_ATTEMPT_ID));
    return new Path(attemptOutputDir, MAP_OUTPUT_FILENAME_STRING +
                                      MAP_OUTPUT_INDEX_SUFFIX_STRING);
  }

  /**
   * Return a local map spill file created earlier.
   * 
   * @param spillNumber the number
   * @return path
   * @throws IOException
   */
  public Path getSpillFile(int spillNumber) throws IOException {
    return lDirAlloc.getLocalPathToRead(
        String.format(SPILL_FILE_PATTERN,
            conf.get(JobContext.TASK_ATTEMPT_ID), spillNumber), conf);
  }

  /**
   * Create a local map spill file name.
   * 
   * @param spillNumber the number
   * @param size the size of the file
   * @return path
   * @throws IOException
   */
  public Path getSpillFileForWrite(int spillNumber, long size)
      throws IOException {
    return lDirAlloc.getLocalPathForWrite(
        String.format(SPILL_FILE_PATTERN,
            conf.get(JobContext.TASK_ATTEMPT_ID), spillNumber), size, conf);
  }

  /**
   * Return a local map spill index file created earlier
   * 
   * @param spillNumber the number
   * @return path
   * @throws IOException
   */
  public Path getSpillIndexFile(int spillNumber) throws IOException {
    return lDirAlloc.getLocalPathToRead(
        String.format(SPILL_INDEX_FILE_PATTERN,
            conf.get(JobContext.TASK_ATTEMPT_ID), spillNumber), conf);
  }

  /**
   * Create a local map spill index file name.
   * 
   * @param spillNumber the number
   * @param size the size of the file
   * @return path
   * @throws IOException
   */
  public Path getSpillIndexFileForWrite(int spillNumber, long size)
      throws IOException {
    return lDirAlloc.getLocalPathForWrite(
        String.format(SPILL_INDEX_FILE_PATTERN,
            conf.get(JobContext.TASK_ATTEMPT_ID), spillNumber), size, conf);
  }

  /**
   * Return a local reduce input file created earlier
   * 
   * @param mapId a map task id
   * @return path
   * @throws IOException 
   */
  public Path getInputFile(int mapId) throws IOException {
    throw new UnsupportedOperationException("Incompatible with LocalRunner");
  }

  /**
   * Create a local reduce input file name.
   * 
   * @param mapId a map task id
   * @param size the size of the file
   * @return path
   * @throws IOException
   */
  public Path getInputFileForWrite(org.apache.hadoop.mapreduce.TaskID mapId,
      long size) throws IOException {
    return lDirAlloc.getLocalPathForWrite(String.format(
        REDUCE_INPUT_FILE_FORMAT_STRING,
        getAttemptOutputDir().toString(), mapId.getId()),
        size, conf);
  }

  /** Removes all of the files related to a task. */
  public void removeAll() throws IOException {
    throw new UnsupportedOperationException("Incompatible with LocalRunner");
  }

  @Override
  public void setConf(Configuration conf) {
    if (conf instanceof JobConf) {
      this.conf = (JobConf) conf;
    } else {
      this.conf = new JobConf(conf);
    }
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
  
}
