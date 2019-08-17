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

package org.apache.hadoop.tools.mapred.lib;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.tools.DistCpConstants;

import java.io.IOException;

/**
 * Class to initialize the DynamicInputChunk invariants.
 */
class DynamicInputChunkContext<K, V> {

  private static Logger LOG = LoggerFactory.getLogger(DynamicInputChunkContext.class);
  private Configuration configuration;
  private Path chunkRootPath = null;
  private String chunkFilePrefix;
  private FileSystem fs;
  private int numChunksLeft = -1; // Un-initialized before 1st dir-scan.

  public DynamicInputChunkContext(Configuration config)
      throws IOException {
    this.configuration = config;
    Path listingFilePath = new Path(getListingFilePath(configuration));
    chunkRootPath = new Path(listingFilePath.getParent(), "chunkDir");
    fs = chunkRootPath.getFileSystem(configuration);
    chunkFilePrefix = listingFilePath.getName() + ".chunk.";
  }

  public Configuration getConfiguration() {
    return configuration;
  }

  public Path getChunkRootPath() {
    return chunkRootPath;
  }

  public String getChunkFilePrefix() {
    return chunkFilePrefix;
  }

  public FileSystem getFs() {
    return fs;
  }

  private static String getListingFilePath(Configuration configuration) {
    final String listingFileString = configuration.get(
        DistCpConstants.CONF_LABEL_LISTING_FILE_PATH, "");
    assert !listingFileString.equals("") : "Listing file not found.";
    return listingFileString;
  }

  public int getNumChunksLeft() {
    return numChunksLeft;
  }

  public DynamicInputChunk acquire(TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {

    String taskId
        = taskAttemptContext.getTaskAttemptID().getTaskID().toString();
    Path acquiredFilePath = new Path(getChunkRootPath(), taskId);

    if (fs.exists(acquiredFilePath)) {
      LOG.info("Acquiring pre-assigned chunk: " + acquiredFilePath);
      return new DynamicInputChunk(acquiredFilePath, taskAttemptContext, this);
    }

    for (FileStatus chunkFile : getListOfChunkFiles()) {
      if (fs.rename(chunkFile.getPath(), acquiredFilePath)) {
        LOG.info(taskId + " acquired " + chunkFile.getPath());
        return new DynamicInputChunk(acquiredFilePath, taskAttemptContext,
            this);
      }
    }
    return null;
  }

  public DynamicInputChunk createChunkForWrite(String chunkId)
      throws IOException {
    return new DynamicInputChunk(chunkId, this);
  }

  public FileStatus [] getListOfChunkFiles() throws IOException {
    Path chunkFilePattern = new Path(chunkRootPath, chunkFilePrefix + "*");
    FileStatus chunkFiles[] = fs.globStatus(chunkFilePattern);
    numChunksLeft = chunkFiles.length;
    return chunkFiles;
  }
}
