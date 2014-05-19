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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.CopyListingFileStatus;
import org.apache.hadoop.tools.DistCpConstants;
import org.apache.hadoop.tools.util.DistCpUtils;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskID;

import java.io.IOException;

/**
 * The DynamicInputChunk represents a single chunk of work, when used in
 * conjunction with the DynamicInputFormat and the DynamicRecordReader.
 * The records in the DynamicInputFormat's input-file are split across various
 * DynamicInputChunks. Each one is claimed and processed in an iteration of
 * a dynamic-mapper. When a DynamicInputChunk has been exhausted, the faster
 * mapper may claim another and process it, until there are no more to be
 * consumed.
 */
class DynamicInputChunk<K, V> {
  private static Log LOG = LogFactory.getLog(DynamicInputChunk.class);

  private static Configuration configuration;
  private static Path chunkRootPath;
  private static String chunkFilePrefix;
  private static int numChunksLeft = -1; // Un-initialized before 1st dir-scan.
  private static FileSystem fs;

  private Path chunkFilePath;
  private SequenceFileRecordReader<K, V> reader;
  private SequenceFile.Writer writer;

  private static void initializeChunkInvariants(Configuration config)
                                                  throws IOException {
    configuration = config;
    Path listingFilePath = new Path(getListingFilePath(configuration));
    chunkRootPath = new Path(listingFilePath.getParent(), "chunkDir");
    fs = chunkRootPath.getFileSystem(configuration);
    chunkFilePrefix = listingFilePath.getName() + ".chunk.";
  }

  private static String getListingFilePath(Configuration configuration) {
    final String listingFileString = configuration.get(
            DistCpConstants.CONF_LABEL_LISTING_FILE_PATH, "");
    assert !listingFileString.equals("") : "Listing file not found.";
    return listingFileString;
  }

  private static boolean areInvariantsInitialized() {
    return chunkRootPath != null;
  }

  private DynamicInputChunk(String chunkId, Configuration configuration)
                                                      throws IOException {
    if (!areInvariantsInitialized())
      initializeChunkInvariants(configuration);

    chunkFilePath = new Path(chunkRootPath, chunkFilePrefix + chunkId);
    openForWrite();
  }


  private void openForWrite() throws IOException {
    writer = SequenceFile.createWriter(
            chunkFilePath.getFileSystem(configuration), configuration,
            chunkFilePath, Text.class, CopyListingFileStatus.class,
            SequenceFile.CompressionType.NONE);

  }

  /**
   * Factory method to create chunk-files for writing to.
   * (For instance, when the DynamicInputFormat splits the input-file into
   * chunks.)
   * @param chunkId String to identify the chunk.
   * @param configuration Configuration, describing the location of the listing-
   * file, file-system for the map-job, etc.
   * @return A DynamicInputChunk, corresponding to a chunk-file, with the name
   * incorporating the chunk-id.
   * @throws IOException Exception on failure to create the chunk.
   */
  public static DynamicInputChunk createChunkForWrite(String chunkId,
                          Configuration configuration) throws IOException {
    return new DynamicInputChunk(chunkId, configuration);
  }

  /**
   * Method to write records into a chunk.
   * @param key Key from the listing file.
   * @param value Corresponding value from the listing file.
   * @throws IOException Exception onf failure to write to the file.
   */
  public void write(Text key, CopyListingFileStatus value) throws IOException {
    writer.append(key, value);
  }

  /**
   * Closes streams opened to the chunk-file.
   */
  public void close() {
    IOUtils.cleanup(LOG, reader, writer);
  }

  /**
   * Reassigns the chunk to a specified Map-Task, for consumption.
   * @param taskId The Map-Task to which a the chunk is to be reassigned.
   * @throws IOException Exception on failure to reassign.
   */
  public void assignTo(TaskID taskId) throws IOException {
    Path newPath = new Path(chunkRootPath, taskId.toString());
    if (!fs.rename(chunkFilePath, newPath)) {
      LOG.warn(chunkFilePath + " could not be assigned to " + taskId);
    }
  }

  private DynamicInputChunk(Path chunkFilePath,
                            TaskAttemptContext taskAttemptContext)
                                   throws IOException, InterruptedException {
    if (!areInvariantsInitialized())
      initializeChunkInvariants(taskAttemptContext.getConfiguration());

    this.chunkFilePath = chunkFilePath;
    openForRead(taskAttemptContext);
  }

  private void openForRead(TaskAttemptContext taskAttemptContext)
          throws IOException, InterruptedException {
    reader = new SequenceFileRecordReader<K, V>();
    reader.initialize(new FileSplit(chunkFilePath, 0,
            DistCpUtils.getFileSize(chunkFilePath, configuration), null),
            taskAttemptContext);
  }

  /**
   * Factory method that
   * 1. acquires a chunk for the specified map-task attempt
   * 2. returns a DynamicInputChunk associated with the acquired chunk-file.
   * @param taskAttemptContext The attempt-context for the map task that's
   * trying to acquire a chunk.
   * @return The acquired dynamic-chunk. The chunk-file is renamed to the
   * attempt-id (from the attempt-context.)
   * @throws IOException Exception on failure.
   * @throws InterruptedException Exception on failure.
   */
  public static DynamicInputChunk acquire(TaskAttemptContext taskAttemptContext)
                                      throws IOException, InterruptedException {
    if (!areInvariantsInitialized())
        initializeChunkInvariants(taskAttemptContext.getConfiguration());

    String taskId
            = taskAttemptContext.getTaskAttemptID().getTaskID().toString();
    Path acquiredFilePath = new Path(chunkRootPath, taskId);

    if (fs.exists(acquiredFilePath)) {
      LOG.info("Acquiring pre-assigned chunk: " + acquiredFilePath);
      return new DynamicInputChunk(acquiredFilePath, taskAttemptContext);
    }

    for (FileStatus chunkFile : getListOfChunkFiles()) {
      if (fs.rename(chunkFile.getPath(), acquiredFilePath)) {
        LOG.info(taskId + " acquired " + chunkFile.getPath());
        return new DynamicInputChunk(acquiredFilePath, taskAttemptContext);
      }
      else
        LOG.warn(taskId + " could not acquire " + chunkFile.getPath());
    }

    return null;
  }

  /**
   * Method to be called to relinquish an acquired chunk. All streams open to
   * the chunk are closed, and the chunk-file is deleted.
   * @throws IOException Exception thrown on failure to release (i.e. delete)
   * the chunk file.
   */
  public void release() throws IOException {
    close();
    if (!fs.delete(chunkFilePath, false)) {
      LOG.error("Unable to release chunk at path: " + chunkFilePath);
      throw new IOException("Unable to release chunk at path: " + chunkFilePath);
    }
  }

  static FileStatus [] getListOfChunkFiles() throws IOException {
    Path chunkFilePattern = new Path(chunkRootPath, chunkFilePrefix + "*");
    FileStatus chunkFiles[] = fs.globStatus(chunkFilePattern);
    numChunksLeft = chunkFiles.length;
    return chunkFiles;
  }

  /**
   * Getter for the chunk-file's path, on HDFS.
   * @return The qualified path to the chunk-file.
   */
  public Path getPath() {
    return chunkFilePath;
  }

  /**
   * Getter for the record-reader, opened to the chunk-file.
   * @return Opened Sequence-file reader.
   */
  public SequenceFileRecordReader<K,V> getReader() {
    assert reader != null : "Reader un-initialized!";
    return reader;
  }

  /**
   * Getter for the number of chunk-files left in the chunk-file directory.
   * Useful to determine how many chunks (and hence, records) are left to be
   * processed.
   * @return Before the first scan of the directory, the number returned is -1.
   * Otherwise, the number of chunk-files seen from the last scan is returned.
   */
  public static int getNumChunksLeft() {
    return numChunksLeft;
  }
}
