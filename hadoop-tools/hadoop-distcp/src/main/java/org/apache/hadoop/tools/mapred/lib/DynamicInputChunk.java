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
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.tools.CopyListingFileStatus;
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
  private Path chunkFilePath;
  private SequenceFileRecordReader<K, V> reader;
  private SequenceFile.Writer writer;
  private DynamicInputChunkContext chunkContext;

  DynamicInputChunk(String chunkId, DynamicInputChunkContext chunkContext)
                                                      throws IOException {
    this.chunkContext = chunkContext;
    chunkFilePath = new Path(chunkContext.getChunkRootPath(),
        chunkContext.getChunkFilePrefix() + chunkId);
    openForWrite();
  }

  private void openForWrite() throws IOException {
    writer = SequenceFile.createWriter(
            chunkContext.getFs(), chunkContext.getConfiguration(),
            chunkFilePath, Text.class, CopyListingFileStatus.class,
            SequenceFile.CompressionType.NONE);

  }

  /**
   * Method to write records into a chunk.
   * @param key Key from the listing file.
   * @param value Corresponding value from the listing file.
   * @throws IOException Exception on failure to write to the file.
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
    Path newPath = new Path(chunkContext.getChunkRootPath(), taskId.toString());
    if (!chunkContext.getFs().rename(chunkFilePath, newPath)) {
      LOG.warn(chunkFilePath + " could not be assigned to " + taskId);
    }
  }

  public DynamicInputChunk(Path chunkFilePath,
      TaskAttemptContext taskAttemptContext,
      DynamicInputChunkContext chunkContext) throws IOException,
      InterruptedException {

    this.chunkFilePath = chunkFilePath;
    this.chunkContext = chunkContext;
    openForRead(taskAttemptContext);
  }

  private void openForRead(TaskAttemptContext taskAttemptContext)
          throws IOException, InterruptedException {
    reader = new SequenceFileRecordReader<K, V>();
    reader.initialize(new FileSplit(chunkFilePath, 0,
            DistCpUtils.getFileSize(chunkFilePath,
                chunkContext.getConfiguration()), null), taskAttemptContext);
  }

  /**
   * Method to be called to relinquish an acquired chunk. All streams open to
   * the chunk are closed, and the chunk-file is deleted.
   * @throws IOException Exception thrown on failure to release (i.e. delete)
   * the chunk file.
   */
  public void release() throws IOException {
    close();
    if (!chunkContext.getFs().delete(chunkFilePath, false)) {
      LOG.error("Unable to release chunk at path: " + chunkFilePath);
      throw new IOException("Unable to release chunk at path: " +
          chunkFilePath);
    }
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

}
