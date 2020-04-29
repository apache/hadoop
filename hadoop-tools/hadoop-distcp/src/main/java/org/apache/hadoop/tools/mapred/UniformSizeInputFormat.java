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

package org.apache.hadoop.tools.mapred;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.tools.CopyListingFileStatus;
import org.apache.hadoop.tools.DistCpConstants;
import org.apache.hadoop.tools.util.DistCpUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

/**
 * UniformSizeInputFormat extends the InputFormat class, to produce
 * input-splits for DistCp.
 * It looks at the copy-listing and groups the contents into input-splits such
 * that the total-number of bytes to be copied for each input split is
 * uniform.
 */
public class UniformSizeInputFormat
    extends InputFormat<Text, CopyListingFileStatus> {
  private static final Logger LOG
                = LoggerFactory.getLogger(UniformSizeInputFormat.class);

  /**
   * Implementation of InputFormat::getSplits(). Returns a list of InputSplits,
   * such that the number of bytes to be copied for all the splits are
   * approximately equal.
   * @param context JobContext for the job.
   * @return The list of uniformly-distributed input-splits.
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  public List<InputSplit> getSplits(JobContext context)
                      throws IOException, InterruptedException {
    Configuration configuration = context.getConfiguration();
    int numSplits = DistCpUtils.getInt(configuration,
                                       JobContext.NUM_MAPS);

    if (numSplits == 0) return new ArrayList<InputSplit>();

    return getSplits(configuration, numSplits,
                     DistCpUtils.getLong(configuration,
                          DistCpConstants.CONF_LABEL_TOTAL_BYTES_TO_BE_COPIED));
  }

  private List<InputSplit> getSplits(Configuration configuration, int numSplits,
                                     long totalSizeBytes) throws IOException {
    List<InputSplit> splits = new ArrayList<InputSplit>(numSplits);
    long nBytesPerSplit = (long) Math.ceil(totalSizeBytes * 1.0 / numSplits);

    CopyListingFileStatus srcFileStatus = new CopyListingFileStatus();
    Text srcRelPath = new Text();
    long currentSplitSize = 0;
    long lastSplitStart = 0;
    long lastPosition = 0;

    final Path listingFilePath = getListingFilePath(configuration);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Average bytes per map: " + nBytesPerSplit +
          ", Number of maps: " + numSplits + ", total size: " + totalSizeBytes);
    }
    SequenceFile.Reader reader=null;
    try {
      reader = getListingFileReader(configuration);
      while (reader.next(srcRelPath, srcFileStatus)) {
        // If adding the current file would cause the bytes per map to exceed
        // limit. Add the current file to new split
        if (currentSplitSize + srcFileStatus.getChunkLength() > nBytesPerSplit
            && lastPosition != 0) {
          FileSplit split = new FileSplit(listingFilePath, lastSplitStart,
              lastPosition - lastSplitStart, null);
          if (LOG.isDebugEnabled()) {
            LOG.debug ("Creating split : " + split + ", bytes in split: " + currentSplitSize);
          }
          splits.add(split);
          lastSplitStart = lastPosition;
          currentSplitSize = 0;
        }
        currentSplitSize += srcFileStatus.getChunkLength();
        lastPosition = reader.getPosition();
      }
      if (lastPosition > lastSplitStart) {
        FileSplit split = new FileSplit(listingFilePath, lastSplitStart,
            lastPosition - lastSplitStart, null);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Creating split : " + split + ", bytes in split: "
              + currentSplitSize);
        }
        splits.add(split);
      }

    } finally {
      IOUtils.closeStream(reader);
    }

    return splits;
  }

  private static Path getListingFilePath(Configuration configuration) {
    final String listingFilePathString =
            configuration.get(DistCpConstants.CONF_LABEL_LISTING_FILE_PATH, "");

    assert !listingFilePathString.equals("")
              : "Couldn't find listing file. Invalid input.";
    return new Path(listingFilePathString);
  }

  private SequenceFile.Reader getListingFileReader(Configuration configuration) {

    final Path listingFilePath = getListingFilePath(configuration);
    try {
      final FileSystem fileSystem = listingFilePath.getFileSystem(configuration);
      if (!fileSystem.exists(listingFilePath))
        throw new IllegalArgumentException("Listing file doesn't exist at: "
                                           + listingFilePath);

      return new SequenceFile.Reader(configuration,
                                     SequenceFile.Reader.file(listingFilePath));
    }
    catch (IOException exception) {
      LOG.error("Couldn't find listing file at: " + listingFilePath, exception);
      throw new IllegalArgumentException("Couldn't find listing-file at: "
                                         + listingFilePath, exception);
    }
  }

  /**
   * Implementation of InputFormat::createRecordReader().
   * @param split The split for which the RecordReader is sought.
   * @param context The context of the current task-attempt.
   * @return A SequenceFileRecordReader instance, (since the copy-listing is a
   * simple sequence-file.)
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  public RecordReader<Text, CopyListingFileStatus> createRecordReader(
      InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    return new SequenceFileRecordReader<Text, CopyListingFileStatus>();
  }
}
