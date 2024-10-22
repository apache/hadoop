/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.tools.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;
import org.apache.hadoop.tools.CopyListingFileStatus;
import org.apache.hadoop.tools.DistCpConstants;
import org.apache.hadoop.tools.util.DistCpUtils;
import org.apache.hadoop.util.Preconditions;

/**
 * UniformRecordInputFormat extends the InputFormat class, to produce
 * record-splits for DistCp.
 * It looks at the copy-listing and groups the contents into record-splits such
 * that the total-records to be copied for each input split is uniform.
 */
public class UniformRecordInputFormat extends InputFormat<Text, CopyListingFileStatus> {
  private static final Logger LOG = LoggerFactory.getLogger(UniformRecordInputFormat.class);

  /**
   * Implementation of InputFormat::getSplits(). Returns a list of InputSplits,
   * such that the number of records to be copied for all the splits are
   * approximately equal.
   * @param context JobContext for the job.
   * @return The list of uniformly-distributed input-splits.
   * @throws IOException Exception Reading split file
   * @throws InterruptedException Thread interrupted exception
   */
  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    int numSplits = getNumSplits(conf);
    if (numSplits == 0){
      return new ArrayList<InputSplit>();
    }

    return createSplits(conf, numSplits, getNumberOfRecords(conf));
  }

  private List<InputSplit> createSplits(Configuration configuration, int numSplits, long numRecords)
          throws IOException {
    List<InputSplit> splits = new ArrayList(numSplits);
    long nRecordsPerSplit = (long) Math.floor(numRecords * 1.0 / numSplits);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Average records per map: " + nRecordsPerSplit +
              ", Number of maps: " + numSplits + ", total records: " + numRecords);
    }

    Path listingFilePath = getListingFilePath(configuration);
    CopyListingFileStatus srcFileStatus = new CopyListingFileStatus();
    Text srcRelPath = new Text();
    long lastPosition = 0L;
    long count = 0L;
    long remains = numRecords - nRecordsPerSplit * (long) numSplits;

    SequenceFile.Reader reader = null;
    try {
      reader = getListingFileReader(configuration);
      while (reader.next(srcRelPath, srcFileStatus)) {
        count++;

        // a split's size must be nRecordsPerSplit or (nRecordsPerSplit + 1)
        // the first N (num of remains) splits have a size of (nRecordsPerSplit + 1),
        // the others have a size of nRecordsPerSplit
        if ((remains > 0 && count % (nRecordsPerSplit + 1) == 0) ||
                (remains == 0 && count % nRecordsPerSplit == 0)) {

          long currentPosition = reader.getPosition();
          FileSplit split = new FileSplit(listingFilePath, lastPosition,
                  currentPosition - lastPosition, null);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Creating split: " + split + ", records in split: " + count);
          }

          splits.add(split);
          lastPosition = currentPosition;
          if (remains > 0) {
            remains--;
          }
          count = 0L;
        }
      }

      return splits;
    } finally {
      IOUtils.closeStream(reader);
    }
  }

  /**
   * Implementation of InputFormat::createRecordReader().
   * @param split The split for which the RecordReader is sought.
   * @param context The context of the current task-attempt.
   * @return A SequenceFileRecordReader instance, (since the copy-listing is a
   * simple sequence-file.)
   * @throws IOException Exception Reading split file
   * @throws InterruptedException Thread interrupted exception
   */
  public RecordReader<Text, CopyListingFileStatus> createRecordReader(
          InputSplit split, TaskAttemptContext context)
          throws IOException, InterruptedException {
    return new SequenceFileRecordReader<Text, CopyListingFileStatus>();
  }

  private static Path getListingFilePath(Configuration configuration) {
    String listingFilePathString =
            configuration.get(DistCpConstants.CONF_LABEL_LISTING_FILE_PATH, "");

    Preconditions.checkState(!listingFilePathString.equals(""),
            "Listing file doesn't exist at %s", listingFilePathString);
    return new Path(listingFilePathString);
  }

  private SequenceFile.Reader getListingFileReader(Configuration conf) {
    Path listingFilePath = getListingFilePath(conf);

    try {
      FileSystem fs = listingFilePath.getFileSystem(conf);
      Preconditions.checkState(fs.exists(listingFilePath),
              "Listing file doesn't exist at: %s", listingFilePath);

      return new SequenceFile.Reader(conf,
              SequenceFile.Reader.file(listingFilePath));
    } catch (IOException | IllegalStateException exception) {
      LOG.error("Couldn't read listing file at: {}", listingFilePath, exception);
      throw new IllegalStateException("Couldn't read listing file at: "
              + listingFilePath, exception);
    }
  }

  private static long getNumberOfRecords(Configuration configuration) {
    return DistCpUtils.getLong(configuration,
            DistCpConstants.CONF_LABEL_TOTAL_NUMBER_OF_RECORDS);
  }

  private static int getNumSplits(Configuration configuration) {
    return DistCpUtils.getInt(configuration, MRJobConfig.NUM_MAPS);
  }
}
