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
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.tools.DistCpConstants;
import org.apache.hadoop.tools.util.DistCpUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;

import java.util.List;
import java.util.ArrayList;
import java.io.IOException;

/**
 * DynamicInputFormat implements the "Worker pattern" for DistCp.
 * Rather than to split up the copy-list into a set of static splits,
 * the DynamicInputFormat does the following:
 * 1. Splits the copy-list into small chunks on the DFS.
 * 2. Creates a set of empty "dynamic" splits, that each consume as many chunks
 *    as it can.
 * This arrangement ensures that a single slow mapper won't slow down the entire
 * job (since the slack will be picked up by other mappers, who consume more
 * chunks.)
 * By varying the split-ratio, one can vary chunk sizes to achieve different
 * performance characteristics. 
 */
public class DynamicInputFormat<K, V> extends InputFormat<K, V> {
  private static final Log LOG = LogFactory.getLog(DynamicInputFormat.class);

  private static final String CONF_LABEL_LISTING_SPLIT_RATIO
          = "mapred.listing.split.ratio";
  private static final String CONF_LABEL_NUM_SPLITS
          = "mapred.num.splits";
  private static final String CONF_LABEL_NUM_ENTRIES_PER_CHUNK
          = "mapred.num.entries.per.chunk";

  /**
   * Implementation of InputFormat::getSplits(). This method splits up the
   * copy-listing file into chunks, and assigns the first batch to different
   * tasks.
   * @param jobContext JobContext for the map job.
   * @return The list of (empty) dynamic input-splits.
   * @throws IOException, on failure.
   * @throws InterruptedException
   */
  @Override
  public List<InputSplit> getSplits(JobContext jobContext)
      throws IOException, InterruptedException {
    LOG.info("DynamicInputFormat: Getting splits for job:"
             + jobContext.getJobID());
    return createSplits(jobContext,
                        splitCopyListingIntoChunksWithShuffle(jobContext));
  }

  private List<InputSplit> createSplits(JobContext jobContext,
                                        List<DynamicInputChunk> chunks)
          throws IOException {
    int numMaps = getNumMapTasks(jobContext.getConfiguration());

    final int nSplits = Math.min(numMaps, chunks.size());
    List<InputSplit> splits = new ArrayList<InputSplit>(nSplits);
    
    for (int i=0; i< nSplits; ++i) {
      TaskID taskId = new TaskID(jobContext.getJobID(), TaskType.MAP, i);
      chunks.get(i).assignTo(taskId);
      splits.add(new FileSplit(chunks.get(i).getPath(), 0,
          // Setting non-zero length for FileSplit size, to avoid a possible
          // future when 0-sized file-splits are considered "empty" and skipped
          // over.
          MIN_RECORDS_PER_CHUNK,
          null));
    }
    DistCpUtils.publish(jobContext.getConfiguration(),
                        CONF_LABEL_NUM_SPLITS, splits.size());
    return splits;
  }

  private static int N_CHUNKS_OPEN_AT_ONCE_DEFAULT = 16;

  private List<DynamicInputChunk> splitCopyListingIntoChunksWithShuffle
                                    (JobContext context) throws IOException {

    final Configuration configuration = context.getConfiguration();
    int numRecords = getNumberOfRecords(configuration);
    int numMaps = getNumMapTasks(configuration);
    // Number of chunks each map will process, on average.
    int splitRatio = getListingSplitRatio(configuration, numMaps, numRecords);
    validateNumChunksUsing(splitRatio, numMaps);

    int numEntriesPerChunk = (int)Math.ceil((float)numRecords
                                          /(splitRatio * numMaps));
    DistCpUtils.publish(context.getConfiguration(),
                        CONF_LABEL_NUM_ENTRIES_PER_CHUNK,
                        numEntriesPerChunk);

    final int nChunksTotal = (int)Math.ceil((float)numRecords/numEntriesPerChunk);
    int nChunksOpenAtOnce
            = Math.min(N_CHUNKS_OPEN_AT_ONCE_DEFAULT, nChunksTotal);

    Path listingPath = getListingFilePath(configuration);
    SequenceFile.Reader reader
            = new SequenceFile.Reader(configuration,
                                      SequenceFile.Reader.file(listingPath));

    List<DynamicInputChunk> openChunks
                  = new ArrayList<DynamicInputChunk>();
    
    List<DynamicInputChunk> chunksFinal = new ArrayList<DynamicInputChunk>();

    FileStatus fileStatus = new FileStatus();
    Text relPath = new Text();
    int recordCounter = 0;
    int chunkCount = 0;

    try {

      while (reader.next(relPath, fileStatus)) {
        if (recordCounter % (nChunksOpenAtOnce*numEntriesPerChunk) == 0) {
          // All chunks full. Create new chunk-set.
          closeAll(openChunks);
          chunksFinal.addAll(openChunks);

          openChunks = createChunks(
                  configuration, chunkCount, nChunksTotal, nChunksOpenAtOnce);

          chunkCount += openChunks.size();

          nChunksOpenAtOnce = openChunks.size();
          recordCounter = 0;
        }

        // Shuffle into open chunks.
        openChunks.get(recordCounter%nChunksOpenAtOnce).write(relPath, fileStatus);
        ++recordCounter;
      }

    } finally {
      closeAll(openChunks);
      chunksFinal.addAll(openChunks);
      IOUtils.closeStream(reader);
    }

    LOG.info("Number of dynamic-chunk-files created: " + chunksFinal.size()); 
    return chunksFinal;
  }

  private static void validateNumChunksUsing(int splitRatio, int numMaps)
                                              throws IOException {
    if (splitRatio * numMaps > MAX_CHUNKS_TOLERABLE)
      throw new IOException("Too many chunks created with splitRatio:"
                 + splitRatio + ", numMaps:" + numMaps
                 + ". Reduce numMaps or decrease split-ratio to proceed.");
  }

  private static void closeAll(List<DynamicInputChunk> chunks) {
    for (DynamicInputChunk chunk: chunks)
      chunk.close();
  }

  private static List<DynamicInputChunk> createChunks(Configuration config,
                      int chunkCount, int nChunksTotal, int nChunksOpenAtOnce)
                                          throws IOException {
    List<DynamicInputChunk> chunks = new ArrayList<DynamicInputChunk>();
    int chunkIdUpperBound
            = Math.min(nChunksTotal, chunkCount + nChunksOpenAtOnce);

    // If there will be fewer than nChunksOpenAtOnce chunks left after
    // the current batch of chunks, fold the remaining chunks into
    // the current batch.
    if (nChunksTotal - chunkIdUpperBound < nChunksOpenAtOnce)
      chunkIdUpperBound = nChunksTotal;

    for (int i=chunkCount; i < chunkIdUpperBound; ++i)
      chunks.add(createChunk(i, config));
    return chunks;
  }

  private static DynamicInputChunk createChunk(int chunkId, Configuration config)
                                              throws IOException {
    return DynamicInputChunk.createChunkForWrite(String.format("%05d", chunkId),
                                              config);
  }


  private static Path getListingFilePath(Configuration configuration) {
    String listingFilePathString = configuration.get(
            DistCpConstants.CONF_LABEL_LISTING_FILE_PATH, "");

    assert !listingFilePathString.equals("") : "Listing file not found.";

    Path listingFilePath = new Path(listingFilePathString);
    try {
      assert listingFilePath.getFileSystem(configuration)
              .exists(listingFilePath) : "Listing file: " + listingFilePath +
                                          " not found.";
    } catch (IOException e) {
      assert false :   "Listing file: " + listingFilePath
                    + " couldn't be accessed. " + e.getMessage();
    }
    return listingFilePath;
  }

  private static int getNumberOfRecords(Configuration configuration) {
    return DistCpUtils.getInt(configuration,
                              DistCpConstants.CONF_LABEL_TOTAL_NUMBER_OF_RECORDS);
  }

  private static int getNumMapTasks(Configuration configuration) {
    return DistCpUtils.getInt(configuration,
                              JobContext.NUM_MAPS);
  }

  private static int getListingSplitRatio(Configuration configuration,
                                            int numMaps, int numPaths) {
    return configuration.getInt(
            CONF_LABEL_LISTING_SPLIT_RATIO,
            getSplitRatio(numMaps, numPaths));
  }

  private static final int MAX_CHUNKS_TOLERABLE = 400;
  private static final int MAX_CHUNKS_IDEAL     = 100;
  private static final int MIN_RECORDS_PER_CHUNK = 5;
  private static final int SPLIT_RATIO_DEFAULT  = 2;

  /**
   * Package private, for testability.
   * @param nMaps The number of maps requested for.
   * @param nRecords The number of records to be copied.
   * @return The number of splits each map should handle, ideally.
   */
  static int getSplitRatio(int nMaps, int nRecords) {
    if (nMaps == 1) {
      LOG.warn("nMaps == 1. Why use DynamicInputFormat?");
      return 1;
    }

    if (nMaps > MAX_CHUNKS_IDEAL)
      return SPLIT_RATIO_DEFAULT;

    int nPickups = (int)Math.ceil((float)MAX_CHUNKS_IDEAL/nMaps);
    int nRecordsPerChunk = (int)Math.ceil((float)nRecords/(nMaps*nPickups));

    return nRecordsPerChunk < MIN_RECORDS_PER_CHUNK ?
              SPLIT_RATIO_DEFAULT : nPickups;
  }

  static int getNumEntriesPerChunk(Configuration configuration) {
    return DistCpUtils.getInt(configuration,
                              CONF_LABEL_NUM_ENTRIES_PER_CHUNK);
  }


  /**
   * Implementation of Inputformat::createRecordReader().
   * @param inputSplit The split for which the RecordReader is required.
   * @param taskAttemptContext TaskAttemptContext for the current attempt.
   * @return DynamicRecordReader instance.
   * @throws IOException, on failure.
   * @throws InterruptedException
   */
  @Override
  public RecordReader<K, V> createRecordReader(
          InputSplit inputSplit,
          TaskAttemptContext taskAttemptContext)
          throws IOException, InterruptedException {
    return new DynamicRecordReader<K, V>();
  }
}
