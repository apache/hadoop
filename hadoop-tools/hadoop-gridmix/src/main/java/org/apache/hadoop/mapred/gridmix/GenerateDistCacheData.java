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
package org.apache.hadoop.mapred.gridmix;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.server.tasktracker.TTConfig;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * GridmixJob that generates distributed cache files.
 * {@link GenerateDistCacheData} expects a list of distributed cache files to be
 * generated as input. This list is expected to be stored as a sequence file
 * and the filename is expected to be configured using
 * {@code gridmix.distcache.file.list}.
 * This input file contains the list of distributed cache files and their sizes.
 * For each record (i.e. file size and file path) in this input file,
 * a file with the specific file size at the specific path is created.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
class GenerateDistCacheData extends GridmixJob {

  /**
   * Number of distributed cache files to be created by gridmix
   */
  static final String GRIDMIX_DISTCACHE_FILE_COUNT =
      "gridmix.distcache.file.count";
  /**
   * Total number of bytes to be written to the distributed cache files by
   * gridmix. i.e. Sum of sizes of all unique distributed cache files to be
   * created by gridmix.
   */
  static final String GRIDMIX_DISTCACHE_BYTE_COUNT =
      "gridmix.distcache.byte.count";
  /**
   * The special file created(and used) by gridmix, that contains the list of
   * unique distributed cache files that are to be created and their sizes.
   */
  static final String GRIDMIX_DISTCACHE_FILE_LIST =
      "gridmix.distcache.file.list";
  static final String JOB_NAME = "GRIDMIX_GENERATE_DISTCACHE_DATA";

  /**
   * Create distributed cache file with the permissions 0644.
   * Since the private distributed cache directory doesn't have execute
   * permission for others, it is OK to set read permission for others for
   * the files under that directory and still they will become 'private'
   * distributed cache files on the simulated cluster.
   */
  static final short GRIDMIX_DISTCACHE_FILE_PERM = 0644;

  public GenerateDistCacheData(Configuration conf) throws IOException {
    super(conf, 0L, JOB_NAME);
  }

  @Override
  public Job call() throws IOException, InterruptedException,
                           ClassNotFoundException {
    UserGroupInformation ugi = UserGroupInformation.getLoginUser();
    ugi.doAs( new PrivilegedExceptionAction <Job>() {
       public Job run() throws IOException, ClassNotFoundException,
                               InterruptedException {
        job.setMapperClass(GenDCDataMapper.class);
        job.setNumReduceTasks(0);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(BytesWritable.class);
        job.setInputFormatClass(GenDCDataFormat.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setJarByClass(GenerateDistCacheData.class);
        try {
          FileInputFormat.addInputPath(job, new Path("ignored"));
        } catch (IOException e) {
          LOG.error("Error while adding input path ", e);
        }
        job.submit();
        return job;
      }
    });
    return job;
  }

  @Override
  protected boolean canEmulateCompression() {
    return false;
  }

  public static class GenDCDataMapper
      extends Mapper<LongWritable, BytesWritable, NullWritable, BytesWritable> {

    private BytesWritable val;
    private final Random r = new Random();
    private FileSystem fs;

    @Override
    protected void setup(Context context)
        throws IOException, InterruptedException {
      val = new BytesWritable(new byte[context.getConfiguration().getInt(
              GenerateData.GRIDMIX_VAL_BYTES, 1024 * 1024)]);
      fs = FileSystem.get(context.getConfiguration());
    }

    // Create one distributed cache file with the needed file size.
    // key is distributed cache file size and
    // value is distributed cache file path.
    @Override
    public void map(LongWritable key, BytesWritable value, Context context)
        throws IOException, InterruptedException {

      String fileName = new String(value.getBytes(), 0, value.getLength());
      Path path = new Path(fileName);

      FSDataOutputStream dos =
          FileSystem.create(fs, path, new FsPermission(GRIDMIX_DISTCACHE_FILE_PERM));

      int size = 0;
      for (long bytes = key.get(); bytes > 0; bytes -= size) {
        r.nextBytes(val.getBytes());
        size = (int)Math.min(val.getLength(), bytes);
        dos.write(val.getBytes(), 0, size);// Write to distCache file
      }
      dos.close();
    }
  }

  /**
   * InputFormat for GenerateDistCacheData.
   * Input to GenerateDistCacheData is the special file(in SequenceFile format)
   * that contains the list of distributed cache files to be generated along
   * with their file sizes.
   */
  static class GenDCDataFormat
      extends InputFormat<LongWritable, BytesWritable> {

    // Split the special file that contains the list of distributed cache file
    // paths and their file sizes such that each split corresponds to
    // approximately same amount of distributed cache data to be generated.
    // Consider numTaskTrackers * numMapSlotsPerTracker as the number of maps
    // for this job, if there is lot of data to be generated.
    @Override
    public List<InputSplit> getSplits(JobContext jobCtxt) throws IOException {
      final JobConf jobConf = new JobConf(jobCtxt.getConfiguration());
      final JobClient client = new JobClient(jobConf);
      ClusterStatus stat = client.getClusterStatus(true);
      int numTrackers = stat.getTaskTrackers();
      final int fileCount = jobConf.getInt(GRIDMIX_DISTCACHE_FILE_COUNT, -1);

      // Total size of distributed cache files to be generated
      final long totalSize = jobConf.getLong(GRIDMIX_DISTCACHE_BYTE_COUNT, -1);
      // Get the path of the special file
      String distCacheFileList = jobConf.get(GRIDMIX_DISTCACHE_FILE_LIST);
      if (fileCount < 0 || totalSize < 0 || distCacheFileList == null) {
        throw new RuntimeException("Invalid metadata: #files (" + fileCount
            + "), total_size (" + totalSize + "), filelisturi ("
            + distCacheFileList + ")");
      }

      Path sequenceFile = new Path(distCacheFileList);
      FileSystem fs = sequenceFile.getFileSystem(jobConf);
      FileStatus srcst = fs.getFileStatus(sequenceFile);
      // Consider the number of TTs * mapSlotsPerTracker as number of mappers.
      int numMapSlotsPerTracker = jobConf.getInt(TTConfig.TT_MAP_SLOTS, 2);
      int numSplits = numTrackers * numMapSlotsPerTracker;

      List<InputSplit> splits = new ArrayList<InputSplit>(numSplits);
      LongWritable key = new LongWritable();
      BytesWritable value = new BytesWritable();

      // Average size of data to be generated by each map task
      final long targetSize = Math.max(totalSize / numSplits,
                                DistributedCacheEmulator.AVG_BYTES_PER_MAP);
      long splitStartPosition = 0L;
      long splitEndPosition = 0L;
      long acc = 0L;
      long bytesRemaining = srcst.getLen();
      SequenceFile.Reader reader = null;
      try {
        reader = new SequenceFile.Reader(fs, sequenceFile, jobConf);
        while (reader.next(key, value)) {

          // If adding this file would put this split past the target size,
          // cut the last split and put this file in the next split.
          if (acc + key.get() > targetSize && acc != 0) {
            long splitSize = splitEndPosition - splitStartPosition;
            splits.add(new FileSplit(
                sequenceFile, splitStartPosition, splitSize, (String[])null));
            bytesRemaining -= splitSize;
            splitStartPosition = splitEndPosition;
            acc = 0L;
          }
          acc += key.get();
          splitEndPosition = reader.getPosition();
        }
      } finally {
        if (reader != null) {
          reader.close();
        }
      }
      if (bytesRemaining != 0) {
        splits.add(new FileSplit(
            sequenceFile, splitStartPosition, bytesRemaining, (String[])null));
      }

      return splits;
    }

    /**
     * Returns a reader for this split of the distributed cache file list.
     */
    @Override
    public RecordReader<LongWritable, BytesWritable> createRecordReader(
        InputSplit split, final TaskAttemptContext taskContext)
        throws IOException, InterruptedException {
      return new SequenceFileRecordReader<LongWritable, BytesWritable>();
    }
  }
}
