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

package org.apache.hadoop.raid;

import java.io.IOException;
import java.io.PrintStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Date;
import java.text.SimpleDateFormat;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.RaidDFSUtil;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import org.apache.hadoop.util.StringUtils;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.InputSplit;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/**
 * distributed block fixer, uses map reduce jobs to fix corrupt files
 *
 * configuration options
 * raid.blockfix.filespertask       - number of corrupt files to fix in a single
 *                                    map reduce task (i.e., at one mapper node)
 *
 * raid.blockfix.fairscheduler.pool - the pool to use for block fixer jobs
 *
 * raid.blockfix.maxpendingfiles    - maximum number of files to fix 
 *                                    simultaneously
 */
public class DistBlockFixer extends BlockFixer {
  // volatile should be sufficient since only the block fixer thread
  // updates numJobsRunning (other threads may read)
  private volatile int numJobsRunning = 0;

  private static final String WORK_DIR_PREFIX = "blockfixer";
  private static final String IN_FILE_SUFFIX = ".in";
  private static final String PART_PREFIX = "part-";
  
  private static final String BLOCKFIX_FILES_PER_TASK = 
    "raid.blockfix.filespertask";
  private static final String BLOCKFIX_MAX_PENDING_FILES =
    "raid.blockfix.maxpendingfiles";
  private static final String BLOCKFIX_POOL = 
    "raid.blockfix.fairscheduler.pool";
  // mapred.fairscheduler.pool is only used in the local configuration
  // passed to a block fixing job
  private static final String MAPRED_POOL = 
    "mapred.fairscheduler.pool";

  // default number of files to fix in a task
  private static final long DEFAULT_BLOCKFIX_FILES_PER_TASK = 10L;

  // default number of files to fix simultaneously
  private static final long DEFAULT_BLOCKFIX_MAX_PENDING_FILES = 1000L;
 
  protected static final Log LOG = LogFactory.getLog(DistBlockFixer.class);

  // number of files to fix in a task
  private long filesPerTask;

  // number of files to fix simultaneously
  final private long maxPendingFiles;

  // number of files being fixed right now
  private long pendingFiles;

  // pool name to use (may be null, in which case no special pool is used)
  private String poolName;

  private long lastCheckTime;

  private final SimpleDateFormat dateFormat = 
    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  private Map<String, CorruptFileInfo> fileIndex = 
    new HashMap<String, CorruptFileInfo>();
  private Map<Job, List<CorruptFileInfo>> jobIndex =
    new HashMap<Job, List<CorruptFileInfo>>();

  static enum Counter {
    FILES_SUCCEEDED, FILES_FAILED, FILES_NOACTION
  }

  public DistBlockFixer(Configuration conf) {
    super(conf);
    filesPerTask = DistBlockFixer.filesPerTask(getConf());
    maxPendingFiles = DistBlockFixer.maxPendingFiles(getConf());
    pendingFiles = 0L;
    poolName = conf.get(BLOCKFIX_POOL);

    // start off due for the first iteration
    lastCheckTime = System.currentTimeMillis() - blockFixInterval;
  }

  /**
   * determines how many files to fix in a single task
   */ 
  protected static long filesPerTask(Configuration conf) {
    return conf.getLong(BLOCKFIX_FILES_PER_TASK, 
                        DEFAULT_BLOCKFIX_FILES_PER_TASK);

  }
  /**
   * determines how many files to fix simultaneously
   */ 
  protected static long maxPendingFiles(Configuration conf) {
    return conf.getLong(BLOCKFIX_MAX_PENDING_FILES, 
                        DEFAULT_BLOCKFIX_MAX_PENDING_FILES);
  }

  /**
   * runs the block fixer periodically
   */
  public void run() {
    while (running) {
      // check if it is time to run the block fixer
      long now = System.currentTimeMillis();
      if (now >= lastCheckTime + blockFixInterval) {
        lastCheckTime = now;
        try {
          checkAndFixBlocks(now);
        } catch (InterruptedException ignore) {
          LOG.info("interrupted");
        } catch (Exception e) {
          // log exceptions and keep running
          LOG.error(StringUtils.stringifyException(e));
        } catch (Error e) {
          LOG.error(StringUtils.stringifyException(e));
          throw e;
        }
      }
      
      // try to sleep for the remainder of the interval
      long sleepPeriod = (lastCheckTime - System.currentTimeMillis()) + 
        blockFixInterval;
      
      if ((sleepPeriod > 0L) && running) {
        try {
          Thread.sleep(sleepPeriod);
        } catch (InterruptedException ignore) {
          LOG.info("interrupted");
        }
      }
    }
  }

  /**
   * checks for corrupt blocks and fixes them (if any)
   */
  private void checkAndFixBlocks(long startTime)
    throws IOException, InterruptedException, ClassNotFoundException {
    checkJobs();

    if (pendingFiles >= maxPendingFiles) {
      return;
    }

    List<Path> corruptFiles = getCorruptFiles();
    filterUnfixableSourceFiles(corruptFiles.iterator());

    String startTimeStr = dateFormat.format(new Date(startTime));

    LOG.info("found " + corruptFiles.size() + " corrupt files");

    if (corruptFiles.size() > 0) {
      String jobName = "blockfixer." + startTime;
      startJob(jobName, corruptFiles);
    }
  }

  /**
   * Handle a failed job.
   */
  private void failJob(Job job) throws IOException {
    // assume no files have been fixed
    LOG.error("DistBlockFixer job " + job.getJobID() + "(" + job.getJobName() +
      ") finished (failed)");
    for (CorruptFileInfo fileInfo: jobIndex.get(job)) {
      fileInfo.fail();
    }
    numJobsRunning--;
  }

  /**
   * Handle a successful job.
   */ 
  private void succeedJob(Job job, long filesSucceeded, long filesFailed)
    throws IOException {
    LOG.info("DistBlockFixer job " + job.getJobID() + "(" + job.getJobName() +
      ") finished (succeeded)");

    if (filesFailed == 0) {
      // no files have failed
      for (CorruptFileInfo fileInfo: jobIndex.get(job)) {
        fileInfo.succeed();
      }
    } else {
      // we have to look at the output to check which files have failed
      Set<String> failedFiles = getFailedFiles(job);
      
      for (CorruptFileInfo fileInfo: jobIndex.get(job)) {
        if (failedFiles.contains(fileInfo.getFile().toString())) {
          fileInfo.fail();
        } else {
          // call succeed for files that have succeeded or for which no action
          // was taken
          fileInfo.succeed();
        }
      }
    }
    // report succeeded files to metrics
    incrFilesFixed(filesSucceeded);
    numJobsRunning--;
  }

  /**
   * checks if jobs have completed and updates job and file index
   * returns a list of failed files for restarting
   */
  private void checkJobs() throws IOException {
    Iterator<Job> jobIter = jobIndex.keySet().iterator();
    while(jobIter.hasNext()) {
      Job job = jobIter.next();

      try {
        if (job.isComplete()) {
          long filesSucceeded =
            job.getCounters().findCounter(Counter.FILES_SUCCEEDED).getValue();
          long filesFailed =
            job.getCounters().findCounter(Counter.FILES_FAILED).getValue();
          long filesNoAction =
            job.getCounters().findCounter(Counter.FILES_NOACTION).getValue();
          int files = jobIndex.get(job).size();
          if (job.isSuccessful() && 
              (filesSucceeded + filesFailed + filesNoAction == 
               ((long) files))) {
            // job has processed all files
            succeedJob(job, filesSucceeded, filesFailed);
          } else {
            failJob(job);
          }
          jobIter.remove();
        } else {
          LOG.info("job " + job.getJobName() + " still running");
        }
      } catch (Exception e) {
        LOG.error(StringUtils.stringifyException(e));
        failJob(job);
        try {
          job.killJob();
        } catch (Exception ee) {
          LOG.error(StringUtils.stringifyException(ee));
        }
        jobIter.remove();
      }
    }
    purgeFileIndex();
  }

  /**
   * determines which files have failed for a given job
   */
  private Set<String> getFailedFiles(Job job) throws IOException {
    Set<String> failedFiles = new HashSet<String>();

    Path outDir = SequenceFileOutputFormat.getOutputPath(job);
    FileSystem fs  = outDir.getFileSystem(getConf());
    if (!fs.getFileStatus(outDir).isDir()) {
      throw new IOException(outDir.toString() + " is not a directory");
    }

    FileStatus[] files = fs.listStatus(outDir);

    for (FileStatus f: files) {
      Path fPath = f.getPath();
      if ((!f.isDir()) && (fPath.getName().startsWith(PART_PREFIX))) {
        LOG.info("opening " + fPath.toString());
        SequenceFile.Reader reader = 
          new SequenceFile.Reader(fs, fPath, getConf());

        Text key = new Text();
        Text value = new Text();
        while (reader.next(key, value)) {
          failedFiles.add(key.toString());
        }
        reader.close();
      }
    }
    return failedFiles;
  }


  /**
   * purge expired jobs from the file index
   */
  private void purgeFileIndex() {
    Iterator<String> fileIter = fileIndex.keySet().iterator();
    while(fileIter.hasNext()) {
      String file = fileIter.next();
      if (fileIndex.get(file).isExpired()) {
        fileIter.remove();
      }
    }
    
  }

  /**
   * creates and submits a job, updates file index and job index
   */
  private Job startJob(String jobName, List<Path> corruptFiles) 
    throws IOException, InterruptedException, ClassNotFoundException {
    Path inDir = new Path(WORK_DIR_PREFIX + "/in/" + jobName);
    Path outDir = new Path(WORK_DIR_PREFIX + "/out/" + jobName);
    List<Path> filesInJob = createInputFile(jobName, inDir, corruptFiles);

    Configuration jobConf = new Configuration(getConf());
    if (poolName != null) {
      jobConf.set(MAPRED_POOL, poolName);
    }
    Job job = new Job(jobConf, jobName);
    job.setJarByClass(getClass());
    job.setMapperClass(DistBlockFixerMapper.class);
    job.setNumReduceTasks(0);
    job.setInputFormatClass(DistBlockFixerInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    DistBlockFixerInputFormat.setInputPaths(job, inDir);
    SequenceFileOutputFormat.setOutputPath(job, outDir);

    job.submit();
    LOG.info("DistBlockFixer job " + job.getJobID() + "(" + job.getJobName() +
          ") started");

    // submit the job before inserting it into the index
    // this way, if submit fails, we won't have added anything to the index
    insertJob(job, filesInJob);
    return job;
  }

  /**
   * inserts new job into file index and job index
   */
  private void insertJob(Job job, List<Path> corruptFiles) {
    List<CorruptFileInfo> fileInfos = new LinkedList<CorruptFileInfo>();

    for (Path file: corruptFiles) {
      CorruptFileInfo fileInfo = new CorruptFileInfo(file, job);
      fileInfos.add(fileInfo);
      fileIndex.put(file.toString(), fileInfo);
    }

    jobIndex.put(job, fileInfos);
    numJobsRunning++;
  }
    
  /**
   * creates the input file (containing the names of the files to be fixed
   */
  private List<Path> createInputFile(String jobName, Path inDir, 
                                     List<Path> corruptFiles) 
    throws IOException {

    Path file = new Path(inDir, jobName + IN_FILE_SUFFIX);
    FileSystem fs = file.getFileSystem(getConf());
    SequenceFile.Writer fileOut = SequenceFile.createWriter(fs, getConf(), file,
                                                            LongWritable.class,
                                                            Text.class);
    long index = 0L;

    List<Path> filesAdded = new LinkedList<Path>();

    for (Path corruptFile: corruptFiles) {
      if (pendingFiles >= maxPendingFiles) {
        break;
      }

      String corruptFileName = corruptFile.toString();
      fileOut.append(new LongWritable(index++), new Text(corruptFileName));
      filesAdded.add(corruptFile);
      pendingFiles++;

      if (index % filesPerTask == 0) {
        fileOut.sync(); // create sync point to make sure we can split here
      }
    }

    fileOut.close();
    return filesAdded;
  }

  /**
   * gets a list of corrupt files from the name node
   * and filters out files that are currently being fixed or 
   * that were recently fixed
   */
  private List<Path> getCorruptFiles() throws IOException {
    DistributedFileSystem dfs = (DistributedFileSystem) 
      (new Path("/")).getFileSystem(getConf());

    String[] files = RaidDFSUtil.getCorruptFiles(dfs);
    List<Path> corruptFiles = new LinkedList<Path>();

    for (String f: files) {
      Path p = new Path(f);
      // filter out files that are being fixed or that were recently fixed
      if (!fileIndex.containsKey(p.toString())) {
        corruptFiles.add(p);
      }
    }
    RaidUtils.filterTrash(getConf(), corruptFiles);

    return corruptFiles;
  }

  /**
   * returns the number of map reduce jobs running
   */
  public int jobsRunning() {
    return numJobsRunning;
  }

  /**
   * hold information about a corrupt file that is being fixed
   */
  class CorruptFileInfo {

    private Path file;
    private Job job;
    private boolean done;
    private long time;

    public CorruptFileInfo(Path file, Job job) {
      this.file = file;
      this.job = job;
      this.done = false;
      this.time = 0;
    }

    public boolean isDone() {
      return done;
    }
    
    public boolean isExpired() {
      return done && ((System.currentTimeMillis() - time) > historyInterval);
    }

    public Path getFile() {
      return file;
    }
    
    /**
     * updates file index to record a failed attempt at fixing a file,
     * immediately removes the entry from the file index 
     * (instead of letting it expire)
     * so that we can retry right away
     */
    public void fail() {
      // remove this file from the index
      CorruptFileInfo removed = fileIndex.remove(file.toString());
      if (removed == null) {
        LOG.error("trying to remove file not in file index: " +
                  file.toString());
      } else {
        LOG.error("fixing " + file.toString() + " failed");
      }
      pendingFiles--;
    }

    /**
     * marks a file as fixed successfully
     * and sets time stamp for expiry after specified interval
     */
    public void succeed() {
      // leave the file in the index,
      // will be pruged later
      job = null;
      done = true;
      time = System.currentTimeMillis();
      LOG.info("fixing " + file.toString() + " succeeded");
      pendingFiles--;
    }
  }

  static class DistBlockFixerInputFormat
    extends SequenceFileInputFormat<LongWritable, Text> {

    protected static final Log LOG = 
      LogFactory.getLog(DistBlockFixerMapper.class);
    
    /**
     * splits the input files into tasks handled by a single node
     * we have to read the input files to do this based on a number of 
     * items in a sequence
     */
    @Override
    public List <InputSplit> getSplits(JobContext job) 
      throws IOException {
      long filesPerTask = DistBlockFixer.filesPerTask(job.getConfiguration());

      Path[] inPaths = getInputPaths(job);

      List<InputSplit> splits = new LinkedList<InputSplit>();

      long fileCounter = 0;

      for (Path inPath: inPaths) {
        
        FileSystem fs = inPath.getFileSystem(job.getConfiguration());      

        if (!fs.getFileStatus(inPath).isDir()) {
          throw new IOException(inPath.toString() + " is not a directory");
        }

        FileStatus[] inFiles = fs.listStatus(inPath);

        for (FileStatus inFileStatus: inFiles) {
          Path inFile = inFileStatus.getPath();
          
          if (!inFileStatus.isDir() &&
              (inFile.getName().equals(job.getJobName() + IN_FILE_SUFFIX))) {

            fileCounter++;
            SequenceFile.Reader inFileReader = 
              new SequenceFile.Reader(fs, inFile, job.getConfiguration());
            
            long startPos = inFileReader.getPosition();
            long counter = 0;
            
            // create an input split every filesPerTask items in the sequence
            LongWritable key = new LongWritable();
            Text value = new Text();
            try {
              while (inFileReader.next(key, value)) {
                if (counter % filesPerTask == filesPerTask - 1L) {
                  splits.add(new FileSplit(inFile, startPos, 
                                           inFileReader.getPosition() - 
                                           startPos,
                                           null));
                  startPos = inFileReader.getPosition();
                }
                counter++;
              }
              
              // create input split for remaining items if necessary
              // this includes the case where no splits were created by the loop
              if (startPos != inFileReader.getPosition()) {
                splits.add(new FileSplit(inFile, startPos,
                                         inFileReader.getPosition() - startPos,
                                         null));
              }
            } finally {
              inFileReader.close();
            }
          }
        }
      }

      LOG.info("created " + splits.size() + " input splits from " +
               fileCounter + " files");
      
      return splits;
    }

    /**
     * indicates that input file can be split
     */
    @Override
    public boolean isSplitable (JobContext job, Path file) {
      return true;
    }
  }


  /**
   * mapper for fixing stripes with corrupt blocks
   */
  static class DistBlockFixerMapper
    extends Mapper<LongWritable, Text, Text, Text> {

    protected static final Log LOG = 
      LogFactory.getLog(DistBlockFixerMapper.class);

    /**
     * fix a stripe
     */
    @Override
    public void map(LongWritable key, Text fileText, Context context) 
      throws IOException, InterruptedException {
      
      BlockFixerHelper helper = 
        new BlockFixerHelper(context.getConfiguration());

      String fileStr = fileText.toString();
      LOG.info("fixing " + fileStr);

      Path file = new Path(fileStr);
      boolean success = false;

      try {
        boolean fixed = helper.fixFile(file, context);
        
        if (fixed) {
          context.getCounter(Counter.FILES_SUCCEEDED).increment(1L);
        } else {
          context.getCounter(Counter.FILES_NOACTION).increment(1L);
        }
      } catch (Exception e) {
        LOG.error(StringUtils.stringifyException(e));

        // report file as failed
        context.getCounter(Counter.FILES_FAILED).increment(1L);
        String outkey = fileStr;
        String outval = "failed";
        context.write(new Text(outkey), new Text(outval));
      }
      
      context.progress();
    }
  }

}
