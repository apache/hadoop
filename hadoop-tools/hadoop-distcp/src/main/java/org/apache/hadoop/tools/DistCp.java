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

package org.apache.hadoop.tools;

import java.io.IOException;
import java.util.Random;

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobSubmissionFiles;
import org.apache.hadoop.tools.CopyListing.*;
import org.apache.hadoop.tools.mapred.CopyMapper;
import org.apache.hadoop.tools.mapred.CopyOutputFormat;
import org.apache.hadoop.tools.util.DistCpUtils;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.annotations.VisibleForTesting;

/**
 * DistCp is the main driver-class for DistCpV2.
 * For command-line use, DistCp::main() orchestrates the parsing of command-line
 * parameters and the launch of the DistCp job.
 * For programmatic use, a DistCp object can be constructed by specifying
 * options (in a DistCpOptions object), and DistCp::execute() may be used to
 * launch the copy-job. DistCp may alternatively be sub-classed to fine-tune
 * behaviour.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class DistCp extends Configured implements Tool {

  /**
   * Priority of the shutdown hook.
   */
  static final int SHUTDOWN_HOOK_PRIORITY = 30;

  static final Log LOG = LogFactory.getLog(DistCp.class);

  @VisibleForTesting
  DistCpContext context;

  private Path metaFolder;

  private static final String PREFIX = "_distcp";
  private static final String WIP_PREFIX = "._WIP_";
  private static final String DISTCP_DEFAULT_XML = "distcp-default.xml";
  private static final String DISTCP_SITE_XML = "distcp-site.xml";
  static final Random rand = new Random();

  private boolean submitted;
  private FileSystem jobFS;

  private void prepareFileListing(Job job) throws Exception {
    if (context.shouldUseSnapshotDiff()) {
      // When "-diff" or "-rdiff" is passed, do sync() first, then
      // create copyListing based on snapshot diff.
      DistCpSync distCpSync = new DistCpSync(context, getConf());
      if (distCpSync.sync()) {
        createInputFileListingWithDiff(job, distCpSync);
      } else {
        throw new Exception("DistCp sync failed, input options: " + context);
      }
    } else {
      // When no "-diff" or "-rdiff" is passed, create copyListing
      // in regular way.
      createInputFileListing(job);
    }
  }

  /**
   * Public Constructor. Creates DistCp object with specified input-parameters.
   * (E.g. source-paths, target-location, etc.)
   * @param configuration configuration against which the Copy-mapper must run
   * @param inputOptions Immutable options
   * @throws Exception
   */
  public DistCp(Configuration configuration, DistCpOptions inputOptions)
      throws Exception {
    Configuration config = new Configuration(configuration);
    config.addResource(DISTCP_DEFAULT_XML);
    config.addResource(DISTCP_SITE_XML);
    setConf(config);
    if (inputOptions != null) {
      this.context = new DistCpContext(inputOptions);
    }
    this.metaFolder   = createMetaFolderPath();
  }

  /**
   * To be used with the ToolRunner. Not for public consumption.
   */
  @VisibleForTesting
  DistCp() {}

  /**
   * Implementation of Tool::run(). Orchestrates the copy of source file(s)
   * to target location, by:
   *  1. Creating a list of files to be copied to target.
   *  2. Launching a Map-only job to copy the files. (Delegates to execute().)
   * @param argv List of arguments passed to DistCp, from the ToolRunner.
   * @return On success, it returns 0. Else, -1.
   */
  @Override
  public int run(String[] argv) {
    if (argv.length < 1) {
      OptionsParser.usage();
      return DistCpConstants.INVALID_ARGUMENT;
    }
    
    try {
      context = new DistCpContext(OptionsParser.parse(argv));
      checkSplitLargeFile();
      setTargetPathExists();
      LOG.info("Input Options: " + context);
    } catch (Throwable e) {
      LOG.error("Invalid arguments: ", e);
      System.err.println("Invalid arguments: " + e.getMessage());
      OptionsParser.usage();      
      return DistCpConstants.INVALID_ARGUMENT;
    }
    
    try {
      execute();
    } catch (InvalidInputException e) {
      LOG.error("Invalid input: ", e);
      return DistCpConstants.INVALID_ARGUMENT;
    } catch (DuplicateFileException e) {
      LOG.error("Duplicate files in input path: ", e);
      return DistCpConstants.DUPLICATE_INPUT;
    } catch (AclsNotSupportedException e) {
      LOG.error("ACLs not supported on at least one file system: ", e);
      return DistCpConstants.ACLS_NOT_SUPPORTED;
    } catch (XAttrsNotSupportedException e) {
      LOG.error("XAttrs not supported on at least one file system: ", e);
      return DistCpConstants.XATTRS_NOT_SUPPORTED;
    } catch (Exception e) {
      LOG.error("Exception encountered ", e);
      return DistCpConstants.UNKNOWN_ERROR;
    }
    return DistCpConstants.SUCCESS;
  }

  /**
   * Implements the core-execution. Creates the file-list for copy,
   * and launches the Hadoop-job, to do the copy.
   * @return Job handle
   * @throws Exception
   */
  public Job execute() throws Exception {
    Preconditions.checkState(context != null,
        "The DistCpContext should have been created before running DistCp!");
    Job job = createAndSubmitJob();

    if (context.shouldBlock()) {
      waitForJobCompletion(job);
    }
    return job;
  }

  /**
   * Create and submit the mapreduce job.
   * @return The mapreduce job object that has been submitted
   */
  public Job createAndSubmitJob() throws Exception {
    assert context != null;
    assert getConf() != null;
    Job job = null;
    try {
      synchronized(this) {
        //Don't cleanup while we are setting up.
        metaFolder = createMetaFolderPath();
        jobFS = metaFolder.getFileSystem(getConf());
        job = createJob();
      }
      prepareFileListing(job);
      job.submit();
      submitted = true;
    } finally {
      if (!submitted) {
        cleanup();
      }
    }

    String jobID = job.getJobID().toString();
    job.getConfiguration().set(DistCpConstants.CONF_LABEL_DISTCP_JOB_ID,
        jobID);
    LOG.info("DistCp job-id: " + jobID);

    return job;
  }

  /**
   * Wait for the given job to complete.
   * @param job the given mapreduce job that has already been submitted
   */
  public void waitForJobCompletion(Job job) throws Exception {
    assert job != null;
    if (!job.waitForCompletion(true)) {
      throw new IOException("DistCp failure: Job " + job.getJobID()
          + " has failed: " + job.getStatus().getFailureInfo());
    }
  }

  /**
   * Set targetPathExists in both inputOptions and job config,
   * for the benefit of CopyCommitter
   */
  private void setTargetPathExists() throws IOException {
    Path target = context.getTargetPath();
    FileSystem targetFS = target.getFileSystem(getConf());
    boolean targetExists = targetFS.exists(target);
    context.setTargetPathExists(targetExists);
    getConf().setBoolean(DistCpConstants.CONF_LABEL_TARGET_PATH_EXISTS, 
        targetExists);
  }

  /**
   * Check splitting large files is supported and populate configs.
   */
  private void checkSplitLargeFile() throws IOException {
    if (!context.splitLargeFile()) {
      return;
    }

    final Path target = context.getTargetPath();
    final FileSystem targetFS = target.getFileSystem(getConf());
    try {
      Path[] src = null;
      Path tgt = null;
      targetFS.concat(tgt, src);
    } catch (UnsupportedOperationException use) {
      throw new UnsupportedOperationException(
          DistCpOptionSwitch.BLOCKS_PER_CHUNK.getSwitch() +
              " is not supported since the target file system doesn't" +
              " support concat.", use);
    } catch (Exception e) {
      // Ignore other exception
    }

    LOG.info("Set " +
        DistCpConstants.CONF_LABEL_SIMPLE_LISTING_RANDOMIZE_FILES
        + " to false since " + DistCpOptionSwitch.BLOCKS_PER_CHUNK.getSwitch()
        + " is passed.");
    getConf().setBoolean(
        DistCpConstants.CONF_LABEL_SIMPLE_LISTING_RANDOMIZE_FILES, false);
  }

  /**
   * Create Job object for submitting it, with all the configuration
   *
   * @return Reference to job object.
   * @throws IOException - Exception if any
   */
  private Job createJob() throws IOException {
    String jobName = "distcp";
    String userChosenName = getConf().get(JobContext.JOB_NAME);
    if (userChosenName != null)
      jobName += ": " + userChosenName;
    Job job = Job.getInstance(getConf());
    job.setJobName(jobName);
    job.setInputFormatClass(DistCpUtils.getStrategy(getConf(), context));
    job.setJarByClass(CopyMapper.class);
    configureOutputFormat(job);

    job.setMapperClass(CopyMapper.class);
    job.setNumReduceTasks(0);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputFormatClass(CopyOutputFormat.class);
    job.getConfiguration().set(JobContext.MAP_SPECULATIVE, "false");
    job.getConfiguration().set(JobContext.NUM_MAPS,
                  String.valueOf(context.getMaxMaps()));

    context.appendToConf(job.getConfiguration());
    return job;
  }

  /**
   * Setup output format appropriately
   *
   * @param job - Job handle
   * @throws IOException - Exception if any
   */
  private void configureOutputFormat(Job job) throws IOException {
    final Configuration configuration = job.getConfiguration();
    Path targetPath = context.getTargetPath();
    FileSystem targetFS = targetPath.getFileSystem(configuration);
    targetPath = targetPath.makeQualified(targetFS.getUri(),
                                          targetFS.getWorkingDirectory());
    if (context.shouldPreserve(
        DistCpOptions.FileAttribute.ACL)) {
      DistCpUtils.checkFileSystemAclSupport(targetFS);
    }
    if (context.shouldPreserve(
        DistCpOptions.FileAttribute.XATTR)) {
      DistCpUtils.checkFileSystemXAttrSupport(targetFS);
    }
    if (context.shouldAtomicCommit()) {
      Path workDir = context.getAtomicWorkPath();
      if (workDir == null) {
        workDir = targetPath.getParent();
      }
      workDir = new Path(workDir, WIP_PREFIX + targetPath.getName()
                                + rand.nextInt());
      FileSystem workFS = workDir.getFileSystem(configuration);
      if (!FileUtil.compareFs(targetFS, workFS)) {
        throw new IllegalArgumentException("Work path " + workDir +
            " and target path " + targetPath + " are in different file system");
      }
      CopyOutputFormat.setWorkingDirectory(job, workDir);
    } else {
      CopyOutputFormat.setWorkingDirectory(job, targetPath);
    }
    CopyOutputFormat.setCommitDirectory(job, targetPath);

    Path logPath = context.getLogPath();
    if (logPath == null) {
      logPath = new Path(metaFolder, "_logs");
    } else {
      LOG.info("DistCp job log path: " + logPath);
    }
    CopyOutputFormat.setOutputPath(job, logPath);
  }

  /**
   * Create input listing by invoking an appropriate copy listing
   * implementation. Also add delegation tokens for each path
   * to job's credential store
   *
   * @param job - Handle to job
   * @return Returns the path where the copy listing is created
   * @throws IOException - If any
   */
  protected Path createInputFileListing(Job job) throws IOException {
    Path fileListingPath = getFileListingPath();
    CopyListing copyListing = CopyListing.getCopyListing(job.getConfiguration(),
        job.getCredentials(), context);
    copyListing.buildListing(fileListingPath, context);
    return fileListingPath;
  }

  /**
   * Create input listing based on snapshot diff report.
   * @param job - Handle to job
   * @param distCpSync the class wraps the snapshot diff report
   * @return Returns the path where the copy listing is created
   * @throws IOException - If any
   */
  private Path createInputFileListingWithDiff(Job job, DistCpSync distCpSync)
      throws IOException {
    Path fileListingPath = getFileListingPath();
    CopyListing copyListing = new SimpleCopyListing(job.getConfiguration(),
        job.getCredentials(), distCpSync);
    copyListing.buildListing(fileListingPath, context);
    return fileListingPath;
  }

  /**
   * Get default name of the copy listing file. Use the meta folder
   * to create the copy listing file
   *
   * @return - Path where the copy listing file has to be saved
   * @throws IOException - Exception if any
   */
  protected Path getFileListingPath() throws IOException {
    String fileListPathStr = metaFolder + "/fileList.seq";
    Path path = new Path(fileListPathStr);
    return new Path(path.toUri().normalize().toString());
  }

  /**
   * Create a default working folder for the job, under the
   * job staging directory
   *
   * @return Returns the working folder information
   * @throws Exception - Exception if any
   */
  private Path createMetaFolderPath() throws Exception {
    Configuration configuration = getConf();
    Path stagingDir = JobSubmissionFiles.getStagingDir(
            new Cluster(configuration), configuration);
    Path metaFolderPath = new Path(stagingDir, PREFIX + String.valueOf(rand.nextInt()));
    if (LOG.isDebugEnabled())
      LOG.debug("Meta folder location: " + metaFolderPath);
    configuration.set(DistCpConstants.CONF_LABEL_META_FOLDER, metaFolderPath.toString());    
    return metaFolderPath;
  }

  /**
   * Main function of the DistCp program. Parses the input arguments (via OptionsParser),
   * and invokes the DistCp::run() method, via the ToolRunner.
   * @param argv Command-line arguments sent to DistCp.
   */
  public static void main(String argv[]) {
    int exitCode;
    try {
      DistCp distCp = new DistCp();
      Cleanup CLEANUP = new Cleanup(distCp);

      ShutdownHookManager.get().addShutdownHook(CLEANUP,
        SHUTDOWN_HOOK_PRIORITY);
      exitCode = ToolRunner.run(getDefaultConf(), distCp, argv);
    }
    catch (Exception e) {
      LOG.error("Couldn't complete DistCp operation: ", e);
      exitCode = DistCpConstants.UNKNOWN_ERROR;
    }
    System.exit(exitCode);
  }

  /**
   * Loads properties from distcp-default.xml into configuration
   * object
   * @return Configuration which includes properties from distcp-default.xml
   *         and distcp-site.xml
   */
  private static Configuration getDefaultConf() {
    Configuration config = new Configuration();
    config.addResource(DISTCP_DEFAULT_XML);
    config.addResource(DISTCP_SITE_XML);
    return config;
  }

  private synchronized void cleanup() {
    try {
      if (metaFolder != null) {
        if (jobFS != null) {
          jobFS.delete(metaFolder, true);
        }
        metaFolder = null;
      }
    } catch (IOException e) {
      LOG.error("Unable to cleanup meta folder: " + metaFolder, e);
    }
  }

  private boolean isSubmitted() {
    return submitted;
  }

  private static class Cleanup implements Runnable {
    private final DistCp distCp;

    Cleanup(DistCp distCp) {
      this.distCp = distCp;
    }

    @Override
    public void run() {
      if (distCp.isSubmitted()) return;

      distCp.cleanup();
    }
  }
}
