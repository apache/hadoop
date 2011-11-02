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

package org.apache.hadoop.mapreduce.lib.output;

import java.io.IOException;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

/** An {@link OutputCommitter} that commits files specified 
 * in job output directory i.e. ${mapreduce.output.fileoutputformat.outputdir}. 
 **/
@InterfaceAudience.Public
@InterfaceStability.Stable
public class FileOutputCommitter extends OutputCommitter {

  private static final Log LOG = LogFactory.getLog(FileOutputCommitter.class);

  /**
   * Temporary directory name 
   */
  protected static final String TEMP_DIR_NAME = "_temporary";
  public static final String SUCCEEDED_FILE_NAME = "_SUCCESS";
  static final String SUCCESSFUL_JOB_OUTPUT_DIR_MARKER = 
    "mapreduce.fileoutputcommitter.marksuccessfuljobs";
  private FileSystem outputFileSystem = null;
  private Path outputPath = null;
  private Path workPath = null;

  /**
   * Create a file output committer
   * @param outputPath the job's output path
   * @param context the task's context
   * @throws IOException
   */
  public FileOutputCommitter(Path outputPath, 
                             TaskAttemptContext context) throws IOException {
    if (outputPath != null) {
      this.outputPath = outputPath;
      outputFileSystem = outputPath.getFileSystem(context.getConfiguration());
      workPath = new Path(outputPath,
                          getTaskAttemptBaseDirName(context))
                          .makeQualified(outputFileSystem);
    }
  }

  /**
   * Create the temporary directory that is the root of all of the task 
   * work directories.
   * @param context the job's context
   */
  public void setupJob(JobContext context) throws IOException {
    if (outputPath != null) {
      Path tmpDir = new Path(outputPath, getJobAttemptBaseDirName(context) + 
    		  Path.SEPARATOR + FileOutputCommitter.TEMP_DIR_NAME);
      FileSystem fileSys = tmpDir.getFileSystem(context.getConfiguration());
      if (!fileSys.mkdirs(tmpDir)) {
        LOG.error("Mkdirs failed to create " + tmpDir.toString());
      }
    }
  }

  // True if the job requires output.dir marked on successful job.
  // Note that by default it is set to true.
  private boolean shouldMarkOutputDir(Configuration conf) {
    return conf.getBoolean(SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, true);
  }
  
  // Create a _success file in the job's output dir
  private void markOutputDirSuccessful(MRJobConfig context) throws IOException {
    if (outputPath != null) {
      // create a file in the output folder to mark the job completion
      Path filePath = new Path(outputPath, SUCCEEDED_FILE_NAME);
      outputFileSystem.create(filePath).close();
    }
  }
  
  /**
   * Move all job output to the final place.
   * Delete the temporary directory, including all of the work directories.
   * Create a _SUCCESS file to make it as successful.
   * @param context the job's context
   */
  public void commitJob(JobContext context) throws IOException {
    if (outputPath != null) {
      //delete the task temp directory from the current jobtempdir
      Path tmpDir = new Path(outputPath, getJobAttemptBaseDirName(context) +
          Path.SEPARATOR + FileOutputCommitter.TEMP_DIR_NAME);
      FileSystem fileSys = tmpDir.getFileSystem(context.getConfiguration());
      if (fileSys.exists(tmpDir)) {
        fileSys.delete(tmpDir, true);
      } else {
        LOG.warn("Task temp dir could not be deleted " + tmpDir);
      }

      //move the job output to final place
      Path jobOutputPath = 
          new Path(outputPath, getJobAttemptBaseDirName(context));
      moveJobOutputs(outputFileSystem, jobOutputPath, outputPath, jobOutputPath);

      // delete the _temporary folder and create a _done file in the o/p folder
      cleanupJob(context);
      if (shouldMarkOutputDir(context.getConfiguration())) {
        markOutputDirSuccessful(context);
      }
    }
  }

  /**
   * Move job output to final location 
   * @param fs Filesystem handle
   * @param origJobOutputPath The original location of the job output
   * Required to generate the relative path for correct moving of data. 
   * @param finalOutputDir The final output directory to which the job output 
   *                       needs to be moved
   * @param jobOutput The current job output directory being moved 
   * @throws IOException
   */
  private void moveJobOutputs(FileSystem fs, final Path origJobOutputPath, 
      Path finalOutputDir, Path jobOutput) throws IOException {
    LOG.debug("Told to move job output from " + jobOutput
        + " to " + finalOutputDir + 
        " and orig job output path is " + origJobOutputPath);    
    if (fs.isFile(jobOutput)) {
      Path finalOutputPath = 
          getFinalPath(finalOutputDir, jobOutput, origJobOutputPath);
      if (!fs.rename(jobOutput, finalOutputPath)) {
        if (!fs.delete(finalOutputPath, true)) {
          throw new IOException("Failed to delete earlier output of job");
        }
        if (!fs.rename(jobOutput, finalOutputPath)) {
          throw new IOException("Failed to save output of job");
        }
      }
      LOG.debug("Moved job output file from " + jobOutput + " to " + 
          finalOutputPath);
    } else if (fs.getFileStatus(jobOutput).isDirectory()) {
      LOG.debug("Job output file " + jobOutput + " is a dir");
      FileStatus[] paths = fs.listStatus(jobOutput);
      Path finalOutputPath = 
          getFinalPath(finalOutputDir, jobOutput, origJobOutputPath);
      fs.mkdirs(finalOutputPath);
      LOG.debug("Creating dirs along job output path " + finalOutputPath);
      if (paths != null) {
        for (FileStatus path : paths) {
          moveJobOutputs(fs, origJobOutputPath, finalOutputDir, path.getPath());
        }
      }
    }
  }

  @Override
  @Deprecated
  public void cleanupJob(JobContext context) throws IOException {
    if (outputPath != null) {
      Path tmpDir = new Path(outputPath, FileOutputCommitter.TEMP_DIR_NAME);
      FileSystem fileSys = tmpDir.getFileSystem(context.getConfiguration());
      if (fileSys.exists(tmpDir)) {
        fileSys.delete(tmpDir, true);
      }
    } else {
      LOG.warn("Output Path is null in cleanup");
    }
  }

  /**
   * Delete the temporary directory, including all of the work directories.
   * @param context the job's context
   */
  @Override
  public void abortJob(JobContext context, JobStatus.State state) 
  throws IOException {
    // delete the _temporary folder
    cleanupJob(context);
  }
  
  /**
   * No task setup required.
   */
  @Override
  public void setupTask(TaskAttemptContext context) throws IOException {
    // FileOutputCommitter's setupTask doesn't do anything. Because the
    // temporary task directory is created on demand when the 
    // task is writing.
  }

  /**
   * Move the files from the work directory to the job output directory
   * @param context the task context
   */
  public void commitTask(TaskAttemptContext context) 
  throws IOException {
    TaskAttemptID attemptId = context.getTaskAttemptID();
    if (workPath != null) {
      context.progress();
      if (outputFileSystem.exists(workPath)) {
        // Move the task outputs to the current job attempt output dir
    	  Path jobOutputPath = 
    	      new Path(outputPath, getJobAttemptBaseDirName(context));
        moveTaskOutputs(context, outputFileSystem, jobOutputPath, workPath);
        // Delete the temporary task-specific output directory
        if (!outputFileSystem.delete(workPath, true)) {
          LOG.warn("Failed to delete the temporary output" + 
          " directory of task: " + attemptId + " - " + workPath);
        }
        LOG.info("Saved output of task '" + attemptId + "' to " + 
                 outputPath);
      }
    }
  }

  /**
   * Move all of the files from the work directory to the final output
   * @param context the task context
   * @param fs the output file system
   * @param jobOutputDir the final output direcotry
   * @param taskOutput the work path
   * @throws IOException
   */
  private void moveTaskOutputs(TaskAttemptContext context,
                               FileSystem fs,
                               Path jobOutputDir,
                               Path taskOutput) 
  throws IOException {
    TaskAttemptID attemptId = context.getTaskAttemptID();
    context.progress();
    LOG.debug("Told to move taskoutput from " + taskOutput
        + " to " + jobOutputDir);    
    if (fs.isFile(taskOutput)) {
      Path finalOutputPath = getFinalPath(jobOutputDir, taskOutput, 
                                          workPath);
      if (!fs.rename(taskOutput, finalOutputPath)) {
        if (!fs.delete(finalOutputPath, true)) {
          throw new IOException("Failed to delete earlier output of task: " + 
                                 attemptId);
        }
        if (!fs.rename(taskOutput, finalOutputPath)) {
          throw new IOException("Failed to save output of task: " + 
        		  attemptId);
        }
      }
      LOG.debug("Moved " + taskOutput + " to " + finalOutputPath);
    } else if(fs.getFileStatus(taskOutput).isDirectory()) {
      LOG.debug("Taskoutput " + taskOutput + " is a dir");
      FileStatus[] paths = fs.listStatus(taskOutput);
      Path finalOutputPath = getFinalPath(jobOutputDir, taskOutput, workPath);
      fs.mkdirs(finalOutputPath);
      LOG.debug("Creating dirs along path " + finalOutputPath);
      if (paths != null) {
        for (FileStatus path : paths) {
          moveTaskOutputs(context, fs, jobOutputDir, path.getPath());
        }
      }
    }
  }

  /**
   * Delete the work directory
   * @throws IOException 
   */
  @Override
  public void abortTask(TaskAttemptContext context) throws IOException {
    if (workPath != null) { 
      context.progress();
      outputFileSystem.delete(workPath, true);
    }
  }

  /**
   * Find the final name of a given output file, given the job output directory
   * and the work directory.
   * @param jobOutputDir the job's output directory
   * @param taskOutput the specific task output file
   * @param taskOutputPath the job's work directory
   * @return the final path for the specific output file
   * @throws IOException
   */
  private Path getFinalPath(Path jobOutputDir, Path taskOutput, 
                            Path taskOutputPath) throws IOException {    
    URI taskOutputUri = taskOutput.makeQualified(outputFileSystem.getUri(), 
        outputFileSystem.getWorkingDirectory()).toUri();
    URI taskOutputPathUri = 
        taskOutputPath.makeQualified(
            outputFileSystem.getUri(),
            outputFileSystem.getWorkingDirectory()).toUri();
    URI relativePath = taskOutputPathUri.relativize(taskOutputUri);
    if (taskOutputUri == relativePath) {
      throw new IOException("Can not get the relative path: base = " + 
          taskOutputPathUri + " child = " + taskOutputUri);
    }
    if (relativePath.getPath().length() > 0) {
      return new Path(jobOutputDir, relativePath.getPath());
    } else {
      return jobOutputDir;
    }
  }

  /**
   * Did this task write any files in the work directory?
   * @param context the task's context
   */
  @Override
  public boolean needsTaskCommit(TaskAttemptContext context
                                 ) throws IOException {
    return workPath != null && outputFileSystem.exists(workPath);
  }

  /**
   * Get the directory that the task should write results into
   * @return the work directory
   * @throws IOException
   */
  public Path getWorkPath() throws IOException {
    return workPath;
  }

  @Override
  public boolean isRecoverySupported() {
    return true;
  }
  
  @Override
  public void recoverTask(TaskAttemptContext context)
      throws IOException {
    context.progress();
    Path jobOutputPath = 
        new Path(outputPath, getJobAttemptBaseDirName(context));
    int previousAttempt =         
        context.getConfiguration().getInt(
            MRJobConfig.APPLICATION_ATTEMPT_ID, 0) - 1;
    if (previousAttempt < 0) {
      throw new IOException ("Cannot recover task output for first attempt...");
    }

    Path pathToRecover = 
        new Path(outputPath, getJobAttemptBaseDirName(previousAttempt));
    LOG.debug("Trying to recover task from " + pathToRecover
        + " into " + jobOutputPath);
    if (outputFileSystem.exists(pathToRecover)) {
      // Move the task outputs to their final place
      moveJobOutputs(outputFileSystem, 
          pathToRecover, jobOutputPath, pathToRecover);
      LOG.info("Saved output of job to " + jobOutputPath);
    }
  }

  protected static String getJobAttemptBaseDirName(JobContext context) {
    int appAttemptId = 
        context.getConfiguration().getInt(
            MRJobConfig.APPLICATION_ATTEMPT_ID, 0);
    return getJobAttemptBaseDirName(appAttemptId);
  }

  protected static String getJobAttemptBaseDirName(int appAttemptId) {
    return FileOutputCommitter.TEMP_DIR_NAME + Path.SEPARATOR + 
      + appAttemptId;
  }

  protected static String getTaskAttemptBaseDirName(
      TaskAttemptContext context) {
	  return getJobAttemptBaseDirName(context) + Path.SEPARATOR + 
	  FileOutputCommitter.TEMP_DIR_NAME + Path.SEPARATOR +
      "_" + context.getTaskAttemptID().toString();
  }
}
