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

package org.apache.hadoop.mapred;

import java.io.IOException;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/** An {@link OutputCommitter} that commits files specified 
 * in job output directory i.e. ${mapreduce.output.fileoutputformat.outputdir}. 
 **/
@InterfaceAudience.Public
@InterfaceStability.Stable
public class FileOutputCommitter extends OutputCommitter {

  public static final Log LOG = LogFactory.getLog(
      "org.apache.hadoop.mapred.FileOutputCommitter");
  
  /**
   * Temporary directory name 
   */
  public static final String TEMP_DIR_NAME = "_temporary";
  public static final String SUCCEEDED_FILE_NAME = "_SUCCESS";
  static final String SUCCESSFUL_JOB_OUTPUT_DIR_MARKER = 
    "mapreduce.fileoutputcommitter.marksuccessfuljobs";

  public void setupJob(JobContext context) throws IOException {
    JobConf conf = context.getJobConf();
    Path outputPath = FileOutputFormat.getOutputPath(conf);
    if (outputPath != null) {
      Path tmpDir = 
          new Path(outputPath, getJobAttemptBaseDirName(context) + 
              Path.SEPARATOR + FileOutputCommitter.TEMP_DIR_NAME);
      FileSystem fileSys = tmpDir.getFileSystem(conf);
      if (!fileSys.mkdirs(tmpDir)) {
        LOG.error("Mkdirs failed to create " + tmpDir.toString());
      }
    }
  }

  // True if the job requires output.dir marked on successful job.
  // Note that by default it is set to true.
  private boolean shouldMarkOutputDir(JobConf conf) {
    return conf.getBoolean(SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, true);
  }
  
  public void commitJob(JobContext context) throws IOException {
    //delete the task temp directory from the current jobtempdir
    JobConf conf = context.getJobConf();
    Path outputPath = FileOutputFormat.getOutputPath(conf);
    if (outputPath != null) {
      FileSystem outputFileSystem = outputPath.getFileSystem(conf);
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
      moveJobOutputs(outputFileSystem, 
          jobOutputPath, outputPath, jobOutputPath);

      // delete the _temporary folder in the output folder
      cleanupJob(context);
      // check if the output-dir marking is required
      if (shouldMarkOutputDir(context.getJobConf())) {
        // create a _success file in the output folder
        markOutputDirSuccessful(context);
      }
    }
  }
  
  // Create a _success file in the job's output folder
  private void markOutputDirSuccessful(JobContext context) throws IOException {
    JobConf conf = context.getJobConf();
    // get the o/p path
    Path outputPath = FileOutputFormat.getOutputPath(conf);
    if (outputPath != null) {
      // get the filesys
      FileSystem fileSys = outputPath.getFileSystem(conf);
      // create a file in the output folder to mark the job completion
      Path filePath = new Path(outputPath, SUCCEEDED_FILE_NAME);
      fileSys.create(filePath).close();
    }
  }

  private void moveJobOutputs(FileSystem fs, final Path origJobOutputPath,
      Path finalOutputDir, Path jobOutput) throws IOException {
    LOG.debug("Told to move job output from " + jobOutput
        + " to " + finalOutputDir + 
        " and orig job output path is " + origJobOutputPath);  
    if (fs.isFile(jobOutput)) {
      Path finalOutputPath = 
          getFinalPath(fs, finalOutputDir, jobOutput, origJobOutputPath);
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
          getFinalPath(fs, finalOutputDir, jobOutput, origJobOutputPath);
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
    JobConf conf = context.getJobConf();
    // do the clean up of temporary directory
    Path outputPath = FileOutputFormat.getOutputPath(conf);
    if (outputPath != null) {
      Path tmpDir = new Path(outputPath, FileOutputCommitter.TEMP_DIR_NAME);
      FileSystem fileSys = tmpDir.getFileSystem(conf);
      context.getProgressible().progress();
      if (fileSys.exists(tmpDir)) {
        fileSys.delete(tmpDir, true);
      } else {
        LOG.warn("Output Path is Null in cleanup");
      }
    }
  }

  @Override
  public void abortJob(JobContext context, int runState) 
  throws IOException {
    // simply delete the _temporary dir from the o/p folder of the job
    cleanupJob(context);
  }
  
  public void setupTask(TaskAttemptContext context) throws IOException {
    // FileOutputCommitter's setupTask doesn't do anything. Because the
    // temporary task directory is created on demand when the 
    // task is writing.
  }
		  
  public void commitTask(TaskAttemptContext context) 
  throws IOException {
    Path taskOutputPath = getTempTaskOutputPath(context);
    TaskAttemptID attemptId = context.getTaskAttemptID();
    JobConf job = context.getJobConf();
    if (taskOutputPath != null) {
      FileSystem fs = taskOutputPath.getFileSystem(job);
      context.getProgressible().progress();
      if (fs.exists(taskOutputPath)) {
        // Move the task outputs to the current job attempt output dir
        JobConf conf = context.getJobConf();
        Path outputPath = FileOutputFormat.getOutputPath(conf);
        FileSystem outputFileSystem = outputPath.getFileSystem(conf);
        Path jobOutputPath = new Path(outputPath, getJobTempDirName(context));
        moveTaskOutputs(context, outputFileSystem, jobOutputPath, 
            taskOutputPath);

        // Delete the temporary task-specific output directory
        if (!fs.delete(taskOutputPath, true)) {
          LOG.info("Failed to delete the temporary output" + 
          " directory of task: " + attemptId + " - " + taskOutputPath);
        }
        LOG.info("Saved output of task '" + attemptId + "' to " + 
                 jobOutputPath);
      }
    }
  }
		  
  private void moveTaskOutputs(TaskAttemptContext context,
                               FileSystem fs,
                               Path jobOutputDir,
                               Path taskOutput) 
  throws IOException {
    TaskAttemptID attemptId = context.getTaskAttemptID();
    context.getProgressible().progress();
    LOG.debug("Told to move taskoutput from " + taskOutput
        + " to " + jobOutputDir);    
    if (fs.isFile(taskOutput)) {
      Path finalOutputPath = getFinalPath(fs, jobOutputDir, taskOutput, 
                                          getTempTaskOutputPath(context));
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
      Path finalOutputPath = getFinalPath(fs, jobOutputDir, taskOutput, 
	          getTempTaskOutputPath(context));
      fs.mkdirs(finalOutputPath);
      LOG.debug("Creating dirs along path " + finalOutputPath);
      if (paths != null) {
        for (FileStatus path : paths) {
          moveTaskOutputs(context, fs, jobOutputDir, path.getPath());
        }
      }
    }
  }

  public void abortTask(TaskAttemptContext context) throws IOException {
    Path taskOutputPath =  getTempTaskOutputPath(context);
    if (taskOutputPath != null) {
      FileSystem fs = taskOutputPath.getFileSystem(context.getJobConf());
      context.getProgressible().progress();
      fs.delete(taskOutputPath, true);
    }
  }

  @SuppressWarnings("deprecation")
  private Path getFinalPath(FileSystem fs, Path jobOutputDir, Path taskOutput, 
                            Path taskOutputPath) throws IOException {
    URI taskOutputUri = taskOutput.makeQualified(fs).toUri();
    URI taskOutputPathUri = taskOutputPath.makeQualified(fs).toUri();
    URI relativePath = taskOutputPathUri.relativize(taskOutputUri);
    if (taskOutputUri == relativePath) { 
      //taskOutputPath is not a parent of taskOutput
      throw new IOException("Can not get the relative path: base = " + 
          taskOutputPathUri + " child = " + taskOutputUri);
    }
    if (relativePath.getPath().length() > 0) {
      return new Path(jobOutputDir, relativePath.getPath());
    } else {
      return jobOutputDir;
    }
  }

  public boolean needsTaskCommit(TaskAttemptContext context) 
  throws IOException {
    Path taskOutputPath = getTempTaskOutputPath(context);
    if (taskOutputPath != null) {
      context.getProgressible().progress();
      // Get the file-system for the task output directory
      FileSystem fs = taskOutputPath.getFileSystem(context.getJobConf());
      // since task output path is created on demand, 
      // if it exists, task needs a commit
      if (fs.exists(taskOutputPath)) {
        return true;
      }
    }
    return false;
  }

  Path getTempTaskOutputPath(TaskAttemptContext taskContext) 
      throws IOException {
    JobConf conf = taskContext.getJobConf();
    Path outputPath = FileOutputFormat.getOutputPath(conf);
    if (outputPath != null) {
      Path p = new Path(outputPath,
                     (FileOutputCommitter.TEMP_DIR_NAME + Path.SEPARATOR +
                      "_" + taskContext.getTaskAttemptID().toString()));
      FileSystem fs = p.getFileSystem(conf);
      return p.makeQualified(fs);
    }
    return null;
  }
  
  Path getWorkPath(TaskAttemptContext taskContext, Path basePath) 
  throws IOException {
    // ${mapred.out.dir}/_temporary
    Path jobTmpDir = new Path(basePath, FileOutputCommitter.TEMP_DIR_NAME);
    FileSystem fs = jobTmpDir.getFileSystem(taskContext.getJobConf());
    if (!fs.exists(jobTmpDir)) {
      throw new IOException("The temporary job-output directory " + 
          jobTmpDir.toString() + " doesn't exist!"); 
    }
    // ${mapred.out.dir}/_temporary/_${taskid}
    String taskid = taskContext.getTaskAttemptID().toString();
    Path taskTmpDir = new Path(jobTmpDir, "_" + taskid);
    if (!fs.mkdirs(taskTmpDir)) {
      throw new IOException("Mkdirs failed to create " 
          + taskTmpDir.toString());
    }
    return taskTmpDir;
  }
  
  @Override
  public boolean isRecoverySupported() {
    return true;
  }
  
  @Override
  public void recoverTask(TaskAttemptContext context)
      throws IOException {
    Path outputPath = FileOutputFormat.getOutputPath(context.getJobConf());
    context.progress();
    Path jobOutputPath = new Path(outputPath, getJobTempDirName(context));
    int previousAttempt =         
        context.getConfiguration().getInt(
            MRConstants.APPLICATION_ATTEMPT_ID, 0) - 1;
    if (previousAttempt < 0) {
      LOG.warn("Cannot recover task output for first attempt...");
      return;
    }

    FileSystem outputFileSystem = 
        outputPath.getFileSystem(context.getJobConf());
    Path pathToRecover = 
        new Path(outputPath, getJobAttemptBaseDirName(previousAttempt));
    if (outputFileSystem.exists(pathToRecover)) {
      // Move the task outputs to their final place
      LOG.debug("Trying to recover task from " + pathToRecover
          + " into " + jobOutputPath);
      moveJobOutputs(outputFileSystem, 
          pathToRecover, jobOutputPath, pathToRecover);
      LOG.info("Saved output of job to " + jobOutputPath);
    }
  }

  protected static String getJobAttemptBaseDirName(JobContext context) {
    int appAttemptId = 
        context.getJobConf().getInt(
            MRConstants.APPLICATION_ATTEMPT_ID, 0);
    return getJobAttemptBaseDirName(appAttemptId);
  }

  protected static String getJobTempDirName(TaskAttemptContext context) {
    int appAttemptId = 
        context.getJobConf().getInt(
            MRConstants.APPLICATION_ATTEMPT_ID, 0);
    return getJobAttemptBaseDirName(appAttemptId);
  }

  protected static String getJobAttemptBaseDirName(int appAttemptId) {
    return FileOutputCommitter.TEMP_DIR_NAME + Path.SEPARATOR + 
      + appAttemptId;
  }

  protected static String getTaskAttemptBaseDirName(
      TaskAttemptContext context) {
    return getJobTempDirName(context) + Path.SEPARATOR + 
      FileOutputCommitter.TEMP_DIR_NAME + Path.SEPARATOR +
      "_" + context.getTaskAttemptID().toString();
  }
}
