package org.apache.hadoop;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.TaskAttemptContext;

public class CustomOutputCommitter extends OutputCommitter {

  public static final String JOB_SETUP_FILE_NAME = "_job_setup";
  public static final String JOB_COMMIT_FILE_NAME = "_job_commit";
  public static final String JOB_ABORT_FILE_NAME = "_job_abort";
  public static final String TASK_SETUP_FILE_NAME = "_task_setup";
  public static final String TASK_ABORT_FILE_NAME = "_task_abort";
  public static final String TASK_COMMIT_FILE_NAME = "_task_commit";

  @Override
  public void setupJob(JobContext jobContext) throws IOException {
    writeFile(jobContext.getJobConf(), JOB_SETUP_FILE_NAME);
  }

  @Override
  public void commitJob(JobContext jobContext) throws IOException {
    super.commitJob(jobContext);
    writeFile(jobContext.getJobConf(), JOB_COMMIT_FILE_NAME);
  }

  @Override
  public void abortJob(JobContext jobContext, int status) 
  throws IOException {
    super.abortJob(jobContext, status);
    writeFile(jobContext.getJobConf(), JOB_ABORT_FILE_NAME);
  }
  
  @Override
  public void setupTask(TaskAttemptContext taskContext) throws IOException {
    writeFile(taskContext.getJobConf(), TASK_SETUP_FILE_NAME);
  }

  @Override
  public boolean needsTaskCommit(TaskAttemptContext taskContext)
      throws IOException {
    return true;
  }

  @Override
  public void commitTask(TaskAttemptContext taskContext) throws IOException {
    writeFile(taskContext.getJobConf(), TASK_COMMIT_FILE_NAME);
  }

  @Override
  public void abortTask(TaskAttemptContext taskContext) throws IOException {
    writeFile(taskContext.getJobConf(), TASK_ABORT_FILE_NAME);
  }

  private void writeFile(JobConf conf , String filename) throws IOException {
    System.out.println("writing file ----" + filename);
    Path outputPath = FileOutputFormat.getOutputPath(conf);
    FileSystem fs = outputPath.getFileSystem(conf);
    fs.create(new Path(outputPath, filename)).close();
  }
}
