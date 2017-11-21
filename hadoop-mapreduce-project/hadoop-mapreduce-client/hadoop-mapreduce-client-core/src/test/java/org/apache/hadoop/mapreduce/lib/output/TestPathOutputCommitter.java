/*
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

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.security.Credentials;

/**
 * Test the path output committer binding to FileOutputFormat.
 */
public class TestPathOutputCommitter extends Assert {

  @Test
  public void testFileOutputCommitterOverrride() throws Throwable {
    TaskContext context = new TaskContext();
    Path workPath = new Path("file:///work");
    context.setOutputCommitter(
        new SimpleCommitter(new Path("/"), context, workPath));
    assertEquals(workPath, FileOutputFormat.getWorkOutputPath(context));
  }

  @Test
  public void testFileOutputCommitterNullWorkPath() throws Throwable {
    TaskContext context = new TaskContext();
    context.setOutputCommitter(
        new SimpleCommitter(new Path("/"), context, null));
    assertNull(FileOutputFormat.getWorkOutputPath(context));
  }

  private static class SimpleCommitter extends PathOutputCommitter {

    private final Path workPath;

    SimpleCommitter(Path outputPath,
        TaskAttemptContext context, Path workPath) throws IOException {
      super(outputPath, context);
      this.workPath = workPath;
    }

    SimpleCommitter(Path outputPath,
        JobContext context, Path workPath) throws IOException {
      super(outputPath, context);
      this.workPath = workPath;
    }

    @Override
    public Path getWorkPath() throws IOException {
      return workPath;
    }

    @Override
    public void setupJob(JobContext jobContext) throws IOException {

    }

    @Override
    public void setupTask(TaskAttemptContext taskContext) throws IOException {

    }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext taskContext)
        throws IOException {
      return false;
    }

    @Override
    public void commitTask(TaskAttemptContext taskContext) throws IOException {

    }

    @Override
    public void abortTask(TaskAttemptContext taskContext) throws IOException {

    }

    @Override
    public Path getOutputPath() {
      return null;
    }
  }

  /**
   * Stub task context.
   * The {@link #getConfiguration()} method returns the configuration supplied
   * in the constructor; while {@link #setOutputCommitter(OutputCommitter)}
   * sets the committer returned in {@link #getOutputCommitter()}.
   * Otherwise, the methods are all no-ops.
   */
  public static class TaskContext
      implements TaskInputOutputContext<String, String, String, String> {

    private final Configuration configuration;

    public TaskContext() {
      this(new Configuration());
    }

    public TaskContext(Configuration conf) {
      this.configuration = conf;
    }


    private OutputCommitter outputCommitter;

    public void setOutputCommitter(OutputCommitter outputCommitter) {
      this.outputCommitter = outputCommitter;
    }

    @Override
    public OutputCommitter getOutputCommitter() {
      return outputCommitter;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      return false;
    }

    @Override
    public String getCurrentKey() throws IOException, InterruptedException {
      return null;
    }

    @Override
    public String getCurrentValue() throws IOException, InterruptedException {
      return null;
    }

    @Override
    public void write(String key, String value)
        throws IOException, InterruptedException {
    }


    @Override
    public TaskAttemptID getTaskAttemptID() {
      return null;
    }

    @Override
    public void setStatus(String msg) {
    }

    @Override
    public String getStatus() {
      return null;
    }

    @Override
    public float getProgress() {
      return 0;
    }

    @Override
    public Counter getCounter(Enum<?> counterName) {
      return null;
    }

    @Override
    public Counter getCounter(String groupName, String counterName) {
      return null;
    }

    @Override
    public Configuration getConfiguration() {
      return configuration;
    }

    @Override
    public Credentials getCredentials() {
      return null;
    }

    @Override
    public JobID getJobID() {
      return null;
    }

    @Override
    public int getNumReduceTasks() {
      return 0;
    }

    @Override
    public Path getWorkingDirectory() throws IOException {
      return null;
    }

    @Override
    public Class<?> getOutputKeyClass() {
      return null;
    }

    @Override
    public Class<?> getOutputValueClass() {
      return null;
    }

    @Override
    public Class<?> getMapOutputKeyClass() {
      return null;
    }

    @Override
    public Class<?> getMapOutputValueClass() {
      return null;
    }

    @Override
    public String getJobName() {
      return null;
    }

    @Override
    public Class<? extends InputFormat<?, ?>> getInputFormatClass()
        throws ClassNotFoundException {
      return null;
    }

    @Override
    public Class<? extends Mapper<?, ?, ?, ?>> getMapperClass()
        throws ClassNotFoundException {
      return null;
    }

    @Override
    public Class<? extends Reducer<?, ?, ?, ?>> getCombinerClass()
        throws ClassNotFoundException {
      return null;
    }

    @Override
    public Class<? extends Reducer<?, ?, ?, ?>> getReducerClass()
        throws ClassNotFoundException {
      return null;
    }

    @Override
    public Class<? extends OutputFormat<?, ?>> getOutputFormatClass()
        throws ClassNotFoundException {
      return null;
    }

    @Override
    public Class<? extends Partitioner<?, ?>> getPartitionerClass()
        throws ClassNotFoundException {
      return null;
    }

    @Override
    public RawComparator<?> getSortComparator() {
      return null;
    }

    @Override
    public String getJar() {
      return null;
    }

    @Override
    public RawComparator<?> getCombinerKeyGroupingComparator() {
      return null;
    }

    @Override
    public RawComparator<?> getGroupingComparator() {
      return null;
    }

    @Override
    public boolean getJobSetupCleanupNeeded() {
      return false;
    }

    @Override
    public boolean getTaskCleanupNeeded() {
      return false;
    }

    @Override
    public boolean getProfileEnabled() {
      return false;
    }

    @Override
    public String getProfileParams() {
      return null;
    }

    @Override
    public Configuration.IntegerRanges getProfileTaskRange(boolean isMap) {
      return null;
    }

    @Override
    public String getUser() {
      return null;
    }

    @Override
    public boolean getSymlink() {
      return false;
    }

    @Override
    public Path[] getArchiveClassPaths() {
      return new Path[0];
    }

    @Override
    public URI[] getCacheArchives() throws IOException {
      return new URI[0];
    }

    @Override
    public URI[] getCacheFiles() throws IOException {
      return new URI[0];
    }

    @Override
    public Path[] getLocalCacheArchives() throws IOException {
      return new Path[0];
    }

    @Override
    public Path[] getLocalCacheFiles() throws IOException {
      return new Path[0];
    }

    @Override
    public Path[] getFileClassPaths() {
      return new Path[0];
    }

    @Override
    public String[] getArchiveTimestamps() {
      return new String[0];
    }

    @Override
    public String[] getFileTimestamps() {
      return new String[0];
    }

    @Override
    public int getMaxMapAttempts() {
      return 0;
    }

    @Override
    public int getMaxReduceAttempts() {
      return 0;
    }

    @Override
    public void progress() {
    }
  }

}
