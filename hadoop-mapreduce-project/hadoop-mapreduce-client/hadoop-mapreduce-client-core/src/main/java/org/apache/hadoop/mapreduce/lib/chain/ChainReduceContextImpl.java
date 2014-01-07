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
package org.apache.hadoop.mapreduce.lib.chain;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configuration.IntegerRanges;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.security.Credentials;

/**
 * A simple wrapper class that delegates most of its functionality to the
 * underlying context, but overrides the methods to do with record writer and
 * configuration
 */
class ChainReduceContextImpl<KEYIN, VALUEIN, KEYOUT, VALUEOUT> implements
    ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

  private final ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> base;
  private final RecordWriter<KEYOUT, VALUEOUT> rw;
  private final Configuration conf;

  public ChainReduceContextImpl(
      ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> base,
      RecordWriter<KEYOUT, VALUEOUT> output, Configuration conf) {
    this.base = base;
    this.rw = output;
    this.conf = conf;
  }

  @Override
  public Iterable<VALUEIN> getValues() throws IOException, InterruptedException {
    return base.getValues();
  }

  @Override
  public boolean nextKey() throws IOException, InterruptedException {
    return base.nextKey();
  }

  @Override
  public Counter getCounter(Enum<?> counterName) {
    return base.getCounter(counterName);
  }

  @Override
  public Counter getCounter(String groupName, String counterName) {
    return base.getCounter(groupName, counterName);
  }

  @Override
  public KEYIN getCurrentKey() throws IOException, InterruptedException {
    return base.getCurrentKey();
  }

  @Override
  public VALUEIN getCurrentValue() throws IOException, InterruptedException {
    return base.getCurrentValue();
  }

  @Override
  public OutputCommitter getOutputCommitter() {
    return base.getOutputCommitter();
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    return base.nextKeyValue();
  }

  @Override
  public void write(KEYOUT key, VALUEOUT value) throws IOException,
      InterruptedException {
    rw.write(key, value);
  }

  @Override
  public String getStatus() {
    return base.getStatus();
  }

  @Override
  public TaskAttemptID getTaskAttemptID() {
    return base.getTaskAttemptID();
  }

  @Override
  public void setStatus(String msg) {
    base.setStatus(msg);
  }

  @Override
  public Path[] getArchiveClassPaths() {
    return base.getArchiveClassPaths();
  }

  @Override
  public String[] getArchiveTimestamps() {
    return base.getArchiveTimestamps();
  }

  @Override
  public URI[] getCacheArchives() throws IOException {
    return base.getCacheArchives();
  }

  @Override
  public URI[] getCacheFiles() throws IOException {
    return base.getCacheFiles();
  }

  @Override
  public Class<? extends Reducer<?, ?, ?, ?>> getCombinerClass()
      throws ClassNotFoundException {
    return base.getCombinerClass();
  }

  @Override
  public Configuration getConfiguration() {
    return conf;
  }

  @Override
  public Path[] getFileClassPaths() {
    return base.getFileClassPaths();
  }

  @Override
  public String[] getFileTimestamps() {
    return base.getFileTimestamps();
  }

  @Override
  public RawComparator<?> getCombinerKeyGroupingComparator() {
    return base.getCombinerKeyGroupingComparator();
  }

  @Override
  public RawComparator<?> getGroupingComparator() {
    return base.getGroupingComparator();
  }

  @Override
  public Class<? extends InputFormat<?, ?>> getInputFormatClass()
      throws ClassNotFoundException {
    return base.getInputFormatClass();
  }

  @Override
  public String getJar() {
    return base.getJar();
  }

  @Override
  public JobID getJobID() {
    return base.getJobID();
  }

  @Override
  public String getJobName() {
    return base.getJobName();
  }

  @Override
  public boolean getJobSetupCleanupNeeded() {
    return base.getJobSetupCleanupNeeded();
  }

  @Override
  public boolean getTaskCleanupNeeded() {
    return base.getTaskCleanupNeeded();
  }

  @Override
  public Path[] getLocalCacheArchives() throws IOException {
    return base.getLocalCacheArchives();
  }

  @Override
  public Path[] getLocalCacheFiles() throws IOException {
    return base.getLocalCacheFiles();
  }

  @Override
  public Class<?> getMapOutputKeyClass() {
    return base.getMapOutputKeyClass();
  }

  @Override
  public Class<?> getMapOutputValueClass() {
    return base.getMapOutputValueClass();
  }

  @Override
  public Class<? extends Mapper<?, ?, ?, ?>> getMapperClass()
      throws ClassNotFoundException {
    return base.getMapperClass();
  }

  @Override
  public int getMaxMapAttempts() {
    return base.getMaxMapAttempts();
  }

  @Override
  public int getMaxReduceAttempts() {
    return base.getMaxMapAttempts();
  }

  @Override
  public int getNumReduceTasks() {
    return base.getNumReduceTasks();
  }

  @Override
  public Class<? extends OutputFormat<?, ?>> getOutputFormatClass()
      throws ClassNotFoundException {
    return base.getOutputFormatClass();
  }

  @Override
  public Class<?> getOutputKeyClass() {
    return base.getOutputKeyClass();
  }

  @Override
  public Class<?> getOutputValueClass() {
    return base.getOutputValueClass();
  }

  @Override
  public Class<? extends Partitioner<?, ?>> getPartitionerClass()
      throws ClassNotFoundException {
    return base.getPartitionerClass();
  }

  @Override
  public boolean getProfileEnabled() {
    return base.getProfileEnabled();
  }

  @Override
  public String getProfileParams() {
    return base.getProfileParams();
  }

  @Override
  public IntegerRanges getProfileTaskRange(boolean isMap) {
    return base.getProfileTaskRange(isMap);
  }

  @Override
  public Class<? extends Reducer<?, ?, ?, ?>> getReducerClass()
      throws ClassNotFoundException {
    return base.getReducerClass();
  }

  @Override
  public RawComparator<?> getSortComparator() {
    return base.getSortComparator();
  }

  @Override
  public boolean getSymlink() {
    return base.getSymlink();
  }

  @Override
  public String getUser() {
    return base.getUser();
  }

  @Override
  public Path getWorkingDirectory() throws IOException {
    return base.getWorkingDirectory();
  }

  @Override
  public void progress() {
    base.progress();
  }

  @Override
  public Credentials getCredentials() {
    return base.getCredentials();
  }
  
  @Override
  public float getProgress() {
    return base.getProgress();
  }
}
