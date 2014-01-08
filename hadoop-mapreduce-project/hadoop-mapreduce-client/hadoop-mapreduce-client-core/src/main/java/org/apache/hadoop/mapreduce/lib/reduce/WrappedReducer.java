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

package org.apache.hadoop.mapreduce.lib.reduce;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
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
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.security.Credentials;

/**
 * A {@link Reducer} which wraps a given one to allow for custom 
 * {@link Reducer.Context} implementations.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class WrappedReducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> 
    extends Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

  /**
   * A a wrapped {@link Reducer.Context} for custom implementations.
   * @param reduceContext <code>ReduceContext</code> to be wrapped
   * @return a wrapped <code>Reducer.Context</code> for custom implementations
   */
  public Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context 
  getReducerContext(ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> reduceContext) {
    return new Context(reduceContext);
  }
  
  @InterfaceStability.Evolving
  public class Context 
      extends Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context {

    protected ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> reduceContext;

    public Context(ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> reduceContext)
    {
      this.reduceContext = reduceContext; 
    }

    @Override
    public KEYIN getCurrentKey() throws IOException, InterruptedException {
      return reduceContext.getCurrentKey();
    }

    @Override
    public VALUEIN getCurrentValue() throws IOException, InterruptedException {
      return reduceContext.getCurrentValue();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      return reduceContext.nextKeyValue();
    }

    @Override
    public Counter getCounter(Enum counterName) {
      return reduceContext.getCounter(counterName);
    }

    @Override
    public Counter getCounter(String groupName, String counterName) {
      return reduceContext.getCounter(groupName, counterName);
    }

    @Override
    public OutputCommitter getOutputCommitter() {
      return reduceContext.getOutputCommitter();
    }

    @Override
    public void write(KEYOUT key, VALUEOUT value) throws IOException,
        InterruptedException {
      reduceContext.write(key, value);
    }

    @Override
    public String getStatus() {
      return reduceContext.getStatus();
    }

    @Override
    public TaskAttemptID getTaskAttemptID() {
      return reduceContext.getTaskAttemptID();
    }

    @Override
    public void setStatus(String msg) {
      reduceContext.setStatus(msg);
    }

    @Override
    public Path[] getArchiveClassPaths() {
      return reduceContext.getArchiveClassPaths();
    }

    @Override
    public String[] getArchiveTimestamps() {
      return reduceContext.getArchiveTimestamps();
    }

    @Override
    public URI[] getCacheArchives() throws IOException {
      return reduceContext.getCacheArchives();
    }

    @Override
    public URI[] getCacheFiles() throws IOException {
      return reduceContext.getCacheFiles();
    }

    @Override
    public Class<? extends Reducer<?, ?, ?, ?>> getCombinerClass()
        throws ClassNotFoundException {
      return reduceContext.getCombinerClass();
    }

    @Override
    public Configuration getConfiguration() {
      return reduceContext.getConfiguration();
    }

    @Override
    public Path[] getFileClassPaths() {
      return reduceContext.getFileClassPaths();
    }

    @Override
    public String[] getFileTimestamps() {
      return reduceContext.getFileTimestamps();
    }

    @Override
    public RawComparator<?> getCombinerKeyGroupingComparator() {
      return reduceContext.getCombinerKeyGroupingComparator();
    }

    @Override
    public RawComparator<?> getGroupingComparator() {
      return reduceContext.getGroupingComparator();
    }

    @Override
    public Class<? extends InputFormat<?, ?>> getInputFormatClass()
        throws ClassNotFoundException {
      return reduceContext.getInputFormatClass();
    }

    @Override
    public String getJar() {
      return reduceContext.getJar();
    }

    @Override
    public JobID getJobID() {
      return reduceContext.getJobID();
    }

    @Override
    public String getJobName() {
      return reduceContext.getJobName();
    }

    @Override
    public boolean getJobSetupCleanupNeeded() {
      return reduceContext.getJobSetupCleanupNeeded();
    }

    @Override
    public boolean getTaskCleanupNeeded() {
      return reduceContext.getTaskCleanupNeeded();
    }

    @Override
    public Path[] getLocalCacheArchives() throws IOException {
      return reduceContext.getLocalCacheArchives();
    }

    @Override
    public Path[] getLocalCacheFiles() throws IOException {
      return reduceContext.getLocalCacheFiles();
    }

    @Override
    public Class<?> getMapOutputKeyClass() {
      return reduceContext.getMapOutputKeyClass();
    }

    @Override
    public Class<?> getMapOutputValueClass() {
      return reduceContext.getMapOutputValueClass();
    }

    @Override
    public Class<? extends Mapper<?, ?, ?, ?>> getMapperClass()
        throws ClassNotFoundException {
      return reduceContext.getMapperClass();
    }

    @Override
    public int getMaxMapAttempts() {
      return reduceContext.getMaxMapAttempts();
    }

    @Override
    public int getMaxReduceAttempts() {
      return reduceContext.getMaxReduceAttempts();
    }

    @Override
    public int getNumReduceTasks() {
      return reduceContext.getNumReduceTasks();
    }

    @Override
    public Class<? extends OutputFormat<?, ?>> getOutputFormatClass()
        throws ClassNotFoundException {
      return reduceContext.getOutputFormatClass();
    }

    @Override
    public Class<?> getOutputKeyClass() {
      return reduceContext.getOutputKeyClass();
    }

    @Override
    public Class<?> getOutputValueClass() {
      return reduceContext.getOutputValueClass();
    }

    @Override
    public Class<? extends Partitioner<?, ?>> getPartitionerClass()
        throws ClassNotFoundException {
      return reduceContext.getPartitionerClass();
    }

    @Override
    public Class<? extends Reducer<?, ?, ?, ?>> getReducerClass()
        throws ClassNotFoundException {
      return reduceContext.getReducerClass();
    }

    @Override
    public RawComparator<?> getSortComparator() {
      return reduceContext.getSortComparator();
    }

    @Override
    public boolean getSymlink() {
      return reduceContext.getSymlink();
    }

    @Override
    public Path getWorkingDirectory() throws IOException {
      return reduceContext.getWorkingDirectory();
    }

    @Override
    public void progress() {
      reduceContext.progress();
    }

    @Override
    public Iterable<VALUEIN> getValues() throws IOException,
        InterruptedException {
      return reduceContext.getValues();
    }

    @Override
    public boolean nextKey() throws IOException, InterruptedException {
      return reduceContext.nextKey();
    }
    
    @Override
    public boolean getProfileEnabled() {
      return reduceContext.getProfileEnabled();
    }

    @Override
    public String getProfileParams() {
      return reduceContext.getProfileParams();
    }

    @Override
    public IntegerRanges getProfileTaskRange(boolean isMap) {
      return reduceContext.getProfileTaskRange(isMap);
    }

    @Override
    public String getUser() {
      return reduceContext.getUser();
    }

    @Override
    public Credentials getCredentials() {
      return reduceContext.getCredentials();
    }
    
    @Override
    public float getProgress() {
      return reduceContext.getProgress();
    }
  }
}
