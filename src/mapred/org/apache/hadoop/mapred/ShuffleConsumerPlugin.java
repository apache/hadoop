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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import java.io.IOException;
import org.apache.hadoop.mapred.Task.TaskReporter;
import org.apache.hadoop.fs.FileSystem;

/**
 * ShuffleConsumerPlugin for serving Reducers.  It may shuffle MOF files from
 * either the built-in provider (MapOutputServlet) or from a 3rd party ShuffleProviderPlugin.
 *
 */
@InterfaceAudience.LimitedPrivate("MapReduce")
@InterfaceStability.Unstable
public interface ShuffleConsumerPlugin {

  /**
   * initialize this instance after it was created by factory.
   */
  public void init(Context context) throws ClassNotFoundException, IOException;

  /**
   * fetch output of mappers from TaskTrackers
   * @return true iff success.  In case of failure an appropriate Throwable may be available thru getMergeThrowable() member
   */
  public boolean fetchOutputs() throws IOException;

  /**
   * @ret reference to a Throwable object (if merge throws an exception)
   */
  public Throwable getMergeThrowable();

  /**
   * Create a RawKeyValueIterator from copied map outputs.
   *
   * The iterator returned must satisfy the following constraints:
   *   1. Fewer than io.sort.factor files may be sources
   *   2. No more than maxInMemReduce bytes of map outputs may be resident
   *      in memory when the reduce begins
   *
   * If we must perform an intermediate merge to satisfy (1), then we can
   * keep the excluded outputs from (2) in memory and include them in the
   * first merge pass. If not, then said outputs must be written to disk
   * first.
   */
  public RawKeyValueIterator createKVIterator(JobConf job, FileSystem fs, Reporter reporter) throws IOException;

  /**
   * close and clean any resource associated with this object.
   */
  public void close();

  @InterfaceAudience.LimitedPrivate("MapReduce")
  @InterfaceStability.Unstable
  public static class Context {
    private final ReduceTask reduceTask;
    private final TaskUmbilicalProtocol umbilical;
    private final JobConf conf;
    private final TaskReporter reporter;

    public Context(ReduceTask reduceTask, TaskUmbilicalProtocol umbilical, JobConf conf, TaskReporter reporter){
      this.reduceTask = reduceTask;
      this.umbilical = umbilical;
      this.conf = conf;
      this.reporter = reporter;
    }

    public ReduceTask getReduceTask() {
      return reduceTask;
    }
    public JobConf getConf() {
      return conf;
    }
    public TaskUmbilicalProtocol getUmbilical() {
      return umbilical;
    }
    public TaskReporter getReporter() {
      return reporter;
    }
  }
}
