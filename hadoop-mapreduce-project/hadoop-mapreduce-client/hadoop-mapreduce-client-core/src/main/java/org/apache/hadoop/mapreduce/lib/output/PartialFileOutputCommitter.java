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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.task.annotation.Checkpointable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/** An {@link OutputCommitter} that commits files specified
 * in job output directory i.e. ${mapreduce.output.fileoutputformat.outputdir}.
 **/
@Checkpointable
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class PartialFileOutputCommitter
    extends FileOutputCommitter implements PartialOutputCommitter {

  private static final Logger LOG =
      LoggerFactory.getLogger(PartialFileOutputCommitter.class);


  public PartialFileOutputCommitter(Path outputPath,
                             TaskAttemptContext context) throws IOException {
    super(outputPath, context);
  }

  public PartialFileOutputCommitter(Path outputPath,
                             JobContext context) throws IOException {
    super(outputPath, context);
  }

  @Override
  public Path getCommittedTaskPath(int appAttemptId, TaskAttemptContext context) {
    return new Path(getJobAttemptPath(appAttemptId),
        String.valueOf(context.getTaskAttemptID()));
  }

  @VisibleForTesting
  FileSystem fsFor(Path p, Configuration conf) throws IOException {
    return p.getFileSystem(conf);
  }

  @Override
  public void cleanUpPartialOutputForTask(TaskAttemptContext context)
      throws IOException {

    // we double check this is never invoked from a non-preemptable subclass.
    // This should never happen, since the invoking codes is checking it too,
    // but it is safer to double check. Errors handling this would produce
    // inconsistent output.

    if (!this.getClass().isAnnotationPresent(Checkpointable.class)) {
      throw new IllegalStateException("Invoking cleanUpPartialOutputForTask() " +
          "from non @Preemptable class");
    }
    FileSystem fs =
      fsFor(getTaskAttemptPath(context), context.getConfiguration());

    LOG.info("cleanUpPartialOutputForTask: removing everything belonging to " +
        context.getTaskAttemptID().getTaskID() + " in: " +
        getCommittedTaskPath(context).getParent());

    final TaskAttemptID taid = context.getTaskAttemptID();
    final TaskID tid = taid.getTaskID();
    Path pCommit = getCommittedTaskPath(context).getParent();
    // remove any committed output
    for (int i = 0; i < taid.getId(); ++i) {
      TaskAttemptID oldId = new TaskAttemptID(tid, i);
      Path pTask = new Path(pCommit, oldId.toString());
      if (!fs.delete(pTask, true) && fs.exists(pTask)) {
        throw new IOException("Failed to delete " + pTask);
      }
    }
  }

}
