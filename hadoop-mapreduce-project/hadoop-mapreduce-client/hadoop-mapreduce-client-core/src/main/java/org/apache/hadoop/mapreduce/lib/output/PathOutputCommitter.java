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

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * A committer which somehow commits data written to a working directory
 * to the final directory during the commit process. The reference
 * implementation of this is the {@link FileOutputCommitter}.
 *
 * There are two constructors, both of which do nothing but long and
 * validate their arguments.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class PathOutputCommitter extends OutputCommitter {
  private static final Logger LOG =
      LoggerFactory.getLogger(PathOutputCommitter.class);

  private final JobContext context;

  /**
   * Constructor for a task attempt.
   * Subclasses should provide a public constructor with this signature.
   * @param outputPath output path: may be null
   * @param context task context
   * @throws IOException IO problem
   */
  protected PathOutputCommitter(Path outputPath,
      TaskAttemptContext context) throws IOException {
    this.context = Preconditions.checkNotNull(context, "Null context");
    LOG.debug("Creating committer with output path {} and task context"
        + " {}", outputPath, context);
  }

  /**
   * Constructor for a job attempt.
   * Subclasses should provide a public constructor with this signature.
   * @param outputPath output path: may be null
   * @param context task context
   * @throws IOException IO problem
   */
  protected PathOutputCommitter(Path outputPath,
      JobContext context) throws IOException {
    this.context = Preconditions.checkNotNull(context, "Null context");
    LOG.debug("Creating committer with output path {} and job context"
        + " {}", outputPath, context);
  }

  /**
   * Get the directory that the task should write results into.
   * Warning: there's no guarantee that this work path is on the same
   * FS as the final output, or that it's visible across machines.
   * @return the work directory
   * @throws IOException IO problem
   */
  public abstract Path getWorkPath() throws IOException;

  @Override
  public String toString() {
    return "PathOutputCommitter{context=" + context + '}';
  }
}
