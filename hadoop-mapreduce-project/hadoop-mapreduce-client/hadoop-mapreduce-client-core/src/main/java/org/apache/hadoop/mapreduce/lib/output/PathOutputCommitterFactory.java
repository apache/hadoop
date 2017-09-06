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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * A factory for committers implementing the {@link PathOutputCommitter}
 * methods, and so can be used from {@link FileOutputFormat}.
 * The base implementation returns {@link FileOutputCommitter} instances.
 */
public class PathOutputCommitterFactory extends Configured {
  private static final Logger LOG =
      LoggerFactory.getLogger(PathOutputCommitterFactory.class);

  /**
   * Name of the configuration option used to configure the
   * output committer factory to use unless there is a specific
   * one for a schema
   */
  public static final String OUTPUTCOMMITTER_FACTORY_CLASS =
      "mapreduce.pathoutputcommitter.factory.class";

  /**
   * Scheme prefix for per-filesystem scheme committers.
   */
  public static final String OUTPUTCOMMITTER_FACTORY_SCHEME =
      "mapreduce.pathoutputcommitter.factory.scheme";

  /**
   * String format pattern for per-filesystem scheme committers.
   */
  public static final String OUTPUTCOMMITTER_FACTORY_SCHEME_PATTERN =
      OUTPUTCOMMITTER_FACTORY_SCHEME + ".%s";

  /**
   * Default committer factory name: {@value}.
   */
  public static final String OUTPUTCOMMITTER_FACTORY_DEFAULT =
      "org.apache.hadoop.mapreduce.lib.output.PathOutputCommitterFactory";

  /**
   * Create an output committer for a task attempt.
   * @param outputPath output path. This may be null.
   * @param context context
   * @return a new committer
   * @throws IOException problems instantiating the committer
   */
  public PathOutputCommitter createOutputCommitter(Path outputPath,
      TaskAttemptContext context) throws IOException {
    return createDefaultCommitter(outputPath, context);
  }

  /**
   * Create a path output committer for a job.
   * @param outputPath the job's output path, or null if you want the output
   * committer to act as a no-op.
   * @param context the task's context
   * @return the committer to use
   * @throws IOException problems instantiating the committer
   */
  public PathOutputCommitter createOutputCommitter(Path outputPath,
      JobContext context) throws IOException {
    return createDefaultCommitter(outputPath, context);
  }

  /**
   * Create an instance of the default committer, a {@link FileOutputCommitter}
   * for a job. This is made available for subclasses to use if they ever
   * need to create the default committer.
   * @param outputPath the job's output path, or null if you want the output
   * committer to act as a no-op.
   * @param context the job context
   * @return the committer to use
   * @throws IOException problems instantiating the committer
   */
  protected final PathOutputCommitter createDefaultCommitter(Path outputPath,
      JobContext context) throws IOException {
    LOG.debug("Creating FileOutputCommitter for path {} and context {}",
        outputPath, context);
    return new FileOutputCommitter(outputPath, context);
  }

  /**
   * Create an instance of the default committer, a {@link FileOutputCommitter}
   * for a task. This is made available for subclasses to use if they ever
   * need to create the default committer.
   * @param outputPath the job's output path, or null if you want the output
   * committer to act as a no-op.
   * @param context the task's context
   * @return the committer to use
   * @throws IOException problems instantiating the committer
   */
  protected final PathOutputCommitter createDefaultCommitter(Path outputPath,
      TaskAttemptContext context) throws IOException {
    LOG.debug("Creating FileOutputCommitter for path {} and context {}",
        outputPath, context);
    return new FileOutputCommitter(outputPath, context);
  }

  /**
   * Get the committer factory for a configuration.
   * @param outputPath the job's output path. If null, it means that the
   * schema is unknown and a per-schema factory cannot be determined.
   * @param conf configuration
   * @return an instantiated committer factory
   */
  public static PathOutputCommitterFactory getOutputCommitterFactory(
      Path outputPath,
      Configuration conf) {
    String keyName = OUTPUTCOMMITTER_FACTORY_CLASS;
    if (outputPath != null) {
      String scheme = outputPath.toUri().getScheme();
      String schemeKey = String.format(OUTPUTCOMMITTER_FACTORY_SCHEME_PATTERN,
          scheme);

      String factoryClass = conf.getTrimmed(schemeKey);
      if (factoryClass != null) {
        LOG.debug("Using scheme-specific committer factory key {}: {}",
            schemeKey, factoryClass);
        keyName = schemeKey;
      }
    } else {
      LOG.warn("Unknown commit destination; cannot choose scheme committer");
    }
    Class<? extends PathOutputCommitterFactory> factory =
        conf.getClass(keyName,
            PathOutputCommitterFactory.class,
            PathOutputCommitterFactory.class);
    LOG.debug("Using OutputCommitter factory class {}", factory);
    return ReflectionUtils.newInstance(factory, conf);
  }
}
