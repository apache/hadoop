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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
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
 *
 * Algorithm:
 * <ol>
 *   <ul>If an explicit committer factory is named, it is used.</ul>
 *   <ul>The output path is examined.
 *   If is non null and there is an explicit schema for that filesystem,
 *   its factory is instantiated.</ul>
 *   <ul>Otherwise, an instance of {@link FileOutputCommitter} is
 *   created.</ul>
 * </ol>
 *
 * In {@link FileOutputFormat}, the created factory has its method
 * {@link #createOutputCommitter(Path, TaskAttemptContext)} with a task
 * attempt context and a possibly null path.
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class PathOutputCommitterFactory extends Configured {
  private static final Logger LOG =
      LoggerFactory.getLogger(PathOutputCommitterFactory.class);

  /**
   * Name of the configuration option used to configure the
   * output committer factory to use unless there is a specific
   * one for a schema.
   */
  public static final String COMMITTER_FACTORY_CLASS =
      "mapreduce.outputcommitter.factory.class";

  /**
   * Scheme prefix for per-filesystem scheme committers.
   */
  public static final String COMMITTER_FACTORY_SCHEME =
      "mapreduce.outputcommitter.factory.scheme";

  /**
   * String format pattern for per-filesystem scheme committers.
   */
  public static final String COMMITTER_FACTORY_SCHEME_PATTERN =
      COMMITTER_FACTORY_SCHEME + ".%s";


  /**
   * The {@link FileOutputCommitter} factory.
   */
  public static final String FILE_COMMITTER_FACTORY  =
      "org.apache.hadoop.mapreduce.lib.output.FileOutputCommitterFactory";

  /**
   * The {@link FileOutputCommitter} factory.
   */
  public static final String NAMED_COMMITTER_FACTORY  =
      "org.apache.hadoop.mapreduce.lib.output.NamedCommitterFactory";

  /**
   * The named output committer.
   * Creates any committer listed in
   */
  public static final String NAMED_COMMITTER_CLASS =
      "mapreduce.outputcommitter.named.classname";

  /**
   * Default committer factory name: {@value}.
   */
  public static final String COMMITTER_FACTORY_DEFAULT =
      FILE_COMMITTER_FACTORY;

  /**
   * Create an output committer for a task attempt.
   * @param outputPath output path. This may be null.
   * @param context context
   * @return a new committer
   * @throws IOException problems instantiating the committer
   */
  public PathOutputCommitter createOutputCommitter(
      Path outputPath,
      TaskAttemptContext context) throws IOException {
    return createFileOutputCommitter(outputPath, context);
  }

  /**
   * Create a path output committer for a job.
   * This operation is not used by the FileOutputFormat workflow, as committers
   * are initialized only with a job context
   * @param outputPath the job's output path, or null if no output path
   * has been defined.
   * @param context the task's context
   * @return the committer to use
   * @throws IOException problems instantiating the committer
   */
  @InterfaceAudience.Private
  public PathOutputCommitter createOutputCommitter(
      Path outputPath,
      JobContext context) throws IOException {
    return createFileOutputCommitter(outputPath, context);
  }

  /**
   * Create a {@link FileOutputCommitter} for a job.
   * @param outputPath the job's output path, or null if no output path
   * has been defined.
   * @param context the job context
   * @return the committer to use
   * @throws IOException problems instantiating the committer
   */
  protected final PathOutputCommitter createFileOutputCommitter(
      Path outputPath,
      JobContext context) throws IOException {
    LOG.debug("Creating FileOutputCommitter for path {} and context {}",
        outputPath, context);
    return new FileOutputCommitter(outputPath, context);
  }

  /**
   * Create an instance of the default committer, a {@link FileOutputCommitter}
   * for a task.
   * @param outputPath the task's output path, or or null if no output path
   * has been defined.
   * @param context the task's context
   * @return the committer to use
   * @throws IOException problems instantiating the committer
   */
  protected final PathOutputCommitter createFileOutputCommitter(
      Path outputPath,
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
  public static PathOutputCommitterFactory getCommitterFactory(
      Path outputPath,
      Configuration conf) {
    String key = COMMITTER_FACTORY_CLASS;
    if (conf.getTrimmed(key) == null && outputPath != null) {
      // there is no explicit factory and there's an output path
      String scheme = outputPath.toUri().getScheme();
      String schemeKey = String.format(COMMITTER_FACTORY_SCHEME_PATTERN,
          scheme);

      String factoryClass = conf.getTrimmed(schemeKey);
      if (factoryClass != null) {
        key = schemeKey;
      }
    } else {
      // no explicit factory. The default will be used
    }
    Class<? extends PathOutputCommitterFactory> factory =
        conf.getClass(key,
            FileOutputCommitterFactory.class,
            PathOutputCommitterFactory.class);
    LOG.debug("Using OutputCommitter factory class {} from key {}",
        factory, key);
    return ReflectionUtils.newInstance(factory, conf);
  }

}
