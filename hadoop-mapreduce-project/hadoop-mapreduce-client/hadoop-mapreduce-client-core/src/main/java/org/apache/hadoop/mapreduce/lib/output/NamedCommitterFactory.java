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
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * A factory which creates any named committer identified
 * in the option {@link PathOutputCommitterFactory#NAMED_COMMITTER_CLASS}.
 */
public final class NamedCommitterFactory extends
    PathOutputCommitterFactory {
  private static final Logger LOG =
      LoggerFactory.getLogger(NamedCommitterFactory.class);

  @SuppressWarnings("JavaReflectionMemberAccess")
  @Override
  public PathOutputCommitter createOutputCommitter(Path outputPath,
      TaskAttemptContext context) throws IOException {
    Class<? extends PathOutputCommitter> clazz = loadCommitterClass(context);
    LOG.debug("Using PathOutputCommitter implementation {}", clazz);
    try {
      Constructor<? extends PathOutputCommitter> ctor
          = clazz.getConstructor(Path.class, TaskAttemptContext.class);
      return ctor.newInstance(outputPath, context);
    } catch (NoSuchMethodException
        | InstantiationException
        | IllegalAccessException
        | InvocationTargetException e) {
      throw new IOException("Failed to create " + clazz
          + ":" + e, e);
    }
  }

  /**
   * Load the class named in {@link #NAMED_COMMITTER_CLASS}.
   * @param context job or task context
   * @return the committer class
   * @throws IOException if no committer was defined.
   */
  private Class<? extends PathOutputCommitter> loadCommitterClass(
      JobContext context) throws IOException {
    Preconditions.checkNotNull(context, "null context");
    Configuration conf = context.getConfiguration();
    String value = conf.get(NAMED_COMMITTER_CLASS, "");
    if (value.isEmpty()) {
      throw new IOException("No committer defined in " + NAMED_COMMITTER_CLASS);
    }
    return conf.getClass(NAMED_COMMITTER_CLASS,
        FileOutputCommitter.class, PathOutputCommitter.class);
  }
}
