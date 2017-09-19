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

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.test.LambdaTestUtils;

import static org.apache.hadoop.mapreduce.lib.output.PathOutputCommitterFactory.*;

/**
 * Test the committer factory logic, using an FS scheme of "http".
 */
public class TestCommitterFactoryBinding extends Assert {

  private static final String COMMITTER_FACTORY_KEY = String.format(
      OUTPUTCOMMITTER_FACTORY_SCHEME_PATTERN, "http");

  private static final Path HTTP_PATH = new Path("http://hadoop.apache.org/");
  private static final Path HDFS_PATH = new Path("hdfs://localhost:8081/");

  private TaskAttemptID attemptID = new TaskAttemptID("local", 0, TaskType.MAP, 1, 2);

  /**
   * Set a factory for a schema, verify it works.
   * @throws Throwable failure
   */
  @Test
  public void testCommitterFactoryForSchema() throws Throwable {
    Configuration conf = new Configuration();
    bindSchemaFactory(conf);
    CommitterFactory factory = (CommitterFactory)
        getOutputCommitterFactory(HTTP_PATH, conf);
  }

  /**
   * A schema factory only affects that filesystem.
   * @throws Throwable failure
   */
  @Test
  public void testCommitterFactoryFallbackDefault() throws Throwable {
    Configuration conf = new Configuration();
    bindSchemaFactory(conf);
    PathOutputCommitterFactory factory = getOutputCommitterFactory(
        HDFS_PATH, conf);
    assertFalse("Wrong committer factory: " + factory,
        factory instanceof CommitterFactory);
  }

  /**
   * Verify that you can set the default entry and it works for all but
   * any schema which you've explicitly set up.
   * @throws Throwable failure
   */
  @Test
  public void testCommitterFactoryFallbackOverride() throws Throwable {
    Configuration conf = new Configuration();
    bindSchemaFactory(conf);
    conf.set(OUTPUTCOMMITTER_FACTORY_CLASS, CommitterFactory2.class.getName());
    CommitterFactory2 factory = (CommitterFactory2)
        getOutputCommitterFactory(HDFS_PATH, conf);
    CommitterFactory f2 = (CommitterFactory)
        getOutputCommitterFactory(HTTP_PATH, conf);
  }

  @Test
  public void testFileOutputFormatBinding() throws Throwable {
    Configuration conf = new Configuration();
    conf.set(OUTPUTCOMMITTER_FACTORY_CLASS, CommitterFactory2.class.getName());
    conf.set(FileOutputFormat.OUTDIR, HTTP_PATH.toUri().toString());
    bindSchemaFactory(conf);
    TaskAttemptContextImpl context =
        new TaskAttemptContextImpl(conf, attemptID);
    TextOutputFormat<String, String> off = new TextOutputFormat<>();
    OutputCommitter committer = off.getOutputCommitter(context);
    assertTrue("Wrong committer : " + committer,
        committer instanceof SimpleCommitter);

  }

  /**
   * Bind the schema to {@code CommitterFactory}.
   * @param conf config to patch
   */
  protected void bindSchemaFactory(Configuration conf) {
    conf.set(COMMITTER_FACTORY_KEY,
        CommitterFactory.class.getName());
  }

  private Throwable verifyCauseClass(Throwable ex,
      Class<? extends Throwable> clazz) throws Throwable {
    Throwable cause = ex.getCause();
    if (cause == null) {
      throw ex;
    }
    if (!cause.getClass().equals(clazz)) {
      throw cause;
    }
    return cause;
  }

  @Test
  public void testBadCommitterFactory() throws Throwable {
    expectFactoryConstructionFailure(COMMITTER_FACTORY_KEY);
  }

  @Test
  public void testBadCommitterFactoryScheme() throws Throwable {
    expectFactoryConstructionFailure(COMMITTER_FACTORY_KEY);
  }

  /**
   * Set the specific key to a string which is not a factory class; expect
   * a failure.
   * @param key key to set
   * @throws Throwable on a failure
   */
  @SuppressWarnings("ThrowableNotThrown")
  protected void expectFactoryConstructionFailure(String key) throws Throwable {
    Configuration conf = new Configuration();
    conf.set(key, "Not a factory");
    RuntimeException ex = LambdaTestUtils.intercept(
        RuntimeException.class,
        () -> getOutputCommitterFactory(HTTP_PATH, conf));
    verifyCauseClass(
        verifyCauseClass(ex, RuntimeException.class),
        ClassNotFoundException.class);
  }

  private static class SimpleCommitter extends PathOutputCommitter {

    SimpleCommitter(Path outputPath,
        TaskAttemptContext context) throws IOException {
      super(outputPath, context);
    }

    SimpleCommitter(Path outputPath,
        JobContext context) throws IOException {
      super(outputPath, context);
    }

    @Override
    public Path getWorkPath() throws IOException {
      return null;
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
  }

  private static class CommitterFactory extends PathOutputCommitterFactory {

    @Override
    public PathOutputCommitter createOutputCommitter(Path outputPath,
        TaskAttemptContext context) throws IOException {
      return new SimpleCommitter(outputPath, context);
    }

    @Override
    public PathOutputCommitter createOutputCommitter(Path outputPath,
        JobContext context) throws IOException {
      return new SimpleCommitter(outputPath, context);
    }
  }

  private static class CommitterFactory2 extends PathOutputCommitterFactory {

    @Override
    public PathOutputCommitter createOutputCommitter(Path outputPath,
        TaskAttemptContext context) throws IOException {
      return new SimpleCommitter(outputPath, context);
    }

    @Override
    public PathOutputCommitter createOutputCommitter(Path outputPath,
        JobContext context) throws IOException {
      return new SimpleCommitter(outputPath, context);
    }
  }

}
