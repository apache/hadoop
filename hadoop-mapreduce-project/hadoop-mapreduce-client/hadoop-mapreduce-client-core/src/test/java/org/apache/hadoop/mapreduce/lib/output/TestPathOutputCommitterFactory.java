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
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

import static org.apache.hadoop.mapreduce.lib.output.PathOutputCommitterFactory.*;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test the committer factory logic, looking at the override
 * and fallback behavior.
 */
@SuppressWarnings("unchecked")
public class TestPathOutputCommitterFactory extends Assert {

  private static final String HTTP_COMMITTER_FACTORY = String.format(
      COMMITTER_FACTORY_SCHEME_PATTERN, "http");

  private static final Path HTTP_PATH = new Path("http://hadoop.apache.org/");
  private static final Path HDFS_PATH = new Path("hdfs://localhost:8081/");

  private TaskAttemptID taskAttemptID =
      new TaskAttemptID("local", 0, TaskType.MAP, 1, 2);

  /**
   * Set a factory for a schema, verify it works.
   * @throws Throwable failure
   */
  @Test
  public void testCommitterFactoryForSchema() throws Throwable {
    createCommitterFactory(SimpleCommitterFactory.class,
        HTTP_PATH,
        newBondedConfiguration());
  }

  /**
   * A schema factory only affects that filesystem.
   * @throws Throwable failure
   */
  @Test
  public void testCommitterFactoryFallbackDefault() throws Throwable {
    createCommitterFactory(FileOutputCommitterFactory.class,
        HDFS_PATH,
        newBondedConfiguration());
  }

  /**
   * A schema factory only affects that filesystem; test through
   * {@link PathOutputCommitterFactory#createCommitter(Path, TaskAttemptContext)}.
   * @throws Throwable failure
   */
  @Test
  public void testCommitterFallbackDefault() throws Throwable {
    createCommitter(FileOutputCommitter.class,
        HDFS_PATH,
        taskAttempt(newBondedConfiguration()));
  }

  /**
   * Verify that you can override any schema with an explicit name.
   */
  @Test
  public void testCommitterFactoryOverride() throws Throwable {
    Configuration conf = newBondedConfiguration();
    // set up for the schema factory
    // and then set a global one which overrides the others.
    conf.set(COMMITTER_FACTORY_CLASS, OtherFactory.class.getName());
    createCommitterFactory(OtherFactory.class, HDFS_PATH, conf);
    createCommitterFactory(OtherFactory.class, HTTP_PATH, conf);
  }

  /**
   * Verify that if the factory class option is "", schema factory
   * resolution still works.
   */
  @Test
  public void testCommitterFactoryEmptyOption() throws Throwable {
    Configuration conf = newBondedConfiguration();
    // set up for the schema factory
    // and then set a global one which overrides the others.
    conf.set(COMMITTER_FACTORY_CLASS, "");
    createCommitterFactory(SimpleCommitterFactory.class, HTTP_PATH, conf);

    // and HDFS, with no schema, falls back to the default
    createCommitterFactory(FileOutputCommitterFactory.class, HDFS_PATH, conf);
  }

  /**
   * Verify that if the committer factory class is unknown, you cannot
   * create committers.
   */
  @Test
  public void testCommitterFactoryUnknown() throws Throwable {
    Configuration conf = new Configuration();
    // set the factory to an unknown class
    conf.set(COMMITTER_FACTORY_CLASS, "unknown");
    intercept(RuntimeException.class,
        () -> getCommitterFactory(HDFS_PATH, conf));
  }

  /**
   * Verify that if the committer output path is null, you get back
   * a FileOutputCommitter with null output & work paths.
   */
  @Test
  public void testCommitterNullOutputPath() throws Throwable {
    // bind http to schema
    Configuration conf = newBondedConfiguration();
    // then ask committers for a null path
    FileOutputCommitter committer = createCommitter(
        FileOutputCommitterFactory.class,
        FileOutputCommitter.class,
        null, conf);
    assertNull(committer.getOutputPath());
    assertNull(committer.getWorkPath());
  }

  /**
   * Verify that if you explicitly name a committer, that takes priority
   * over any filesystem committer.
   */
  @Test
  public void testNamedCommitterFactory() throws Throwable {
    Configuration conf = new Configuration();
    // set up for the schema factory
    conf.set(COMMITTER_FACTORY_CLASS, NAMED_COMMITTER_FACTORY);
    conf.set(NAMED_COMMITTER_CLASS, SimpleCommitter.class.getName());
    SimpleCommitter sc = createCommitter(
        NamedCommitterFactory.class,
        SimpleCommitter.class, HDFS_PATH, conf);
    assertEquals("Wrong output path from " + sc,
        HDFS_PATH,
        sc.getOutputPath());
  }

  /**
   * Verify that if you explicitly name a committer and there's no
   * path, the committer is picked up.
   */
  @Test
  public void testNamedCommitterFactoryNullPath() throws Throwable {
    Configuration conf = new Configuration();
    // set up for the schema factory
    conf.set(COMMITTER_FACTORY_CLASS, NAMED_COMMITTER_FACTORY);
    conf.set(NAMED_COMMITTER_CLASS, SimpleCommitter.class.getName());
    SimpleCommitter sc = createCommitter(
        NamedCommitterFactory.class,
        SimpleCommitter.class,
        null, conf);
    assertNull(sc.getOutputPath());
  }

  /**
   * Verify that if you explicitly name a committer and there's no
   * path, the committer is picked up.
   */
  @Test
  public void testNamedCommitterNullPath() throws Throwable {
    Configuration conf = new Configuration();
    // set up for the schema factory
    conf.set(COMMITTER_FACTORY_CLASS, NAMED_COMMITTER_FACTORY);
    conf.set(NAMED_COMMITTER_CLASS, SimpleCommitter.class.getName());

    SimpleCommitter sc = createCommitter(
        SimpleCommitter.class,
        null, taskAttempt(conf));
    assertNull(sc.getOutputPath());
  }

  /**
   * Create a factory then a committer, validating the type of both.
   * @param <T> type of factory
   * @param <U> type of committer
   * @param factoryClass expected factory class
   * @param committerClass expected committer class
   * @param path output path (may be null)
   * @param conf configuration
   * @return the committer
   * @throws IOException failure to create
   */
  private <T extends PathOutputCommitterFactory, U extends PathOutputCommitter>
      U createCommitter(Class<T> factoryClass,
      Class<U> committerClass,
      Path path,
      Configuration conf) throws IOException {
    T f = createCommitterFactory(factoryClass, path, conf);
    PathOutputCommitter committer = f.createOutputCommitter(path,
        taskAttempt(conf));
    assertEquals(" Wrong committer for path " + path + " from factory " + f,
        committerClass, committer.getClass());
    return (U) committer;
  }

  /**
   * Create a committer from a task context, via
   * {@link PathOutputCommitterFactory#createCommitter(Path, TaskAttemptContext)}.
   * @param <U> type of committer
   * @param committerClass expected committer class
   * @param path output path (may be null)
   * @param context task attempt context
   * @return the committer
   * @throws IOException failure to create
   */
  private <U extends PathOutputCommitter> U createCommitter(
      Class<U> committerClass,
      Path path,
      TaskAttemptContext context) throws IOException {
    PathOutputCommitter committer = PathOutputCommitterFactory
        .createCommitter(path, context);
    assertEquals(" Wrong committer for path " + path,
        committerClass, committer.getClass());
    return (U) committer;
  }

  /**
   * Create a factory then a committer, validating its type.
   * @param factoryClass expected factory class
   * @param path output path (may be null)
   * @param conf configuration
   * @param <T> type of factory
   * @return the factory
   */
  private <T extends PathOutputCommitterFactory> T createCommitterFactory(
      Class<T> factoryClass,
      Path path,
      Configuration conf) {
    PathOutputCommitterFactory factory = getCommitterFactory(path, conf);
    assertEquals(" Wrong factory for path " + path,
        factoryClass, factory.getClass());
    return (T)factory;
  }

  /**
   * Create a new task attempt context.
   * @param conf config
   * @return a new context
   */
  private TaskAttemptContext taskAttempt(Configuration conf) {
    return new TaskAttemptContextImpl(conf, taskAttemptID);
  }

  /**
   * Verify that if you explicitly name a committer, that takes priority
   * over any filesystem committer.
   */
  @Test
  public void testFileOutputCommitterFactory() throws Throwable {
    Configuration conf = new Configuration();
    // set up for the schema factory
    conf.set(COMMITTER_FACTORY_CLASS, FILE_COMMITTER_FACTORY);
    conf.set(NAMED_COMMITTER_CLASS, SimpleCommitter.class.getName());
    getCommitterFactory(HDFS_PATH, conf);
    createCommitter(
        FileOutputCommitterFactory.class,
        FileOutputCommitter.class, null, conf);
  }

  /**
   * Follow the entire committer chain down and create a new committer from
   * the output format.
   * @throws Throwable on a failure.
   */
  @Test
  public void testFileOutputFormatBinding() throws Throwable {
    Configuration conf = newBondedConfiguration();
    conf.set(FileOutputFormat.OUTDIR, HTTP_PATH.toUri().toString());
    TextOutputFormat<String, String> off = new TextOutputFormat<>();
    SimpleCommitter committer = (SimpleCommitter)
        off.getOutputCommitter(taskAttempt(conf));
    assertEquals("Wrong output path from "+ committer,
        HTTP_PATH,
        committer.getOutputPath());
  }

  /**
   * Follow the entire committer chain down and create a new committer from
   * the output format.
   * @throws Throwable on a failure.
   */
  @Test
  public void testFileOutputFormatBindingNoPath() throws Throwable {
    Configuration conf = new Configuration();
    conf.unset(FileOutputFormat.OUTDIR);
    // set up for the schema factory
    conf.set(COMMITTER_FACTORY_CLASS, NAMED_COMMITTER_FACTORY);
    conf.set(NAMED_COMMITTER_CLASS, SimpleCommitter.class.getName());
    httpToSimpleFactory(conf);
    TextOutputFormat<String, String> off = new TextOutputFormat<>();
    SimpleCommitter committer = (SimpleCommitter)
        off.getOutputCommitter(taskAttempt(conf));
    assertNull("Output path from "+ committer,
        committer.getOutputPath());
  }

  /**
   * Bind the http schema CommitterFactory to {@link SimpleCommitterFactory}.
   * @param conf config to patch
   */
  private Configuration httpToSimpleFactory(Configuration conf) {
    conf.set(HTTP_COMMITTER_FACTORY, SimpleCommitterFactory.class.getName());
    return conf;
  }


  /**
   * Create a configuration with the http schema bonded to the simple factory.
   * @return a new, patched configuration
   */
  private Configuration newBondedConfiguration() {
    return httpToSimpleFactory(new Configuration());
  }

  /**
   * Extract the (mandatory) cause of an exception.
   * @param ex exception
   * @param clazz expected class
   * @return the cause, which will be of the expected type
   * @throws AssertionError if there is a problem
   */
  private <E extends Throwable> E verifyCauseClass(Throwable ex,
      Class<E> clazz) throws AssertionError {
    Throwable cause = ex.getCause();
    if (cause == null) {
      throw new AssertionError("No cause", ex);
    }
    if (!cause.getClass().equals(clazz)) {
      throw new AssertionError("Wrong cause class", cause);
    }
    return (E)cause;
  }

  @Test
  public void testBadCommitterFactory() throws Throwable {
    expectFactoryConstructionFailure(HTTP_COMMITTER_FACTORY);
  }

  @Test
  public void testBoundCommitterWithSchema() throws Throwable {
    // this verifies that a bound committer relays to the underlying committer
    Configuration conf = newBondedConfiguration();
    TestPathOutputCommitter.TaskContext tac
        = new TestPathOutputCommitter.TaskContext(conf);
    BindingPathOutputCommitter committer
        = new BindingPathOutputCommitter(HTTP_PATH, tac);
    intercept(IOException.class, "setupJob",
        () -> committer.setupJob(tac));
  }

  @Test
  public void testBoundCommitterWithDefault() throws Throwable {
    // this verifies that a bound committer relays to the underlying committer
    Configuration conf = newBondedConfiguration();
    TestPathOutputCommitter.TaskContext tac
        = new TestPathOutputCommitter.TaskContext(conf);
    BindingPathOutputCommitter committer
        = new BindingPathOutputCommitter(HDFS_PATH, tac);
    assertEquals(FileOutputCommitter.class,
        committer.getCommitter().getClass());
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
    RuntimeException ex = intercept(RuntimeException.class,
        () -> getCommitterFactory(HTTP_PATH, conf));
    verifyCauseClass(
        verifyCauseClass(ex, RuntimeException.class),
        ClassNotFoundException.class);
  }

  /**
   * A simple committer.
   */
  public static final class SimpleCommitter extends PathOutputCommitter {

    private final Path outputPath;

    public SimpleCommitter(Path outputPath,
        TaskAttemptContext context) throws IOException {
      super(outputPath, context);
      this.outputPath = outputPath;
    }

    @Override
    public Path getWorkPath() throws IOException {
      return null;
    }

    /**
     * Job setup throws an exception.
     * @param jobContext Context of the job
     * @throws IOException always
     */
    @Override
    public void setupJob(JobContext jobContext) throws IOException {
      throw new IOException("setupJob");
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
      return outputPath;
    }
  }

  /**
   * The simple committer factory.
   */
  private static class SimpleCommitterFactory
      extends PathOutputCommitterFactory {

    @Override
    public PathOutputCommitter createOutputCommitter(Path outputPath,
        TaskAttemptContext context) throws IOException {
      return new SimpleCommitter(outputPath, context);
    }

  }

  /**
   * Some other factory.
   */
  private static class OtherFactory extends PathOutputCommitterFactory {

    /**
     * {@inheritDoc}
     * @param outputPath output path. This may be null.
     * @param context context
     * @return
     * @throws IOException
     */
    @Override
    public PathOutputCommitter createOutputCommitter(Path outputPath,
        TaskAttemptContext context) throws IOException {
      return new SimpleCommitter(outputPath, context);
    }

  }

}
