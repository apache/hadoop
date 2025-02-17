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

package org.apache.hadoop.fs.s3a.impl.streams;

import java.io.IOException;
import java.io.UncheckedIOException;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.VectoredIOContext;
import org.apache.hadoop.fs.s3a.prefetch.PrefetchingInputStreamFactory;
import org.apache.hadoop.test.AbstractHadoopTestBase;

import static org.apache.hadoop.fs.s3a.Constants.INPUT_STREAM_CUSTOM_FACTORY;
import static org.apache.hadoop.fs.s3a.Constants.INPUT_STREAM_TYPE;
import static org.apache.hadoop.fs.s3a.Constants.INPUT_STREAM_TYPE_CLASSIC;
import static org.apache.hadoop.fs.s3a.Constants.INPUT_STREAM_TYPE_CUSTOM;
import static org.apache.hadoop.fs.s3a.Constants.INPUT_STREAM_TYPE_DEFAULT;
import static org.apache.hadoop.fs.s3a.Constants.INPUT_STREAM_TYPE_PREFETCH;
import static org.apache.hadoop.fs.s3a.Constants.PREFETCH_ENABLED_KEY;
import static org.apache.hadoop.fs.s3a.impl.streams.StreamFactoryRequirements.Requirements.ExpectUnauditedGetRequests;
import static org.apache.hadoop.fs.s3a.impl.streams.StreamFactoryRequirements.Requirements.RequiresFuturePool;
import static org.apache.hadoop.fs.s3a.impl.streams.StreamIntegration.DEFAULT_STREAM_TYPE;
import static org.apache.hadoop.fs.s3a.impl.streams.StreamIntegration.E_EMPTY_CUSTOM_CLASSNAME;
import static org.apache.hadoop.fs.s3a.impl.streams.StreamIntegration.E_INVALID_STREAM_TYPE;
import static org.apache.hadoop.fs.s3a.impl.streams.StreamIntegration.factoryFromConfig;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit test for stream factory creation.
 * Verifies mapping of name to type, default handling,
 * legacy prefetch switch and failure handling.
 */
public class TestStreamFactories extends AbstractHadoopTestBase {

  /**
   * The empty string and "default" both map to the classic stream.
   */
  @Test
  public void testDefaultFactoryCreation() throws Throwable {
    load("", DEFAULT_STREAM_TYPE,
        ClassicObjectInputStreamFactory.class);
    load(INPUT_STREAM_TYPE_DEFAULT, DEFAULT_STREAM_TYPE,
        ClassicObjectInputStreamFactory.class);
  }

  /**
   * Classic factory.
   */
  @Test
  public void testClassicFactoryCreation() throws Throwable {
    final ClassicObjectInputStreamFactory f =
        load(INPUT_STREAM_TYPE_CLASSIC, DEFAULT_STREAM_TYPE,
            ClassicObjectInputStreamFactory.class);
    final StreamFactoryRequirements requirements = f.factoryRequirements();
    Assertions.assertThat(requirements.requiresFuturePool())
        .describedAs("requires future pool of %s", requirements)
        .isFalse();
    assertRequirement(requirements,
        ExpectUnauditedGetRequests,
        false);
  }

  /**
   * Asset taht the requirements matches the specified need.
   * @param requirements requirements instance
   * @param probe requirement to probe for.
   * @param shouldMatch is the requirement to be met to to fail?
   */
  private static void assertRequirement(
      final StreamFactoryRequirements requirements,
      final StreamFactoryRequirements.Requirements probe,
      final boolean shouldMatch) {
    Assertions.assertThat(requirements.requires(probe))
        .describedAs("%s of %s", probe, requirements)
        .isEqualTo(shouldMatch);
  }

  /**
   * Prefetch factory.
   */
  @Test
  public void testPrefetchFactoryCreation() throws Throwable {
    // load from config option
    final PrefetchingInputStreamFactory f = load(INPUT_STREAM_TYPE_PREFETCH,
        InputStreamType.Prefetch,
        PrefetchingInputStreamFactory.class);
    final StreamFactoryRequirements requirements = f.factoryRequirements();
    Assertions.assertThat(requirements.requiresFuturePool())
        .describedAs("requires future pool of %s", requirements)
        .isTrue();
    assertRequirement(requirements,
        ExpectUnauditedGetRequests,
        false);
    assertRequirement(requirements,
        RequiresFuturePool,
        true);
  }

  /**
   * Prefetch factory via the prefect enabled flag.
   * This is returned before any attempt is made to instantiate
   * the stream type option.
   */

  @Test
  public void testPrefetchEnabledFlag() throws Throwable {

    // request an analytics stream
    final Configuration conf = configWithStream("undefined");
    // but then set the prefetch key
    conf.setBoolean(PREFETCH_ENABLED_KEY, true);
    assertFactorySatisfies(factoryFromConfig(conf),
        INPUT_STREAM_TYPE_PREFETCH,
        InputStreamType.Prefetch,
        PrefetchingInputStreamFactory.class);
  }

  @Test
  public void testRequirementFlagsNoElements() throws Throwable {
    VectoredIOContext vertex = new VectoredIOContext();

    // no elements
    final StreamFactoryRequirements r1 =
        new StreamFactoryRequirements(1, 2, vertex);
    assertRequirement(r1, ExpectUnauditedGetRequests, false);
    assertRequirement(r1, RequiresFuturePool, false);
    Assertions.assertThat(r1.requiresFuturePool())
        .describedAs("requiresFuturePool() %s", r1)
        .isFalse();
    Assertions.assertThat(r1)
        .describedAs("%s", r1)
        .matches(r -> !r.requiresFuturePool(), "requiresFuturePool")
        .satisfies(r ->
            Assertions.assertThat(r.sharedThreads()).isEqualTo(1))
        .satisfies(r ->
            Assertions.assertThat(r.streamThreads()).isEqualTo(2));
  }

  @Test
  public void testRequirementFlagsFutures() throws Throwable {
    VectoredIOContext vertex = new VectoredIOContext();

    final StreamFactoryRequirements r1 =
        new StreamFactoryRequirements(1, 2, vertex, RequiresFuturePool);
    assertRequirement(r1, ExpectUnauditedGetRequests, false);
    assertRequirement(r1, RequiresFuturePool, true);
    Assertions.assertThat(r1.requiresFuturePool())
        .describedAs("requiresFuturePool() %s", r1)
        .isTrue();
  }

  @Test
  public void testRequirementFlagsUnaudited() throws Throwable {
    VectoredIOContext vertex = new VectoredIOContext();

    final StreamFactoryRequirements r1 =
        new StreamFactoryRequirements(1, 2, vertex, ExpectUnauditedGetRequests);
    assertRequirement(r1, ExpectUnauditedGetRequests, true);
    assertRequirement(r1, RequiresFuturePool, false);
  }


  /**
   * Create a factory, assert that it satisfies the requirements.
   * @param name name: only used for assertion messages.
   * @param type expected stream type.
   * @param clazz expected class.
   * @param <T> class to expect
   */
  private static <T extends ObjectInputStreamFactory> T load(
      String name,
      InputStreamType type,
      Class<T> clazz) throws IOException {

    final ObjectInputStreamFactory factory = factory(name);
    assertFactorySatisfies(factory, name, type, clazz);
    factory.init(new Configuration(false));
    factory.bind(new FactoryBindingParameters(new Callbacks()));
    return (T)factory;
  }

  /**
   * Assert that a factory satisfies the requirements.
   * @param factory factory
   * @param name name: only used for assertion messages.
   * @param type expected stream type.
   * @param clazz expected class.
   * @param <T> class to expect
   */
  private static <T extends ObjectInputStreamFactory> void assertFactorySatisfies(
      final ObjectInputStreamFactory factory,
      final String name,
      final InputStreamType type,
      final Class<T> clazz) {
    assertThat(factory)
        .describedAs("Factory for stream %s", name)
        .isInstanceOf(clazz)
        .satisfies(f ->
            assertThat(factory.streamType()).isEqualTo(type));
  }

  /**
   * When an unknown stream type is passed in, it is rejected.
   */
  @Test
  public void testUnknownStreamType() throws Throwable {
    final String name = "unknown";
    intercept(IllegalArgumentException.class, E_INVALID_STREAM_TYPE,
        () -> factory(name));
  }

  /**
   * Create a factory, using the given name as the configuration option.
   * @param name stream name.
   * @return the factory
   */
  private static ObjectInputStreamFactory factory(final String name) {
    return factoryFromConfig(configWithStream(name));
  }

  /**
   * Create a configuration with the given name declared as the input
   * stream.
   * @param name stream name.
   * @return the prepared configuration.
   */
  private static Configuration configWithStream(final String name) {
    final Configuration conf = new Configuration(false);
    conf.set(INPUT_STREAM_TYPE, name);
    return conf;
  }

  /**
   * Custom factory loading: the good path.
   */
  @Test
  public void testCustomFactoryLoad() throws Throwable {
    final Configuration conf = configWithStream(INPUT_STREAM_TYPE_CUSTOM);
    conf.set(INPUT_STREAM_CUSTOM_FACTORY, CustomFactory.class.getName());
    final ObjectInputStreamFactory factory = factoryFromConfig(conf);
    assertThat(factory.streamType())
        .isEqualTo(InputStreamType.Custom);
    assertThat(factory)
        .isInstanceOf(CustomFactory.class);
  }

  /**
   * A custom factory must have a classname.
   */
  @Test
  public void testCustomFactoryUndefined() throws Throwable {
    intercept(IllegalArgumentException.class, E_EMPTY_CUSTOM_CLASSNAME,
        () -> factory(INPUT_STREAM_TYPE_CUSTOM));
  }

  /**
   * Constructor failures are passed in, deeply wrapped though.
   */
  @Test
  public void testCustomConstructorFailure() throws Throwable {
    final Configuration conf = configWithStream(INPUT_STREAM_TYPE_CUSTOM);
    conf.set(INPUT_STREAM_CUSTOM_FACTORY, FactoryFailsToInstantiate.class.getName());
    final RuntimeException ex =
        intercept(RuntimeException.class, "InvocationTargetException",
            () -> factoryFromConfig(conf));
    assertThat(ex.getCause().getCause())
        .describedAs("innermost exception")
        .isInstanceOf(UncheckedIOException.class);
  }

  /**
   * Simple factory.
   */
  public static class CustomFactory extends AbstractObjectInputStreamFactory {

    public CustomFactory() {
      super("custom");
    }

    @Override
    public InputStreamType streamType() {
      return InputStreamType.Custom;
    }

    @Override
    public ObjectInputStream readObject(final ObjectReadParameters parameters) throws IOException {
      return null;
    }

    @Override
    public StreamFactoryRequirements factoryRequirements() {
      return null;
    }
  }

  /**
   * Factory which raises an exception during construction.
   */
  public static final class FactoryFailsToInstantiate extends CustomFactory {

    public FactoryFailsToInstantiate() {
      throw new UncheckedIOException("failed to instantiate", new IOException());
    }

  }

  /**
   * Callbacks from {@link ObjectInputStreamFactory} instances.
   */
  private static final class Callbacks implements ObjectInputStreamFactory.StreamFactoryCallbacks {

    @Override
    public S3AsyncClient getOrCreateAsyncClient(final boolean requireCRT) throws IOException {
      throw new UnsupportedOperationException("not implemented");
    }
  }

}
