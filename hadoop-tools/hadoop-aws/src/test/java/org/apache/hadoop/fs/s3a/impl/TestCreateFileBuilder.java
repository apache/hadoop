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

package org.apache.hadoop.fs.s3a.impl;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataOutputStreamBuilder;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.test.HadoopTestBase;
import org.apache.hadoop.util.Progressable;

import static org.apache.hadoop.fs.s3a.Constants.FS_S3A_CREATE_HEADER;
import static org.apache.hadoop.fs.s3a.Constants.FS_S3A_CREATE_PERFORMANCE;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Unit test of {@link CreateFileBuilder}.
 */
public class TestCreateFileBuilder extends HadoopTestBase {

  private static final BuilderCallbacks CALLBACKS = new BuilderCallbacks();

  private CreateFileBuilder mkBuilder() throws IOException {
    return new CreateFileBuilder(
        FileSystem.getLocal(new Configuration()),
        new Path("/"),
        CALLBACKS);
  }

  private BuilderOutputStream unwrap(FSDataOutputStream out) {
    OutputStream s = out.getWrappedStream();
    Assertions.assertThat(s)
        .isInstanceOf(BuilderOutputStream.class);
    return (BuilderOutputStream) s;
  }

  private BuilderOutputStream build(FSDataOutputStreamBuilder builder)
      throws IOException {
    return unwrap(builder.build());
  }

  @Test
  public void testSimpleBuild() throws Throwable {
    Assertions.assertThat(build(mkBuilder().create()))
        .matches(p -> !p.isOverwrite())
        .matches(p -> !p.isPerformance());
  }

  @Test
  public void testAppendForbidden() throws Throwable {
    intercept(UnsupportedOperationException.class, () ->
        build(mkBuilder().append()));
  }

  @Test
  public void testPerformanceSupport() throws Throwable {
    CreateFileBuilder builder = mkBuilder().create();
    builder.must(FS_S3A_CREATE_PERFORMANCE, true);
    Assertions.assertThat(build(builder))
        .matches(p -> p.isPerformance());
  }

  @Test
  public void testHeaderOptions() throws Throwable {
    final CreateFileBuilder builder = mkBuilder().create()
        .must(FS_S3A_CREATE_HEADER + ".retention", "permanent")
        .opt(FS_S3A_CREATE_HEADER + ".owner", "engineering");
    final Map<String, String> headers = build(builder).getHeaders();
    Assertions.assertThat(headers)
        .containsEntry("retention", "permanent")
        .containsEntry("owner", "engineering");
  }

  @Test
  public void testIncompleteHeader() throws Throwable {
    final CreateFileBuilder builder = mkBuilder().create()
        .must(FS_S3A_CREATE_HEADER, "permanent");
    intercept(IllegalArgumentException.class, () ->
        build(builder));
  }

  private static final class BuilderCallbacks implements
      CreateFileBuilder.CreateFileBuilderCallbacks {

    @Override
    public FSDataOutputStream createFileFromBuilder(final Path path,
        final Progressable progress,
        final CreateFileBuilder.CreateFileOptions options) throws IOException {
      return new FSDataOutputStream(
          new BuilderOutputStream(
              progress,
              options),
          null);
    }
  }

  /**
   * Stream which will be wrapped and which returns the flags used
   * creating the object.
   */
  private static final class BuilderOutputStream extends OutputStream {

    private final Progressable progress;


    private final CreateFileBuilder.CreateFileOptions options;

    private BuilderOutputStream(final Progressable progress,
        final CreateFileBuilder.CreateFileOptions options) {
      this.progress = progress;
      this.options = options;
    }

    private boolean isOverwrite() {
      return options.getFlags().contains(CreateFlag.OVERWRITE);
    }

    private Progressable getProgress() {
      return progress;
    }

    private boolean isPerformance() {
      return options.isPerformance();
    }

    private CreateFileBuilder.CreateFileOptions getOptions() {
      return options;
    }

    private Map<String, String> getHeaders() {
      return options.getHeaders();
    }

    @Override
    public void write(final int b) throws IOException {

    }

    @Override
    public String toString() {
      return "BuilderOutputStream{" +
          "progress=" + progress +
          ", options=" + options +
          "} " + super.toString();
    }
  }

}
