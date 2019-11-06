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
package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.ServiceConfigurationError;

import org.junit.Test;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;

/**
 * Tests related to filesystem creation and lifecycle.
 */
public class TestFileSystemInitialization {

 /**
   * Check if FileSystem can be properly initialized if URLStreamHandlerFactory
   * is registered.
   */
  @Test
  public void testInitializationWithRegisteredStreamFactory() {
    Configuration conf = new Configuration();
    URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory(conf));
    try {
      FileSystem.getFileSystemClass("file", conf);
    }
    catch (IOException ok) {
      // we might get an exception but this not related to infinite loop problem
      assertFalse(false);
    }
  }

  @Test
  public void testMissingLibraries() {
    try {
      Configuration conf = new Configuration();
      Class<? extends FileSystem> fs = FileSystem.getFileSystemClass("s3a",
          conf);
      fail("Expected an exception, got a filesystem: " + fs);
    } catch (Exception | ServiceConfigurationError expected) {
    }
  }

  @Test
  public void testNewInstanceFailure() throws Throwable {
    intercept(IOException.class, FailingFileSystem.INITIALIZE, () ->
        FileSystem.newInstance(new URI("failing://localhost"), FailingFileSystem
            .failingConf()));
    assertThat(FailingFileSystem.initCount).describedAs("init count")
        .isEqualTo(1);
    assertThat(FailingFileSystem.closeCount).describedAs("close count")
        .isEqualTo(1);
  }

  /**
   * An FS which will fail on both init and close, and update
   * counters of invocations as it does so.
   */
  public static class FailingFileSystem extends FileSystem {

    public static final String INITIALIZE = "initialize()";

    public static final String CLOSE = "close()";

    private static int initCount;

    private static int closeCount;

    private static Configuration failingConf() {
      final Configuration conf = new Configuration(false);
      conf.setClass("fs.failing.impl", FailingFileSystem.class,
          FileSystem.class);
      return conf;
    }

    @Override
    public void initialize(final URI name, final Configuration conf)
        throws IOException {
      super.initialize(name, conf);
      initCount++;
      throw new IOException(INITIALIZE);
    }

    @Override
    public void close() throws IOException {
      closeCount++;
      throw new IOException(CLOSE);
    }

    @Override
    public URI getUri() {
      return null;
    }

    @Override
    public FSDataInputStream open(final Path f, final int bufferSize)
        throws IOException {
      return null;
    }

    @Override
    public FSDataOutputStream create(final Path f,
        final FsPermission permission,
        final boolean overwrite,
        final int bufferSize,
        final short replication,
        final long blockSize,
        final Progressable progress) throws IOException {
      return null;
    }

    @Override
    public FSDataOutputStream append(final Path f,
        final int bufferSize,
        final Progressable progress) throws IOException {
      return null;
    }

    @Override
    public boolean rename(final Path src, final Path dst) throws IOException {
      return false;
    }

    @Override
    public boolean delete(final Path f, final boolean recursive)
        throws IOException {
      return false;
    }

    @Override
    public FileStatus[] listStatus(final Path f)
        throws FileNotFoundException, IOException {
      return new FileStatus[0];
    }

    @Override
    public void setWorkingDirectory(final Path new_dir) {

    }

    @Override
    public Path getWorkingDirectory() {
      return null;
    }

    @Override
    public boolean mkdirs(final Path f, final FsPermission permission)
        throws IOException {
      return false;
    }

    @Override
    public FileStatus getFileStatus(final Path f) throws IOException {
      return null;
    }
  }

}
