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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.junit.Test;

/**
 * The default port of DelegateToFileSystem is set from child file system.
 */
public class TestDelegateToFsCheckPath {
  @Test
  public void testCheckPathWithoutDefaultPort() throws URISyntaxException,
      IOException {
    URI uri = new URI("dummy://dummy-host");
    AbstractFileSystem afs = new DummyDelegateToFileSystem(uri,
        new UnOverrideDefaultPortFileSystem());
    afs.checkPath(new Path("dummy://dummy-host"));
  }

  @Test
  public void testCheckPathWithDefaultPort() throws URISyntaxException,
      IOException {
    URI uri = new URI(String.format("dummy://dummy-host:%d",
        OverrideDefaultPortFileSystem.DEFAULT_PORT));
    AbstractFileSystem afs = new DummyDelegateToFileSystem(uri,
        new OverrideDefaultPortFileSystem());
    afs.checkPath(new Path("dummy://dummy-host/user/john/test"));
  }

  private static class DummyDelegateToFileSystem
      extends DelegateToFileSystem {
    public DummyDelegateToFileSystem(URI uri, FileSystem fs)
        throws URISyntaxException, IOException {
      super(uri, fs, new Configuration(), "dummy", false);
    }
  }

  /**
   * UnOverrideDefaultPortFileSystem does not define default port.
   * The default port defined by AbstractFilesystem is used in this case.
   * (default 0).
   */
  private static class UnOverrideDefaultPortFileSystem extends FileSystem {
    @Override
    public URI getUri() {
      // deliberately empty
      return null;
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
      // deliberately empty
      return null;
    }
    @Override
    public FSDataOutputStream create(Path f, FsPermission permission,
        boolean overwrite, int bufferSize, short replication, long blockSize,
        Progressable progress) throws IOException {
      // deliberately empty
      return null;
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize,
        Progressable progress) throws IOException {
      // deliberately empty
      return null;
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
      // deliberately empty
      return false;
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
      // deliberately empty
      return false;
    }

    @Override
    public FileStatus[] listStatus(Path f) throws FileNotFoundException,
        IOException {
      // deliberately empty
      return new FileStatus[0];
    }

    @Override
    public void setWorkingDirectory(Path newDir) {
      // deliberately empty
    }

    @Override
    public Path getWorkingDirectory() {
      // deliberately empty
      return null;
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
      // deliberately empty
      return false;
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
      // deliberately empty
      return null;
    }
  }

  /**
   * OverrideDefaultPortFileSystem defines default port.
   */
  private static class OverrideDefaultPortFileSystem
      extends UnOverrideDefaultPortFileSystem {
    private static final int DEFAULT_PORT = 1234;

    @Override
    public int getDefaultPort() {
      return DEFAULT_PORT;
    }
  }
}
