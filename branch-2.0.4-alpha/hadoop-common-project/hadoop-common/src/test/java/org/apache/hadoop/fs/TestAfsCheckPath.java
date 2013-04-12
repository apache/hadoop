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
import java.util.EnumSet;

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.Progressable;
import org.junit.Test;

public class TestAfsCheckPath {
  
  private static int DEFAULT_PORT = 1234;
  private static int OTHER_PORT = 4321;
  
  @Test
  public void testCheckPathWithNoPorts() throws URISyntaxException {
    URI uri = new URI("dummy://dummy-host");
    AbstractFileSystem afs = new DummyFileSystem(uri);
    afs.checkPath(new Path("dummy://dummy-host"));
  }
  
  @Test
  public void testCheckPathWithDefaultPort() throws URISyntaxException {
    URI uri = new URI("dummy://dummy-host:" + DEFAULT_PORT);
    AbstractFileSystem afs = new DummyFileSystem(uri);
    afs.checkPath(new Path("dummy://dummy-host:" + DEFAULT_PORT));
  }
  
  @Test
  public void testCheckPathWithTheSameNonDefaultPort()
      throws URISyntaxException {
    URI uri = new URI("dummy://dummy-host:" + OTHER_PORT);
    AbstractFileSystem afs = new DummyFileSystem(uri);
    afs.checkPath(new Path("dummy://dummy-host:" + OTHER_PORT));
  }
  
  @Test(expected=InvalidPathException.class)
  public void testCheckPathWithDifferentPorts() throws URISyntaxException {
    URI uri = new URI("dummy://dummy-host:" + DEFAULT_PORT);
    AbstractFileSystem afs = new DummyFileSystem(uri);
    afs.checkPath(new Path("dummy://dummy-host:" + OTHER_PORT));
  }
  
  private static class DummyFileSystem extends AbstractFileSystem {
    
    public DummyFileSystem(URI uri) throws URISyntaxException {
      super(uri, "dummy", true, DEFAULT_PORT);
    }
    
    @Override
    public int getUriDefaultPort() {
      return DEFAULT_PORT;
    }

    @Override
    public FSDataOutputStream createInternal(Path f, EnumSet<CreateFlag> flag,
        FsPermission absolutePermission, int bufferSize, short replication,
        long blockSize, Progressable progress, ChecksumOpt checksumOpt,
        boolean createParent) throws IOException {
      // deliberately empty
      return null;
    }

    @Override
    public boolean delete(Path f, boolean recursive)
        throws AccessControlException, FileNotFoundException,
        UnresolvedLinkException, IOException {
      // deliberately empty
      return false;
    }

    @Override
    public BlockLocation[] getFileBlockLocations(Path f, long start, long len)
        throws IOException {
      // deliberately empty
      return null;
    }

    @Override
    public FileChecksum getFileChecksum(Path f) throws IOException {
      // deliberately empty
      return null;
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
      // deliberately empty
      return null;
    }

    @Override
    public FsStatus getFsStatus() throws IOException {
      // deliberately empty
      return null;
    }

    @Override
    public FsServerDefaults getServerDefaults() throws IOException {
      // deliberately empty
      return null;
    }

    @Override
    public FileStatus[] listStatus(Path f) throws IOException {
      // deliberately empty
      return null;
    }

    @Override
    public void mkdir(Path dir, FsPermission permission, boolean createParent)
        throws IOException {
      // deliberately empty
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
      // deliberately empty
      return null;
    }

    @Override
    public void renameInternal(Path src, Path dst) throws IOException {
      // deliberately empty
    }

    @Override
    public void setOwner(Path f, String username, String groupname)
        throws IOException {
      // deliberately empty
    }

    @Override
    public void setPermission(Path f, FsPermission permission)
        throws IOException {
      // deliberately empty
    }

    @Override
    public boolean setReplication(Path f, short replication) throws IOException {
      // deliberately empty
      return false;
    }

    @Override
    public void setTimes(Path f, long mtime, long atime) throws IOException {
      // deliberately empty
    }

    @Override
    public void setVerifyChecksum(boolean verifyChecksum) throws IOException {
      // deliberately empty
    }
    
  }
}
