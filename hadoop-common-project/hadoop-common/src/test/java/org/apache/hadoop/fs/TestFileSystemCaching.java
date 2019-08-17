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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;

import org.junit.Test;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.Semaphore;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


public class TestFileSystemCaching {

  @Test
  public void testCacheEnabled() throws Exception {
    Configuration conf = newConf();
    FileSystem fs1 = FileSystem.get(new URI("cachedfile://a"), conf);
    FileSystem fs2 = FileSystem.get(new URI("cachedfile://a"), conf);
    assertSame(fs1, fs2);
  }

  private static class DefaultFs extends LocalFileSystem {
    URI uri;
    @Override
    public void initialize(URI uri, Configuration conf) {
      this.uri = uri;
    }
    @Override
    public URI getUri() {
      return uri;
    }
  }
  
  @Test
  public void testDefaultFsUris() throws Exception {
    final Configuration conf = new Configuration();
    conf.set("fs.defaultfs.impl", DefaultFs.class.getName());
    final URI defaultUri = URI.create("defaultfs://host");
    FileSystem.setDefaultUri(conf, defaultUri);

    // sanity check default fs
    final FileSystem defaultFs = FileSystem.get(conf);
    assertEquals(defaultUri, defaultFs.getUri());
    
    // has scheme, no auth
    assertSame(defaultFs, FileSystem.get(URI.create("defaultfs:/"), conf));
    assertSame(defaultFs, FileSystem.get(URI.create("defaultfs:///"), conf));
    
    // has scheme, same auth
    assertSame(defaultFs, FileSystem.get(URI.create("defaultfs://host"), conf));
    
    // has scheme, different auth
    assertNotSame(defaultFs,
        FileSystem.get(URI.create("defaultfs://host2"), conf));
    
    // no scheme, no auth
    assertSame(defaultFs, FileSystem.get(URI.create("/"), conf));
    
    // no scheme, same auth
    intercept(UnsupportedFileSystemException.class,
        () -> FileSystem.get(URI.create("//host"), conf));
    intercept(UnsupportedFileSystemException.class,
        () -> FileSystem.get(URI.create("//host2"), conf));
  }
  
  public static class InitializeForeverFileSystem extends LocalFileSystem {
    final static Semaphore sem = new Semaphore(0);
    @Override
    public void initialize(URI uri, Configuration conf) throws IOException {
      // notify that InitializeForeverFileSystem started initialization
      sem.release();
      try {
        while (true) {
          Thread.sleep(1000);
        }
      } catch (InterruptedException e) {
        return;
      }
    }
  }
  
  @Test
  public void testCacheEnabledWithInitializeForeverFS() throws Exception {
    final Configuration conf = new Configuration();
    Thread t = new Thread() {
      @Override
      public void run() {
        conf.set("fs.localfs1.impl", "org.apache.hadoop.fs." +
         "TestFileSystemCaching$InitializeForeverFileSystem");
        try {
          FileSystem.get(new URI("localfs1://a"), conf);
        } catch (IOException | URISyntaxException e) {
          e.printStackTrace();
        }
      }
    };
    t.start();
    // wait for InitializeForeverFileSystem to start initialization
    InitializeForeverFileSystem.sem.acquire();
    
    conf.set("fs.cachedfile.impl", FileSystem.getFileSystemClass("file", null).getName());
    FileSystem.get(new URI("cachedfile://a"), conf);
    t.interrupt();
    t.join();
  }

  @Test
  public void testCacheDisabled() throws Exception {
    Configuration conf = new Configuration();
    conf.set("fs.uncachedfile.impl", FileSystem.getFileSystemClass("file", null).getName());
    conf.setBoolean("fs.uncachedfile.impl.disable.cache", true);
    FileSystem fs1 = FileSystem.get(new URI("uncachedfile://a"), conf);
    FileSystem fs2 = FileSystem.get(new URI("uncachedfile://a"), conf);
    assertNotSame(fs1, fs2);
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public <T extends TokenIdentifier> void testCacheForUgi() throws Exception {
    final Configuration conf = newConf();
    UserGroupInformation ugiA = UserGroupInformation.createRemoteUser("foo");
    UserGroupInformation ugiB = UserGroupInformation.createRemoteUser("bar");
    FileSystem fsA = getCachedFS(ugiA, conf);
    FileSystem fsA1 = getCachedFS(ugiA, conf);
    //Since the UGIs are the same, we should have the same filesystem for both
    assertSame(fsA, fsA1);
    
    FileSystem fsB = getCachedFS(ugiB, conf);
    //Since the UGIs are different, we should end up with different filesystems
    //corresponding to the two UGIs
    assertNotSame(fsA, fsB);
    
    Token<T> t1 = mock(Token.class);
    UserGroupInformation ugiA2 = UserGroupInformation.createRemoteUser("foo");
    
    fsA = getCachedFS(ugiA2, conf);
    // Although the users in the UGI are same, they have different subjects
    // and so are different.
    assertNotSame(fsA, fsA1);
    
    ugiA.addToken(t1);
    
    fsA = getCachedFS(ugiA, conf);
    // Make sure that different UGI's with the same subject lead to the same
    // file system.
    assertSame(fsA, fsA1);
  }

  /**
   * Get the cached filesystem for "cachedfile://a" for the supplied user
   * @param ugi user
   * @param conf configuration
   * @return the filesystem
   * @throws IOException failure to get/init
   * @throws InterruptedException part of the signature of UGI.doAs()
   */
  private FileSystem getCachedFS(UserGroupInformation ugi, Configuration conf)
      throws IOException, InterruptedException {
    return ugi.doAs((PrivilegedExceptionAction<FileSystem>)
            () -> FileSystem.get(new URI("cachedfile://a"), conf));
  }

  @Test
  public void testUserFS() throws Exception {
    final Configuration conf = newConf();
    FileSystem fsU1 = FileSystem.get(new URI("cachedfile://a"), conf, "bar");
    FileSystem fsU2 = FileSystem.get(new URI("cachedfile://a"), conf, "foo");
    
    assertNotSame(fsU1, fsU2);   
  }

  private Configuration newConf() throws IOException {
    final Configuration conf = new Configuration();
    conf.set("fs.cachedfile.impl",
        FileSystem.getFileSystemClass("file", null).getName());
    return conf;
  }

  @Test
  public void testFsUniqueness() throws Exception {
    final Configuration conf = newConf();
    // multiple invocations of FileSystem.get return the same object.
    FileSystem fs1 = FileSystem.get(conf);
    FileSystem fs2 = FileSystem.get(conf);
    assertSame(fs1, fs2);

    // multiple invocations of FileSystem.newInstance return different objects
    fs1 = FileSystem.newInstance(new URI("cachedfile://a"), conf, "bar");
    fs2 = FileSystem.newInstance(new URI("cachedfile://a"), conf, "bar");
    assertTrue(fs1 != fs2 && !fs1.equals(fs2));
    fs1.close();
    fs2.close();
  }
  
  @Test
  public void testCloseAllForUGI() throws Exception {
    final Configuration conf = newConf();
    UserGroupInformation ugiA = UserGroupInformation.createRemoteUser("foo");
    FileSystem fsA = getCachedFS(ugiA, conf);
    //Now we should get the cached filesystem
    FileSystem fsA1 = getCachedFS(ugiA, conf);
    assertSame(fsA, fsA1);
    
    FileSystem.closeAllForUGI(ugiA);
    
    //Now we should get a different (newly created) filesystem
    fsA1 = getCachedFS(ugiA, conf);
    assertNotSame(fsA, fsA1);
  }
  
  @Test
  public void testDelete() throws IOException {
    FileSystem mockFs = mock(FileSystem.class);
    FileSystem fs = new FilterFileSystem(mockFs);    
    Path path = new Path("/a");

    fs.delete(path, false);
    verify(mockFs).delete(eq(path), eq(false));
    reset(mockFs);
    fs.delete(path, true);
    verify(mockFs).delete(eq(path), eq(true));
  }

  @Test
  public void testDeleteOnExit() throws IOException {
    FileSystem mockFs = mock(FileSystem.class);
    Path path = new Path("/a");
    try (FileSystem fs = new FilterFileSystem(mockFs)) {

      // delete on close if path does exist
      when(mockFs.getFileStatus(eq(path))).thenReturn(new FileStatus());
      assertTrue(fs.deleteOnExit(path));
      verify(mockFs).getFileStatus(eq(path));
      reset(mockFs);
      when(mockFs.getFileStatus(eq(path))).thenReturn(new FileStatus());
      fs.close();
    }
    verify(mockFs).getFileStatus(eq(path));
    verify(mockFs).delete(eq(path), eq(true));
  }

  @Test
  public void testDeleteOnExitFNF() throws IOException {
    FileSystem mockFs = mock(FileSystem.class);
    Path path;
    try (FileSystem fs = new FilterFileSystem(mockFs)) {
      path = new Path("/a");

      // don't delete on close if path doesn't exist
      assertFalse(fs.deleteOnExit(path));
      verify(mockFs).getFileStatus(eq(path));
      reset(mockFs);
      fs.close();
    }
    verify(mockFs, never()).getFileStatus(eq(path));
    verify(mockFs, never()).delete(any(Path.class), anyBoolean());
  }


  @Test
  public void testDeleteOnExitRemoved() throws IOException {
    FileSystem mockFs = mock(FileSystem.class);
    Path path;
    try (FileSystem fs = new FilterFileSystem(mockFs)) {
      path = new Path("/a");

      // don't delete on close if path existed, but later removed
      when(mockFs.getFileStatus(eq(path))).thenReturn(new FileStatus());
      assertTrue(fs.deleteOnExit(path));
      verify(mockFs).getFileStatus(eq(path));
      reset(mockFs);
      fs.close();
    }
    verify(mockFs).getFileStatus(eq(path));
    verify(mockFs, never()).delete(any(Path.class), anyBoolean());
  }

  @Test
  public void testCancelDeleteOnExit() throws IOException {
    FileSystem mockFs = mock(FileSystem.class);
    try (FileSystem fs = new FilterFileSystem(mockFs)) {
      Path path = new Path("/a");

      // don't delete on close if path existed, but later cancelled
      when(mockFs.getFileStatus(eq(path))).thenReturn(new FileStatus());
      assertTrue(fs.deleteOnExit(path));
      verify(mockFs).getFileStatus(eq(path));
      assertTrue(fs.cancelDeleteOnExit(path));
      assertFalse(fs.cancelDeleteOnExit(path)); // false because not registered
      reset(mockFs);
      fs.close();
    }
    verify(mockFs, never()).getFileStatus(any(Path.class));
    verify(mockFs, never()).delete(any(Path.class), anyBoolean());
  }

  @Test
  public void testCacheIncludesURIUserInfo() throws Throwable {
    URI containerA = new URI("wasb://a@account.blob.core.windows.net");
    URI containerB = new URI("wasb://b@account.blob.core.windows.net");
    Configuration conf = new Configuration(false);
    FileSystem.Cache.Key keyA = new FileSystem.Cache.Key(containerA, conf);
    FileSystem.Cache.Key keyB = new FileSystem.Cache.Key(containerB, conf);
    assertNotEquals(keyA, keyB);
    assertNotEquals(keyA, new FileSystem.Cache.Key(
        new URI("wasb://account.blob.core.windows.net"), conf));
    assertEquals(keyA, new FileSystem.Cache.Key(
        new URI("wasb://A@account.blob.core.windows.net"), conf));
    assertNotEquals(keyA, new FileSystem.Cache.Key(
        new URI("wasb://a:password@account.blob.core.windows.net"), conf));
  }
}
