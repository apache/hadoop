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

import static junit.framework.Assert.assertSame;
import static junit.framework.Assert.assertNotSame;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.junit.Test;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.Semaphore;

import static org.mockito.Mockito.mock;
import static junit.framework.Assert.assertTrue;


public class TestFileSystemCaching {

  @Test
  public void testCacheEnabled() throws Exception {
    Configuration conf = new Configuration();
    conf.set("fs.cachedfile.impl", conf.get("fs.file.impl"));
    FileSystem fs1 = FileSystem.get(new URI("cachedfile://a"), conf);
    FileSystem fs2 = FileSystem.get(new URI("cachedfile://a"), conf);
    assertSame(fs1, fs2);
  }

  public static class InitializeForeverFileSystem extends LocalFileSystem {
    final static Semaphore sem = new Semaphore(0);
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
      public void run() {
        conf.set("fs.localfs1.impl", "org.apache.hadoop.fs." +
         "TestFileSystemCaching$InitializeForeverFileSystem");
        try {
          FileSystem.get(new URI("localfs1://a"), conf);
        } catch (IOException e) {
          e.printStackTrace();
        } catch (URISyntaxException e) {
          e.printStackTrace();
        }
      }
    };
    t.start();
    // wait for InitializeForeverFileSystem to start initialization
    InitializeForeverFileSystem.sem.acquire();
    
    conf.set("fs.cachedfile.impl", conf.get("fs.file.impl"));
    FileSystem.get(new URI("cachedfile://a"), conf);
    t.interrupt();
    t.join();
  }

  @Test
  public void testCacheDisabled() throws Exception {
    Configuration conf = new Configuration();
    conf.set("fs.uncachedfile.impl", conf.get("fs.file.impl"));
    conf.setBoolean("fs.uncachedfile.impl.disable.cache", true);
    FileSystem fs1 = FileSystem.get(new URI("uncachedfile://a"), conf);
    FileSystem fs2 = FileSystem.get(new URI("uncachedfile://a"), conf);
    assertNotSame(fs1, fs2);
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public <T extends TokenIdentifier> void testCacheForUgi() throws Exception {
    final Configuration conf = new Configuration();
    conf.set("fs.cachedfile.impl", conf.get("fs.file.impl"));
    UserGroupInformation ugiA = UserGroupInformation.createRemoteUser("foo");
    UserGroupInformation ugiB = UserGroupInformation.createRemoteUser("bar");
    FileSystem fsA = ugiA.doAs(new PrivilegedExceptionAction<FileSystem>() {
      public FileSystem run() throws Exception {
        return FileSystem.get(new URI("cachedfile://a"), conf);
      }
    });
    FileSystem fsA1 = ugiA.doAs(new PrivilegedExceptionAction<FileSystem>() {
      public FileSystem run() throws Exception {
        return FileSystem.get(new URI("cachedfile://a"), conf);
      }
    });
    //Since the UGIs are the same, we should have the same filesystem for both
    assertSame(fsA, fsA1);
    
    FileSystem fsB = ugiB.doAs(new PrivilegedExceptionAction<FileSystem>() {
      public FileSystem run() throws Exception {
        return FileSystem.get(new URI("cachedfile://a"), conf);
      }
    });
    //Since the UGIs are different, we should end up with different filesystems
    //corresponding to the two UGIs
    assertNotSame(fsA, fsB);
    
    Token<T> t1 = mock(Token.class);
    ugiA = UserGroupInformation.createRemoteUser("foo");
    ugiA.addToken(t1);
    
    fsA = ugiA.doAs(new PrivilegedExceptionAction<FileSystem>() {
      public FileSystem run() throws Exception {
        return FileSystem.get(new URI("cachedfile://a"), conf);
      }
    });
    //Although the users in the UGI are same, ugiA has tokens in it, and
    //we should end up with different filesystems corresponding to the two UGIs
    assertNotSame(fsA, fsA1);
    
    ugiA = UserGroupInformation.createRemoteUser("foo");
    ugiA.addToken(t1);
    
    fsA1 = ugiA.doAs(new PrivilegedExceptionAction<FileSystem>() {
      public FileSystem run() throws Exception {
        return FileSystem.get(new URI("cachedfile://a"), conf);
      }
    });
    //Now the users in the UGI are the same, and they also have the same token.
    //We should have the same filesystem for both
    assertSame(fsA, fsA1);
  }
  
  @Test
  public void testUserFS() throws Exception {
    final Configuration conf = new Configuration();
    conf.set("fs.cachedfile.impl", conf.get("fs.file.impl"));
    FileSystem fsU1 = FileSystem.get(new URI("cachedfile://a"), conf, "bar");
    FileSystem fsU2 = FileSystem.get(new URI("cachedfile://a"), conf, "foo");
    
    assertNotSame(fsU1, fsU2);   
  }
  
  @Test
  public void testFsUniqueness() throws Exception {
    final Configuration conf = new Configuration();
    conf.set("fs.cachedfile.impl", conf.get("fs.file.impl"));
    // multiple invocations of FileSystem.get return the same object.
    FileSystem fs1 = FileSystem.get(conf);
    FileSystem fs2 = FileSystem.get(conf);
    assertTrue(fs1 == fs2);

    // multiple invocations of FileSystem.newInstance return different objects
    fs1 = FileSystem.newInstance(new URI("cachedfile://a"), conf, "bar");
    fs2 = FileSystem.newInstance(new URI("cachedfile://a"), conf, "bar");
    assertTrue(fs1 != fs2 && !fs1.equals(fs2));
    fs1.close();
    fs2.close();
  }
}
