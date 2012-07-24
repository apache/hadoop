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
import java.util.concurrent.Semaphore;

import junit.framework.TestCase;

public class TestFileSystemCaching extends TestCase {

  static class DefaultFs extends LocalFileSystem {
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
  
  public void testDefaultFsUris() throws Exception {
    final Configuration conf = new Configuration();
    conf.set("fs.defaultfs.impl", DefaultFs.class.getName());
    final URI defaultUri = URI.create("defaultfs://host");
    FileSystem.setDefaultUri(conf, defaultUri);
    FileSystem fs = null;
    
    // sanity check default fs
    final FileSystem defaultFs = FileSystem.get(conf);
    assertEquals(defaultUri, defaultFs.getUri());
    
    // has scheme, no auth
    fs = FileSystem.get(URI.create("defaultfs:/"), conf);
    assertSame(defaultFs, fs);
    fs = FileSystem.get(URI.create("defaultfs:///"), conf);
    assertSame(defaultFs, fs);
    
    // has scheme, same auth
    fs = FileSystem.get(URI.create("defaultfs://host"), conf);
    assertSame(defaultFs, fs);
    
    // has scheme, different auth
    fs = FileSystem.get(URI.create("defaultfs://host2"), conf);
    assertNotSame(defaultFs, fs);
    
    // no scheme, no auth
    fs = FileSystem.get(URI.create("/"), conf);
    assertSame(defaultFs, fs);
    
    // no scheme, same auth
    try {
      fs = FileSystem.get(URI.create("//host"), conf);
      fail("got fs with auth but no scheme");
    } catch (Exception e) {
      assertEquals("No FileSystem for scheme: null", e.getMessage());
    }
    
    // no scheme, different auth
    try {
      fs = FileSystem.get(URI.create("//host2"), conf);
      fail("got fs with auth but no scheme");
    } catch (Exception e) {
      assertEquals("No FileSystem for scheme: null", e.getMessage());
    }
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
}
