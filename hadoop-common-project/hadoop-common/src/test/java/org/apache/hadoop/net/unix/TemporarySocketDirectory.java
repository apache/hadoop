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
package org.apache.hadoop.net.unix;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Random;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileUtil;

/**
 * Create a temporary directory in which sockets can be created.
 * When creating a UNIX domain socket, the name
 * must be fairly short (around 110 bytes on most platforms).
 */
public class TemporarySocketDirectory implements Closeable {
  private File dir;

  public TemporarySocketDirectory() {
    String tmp = System.getProperty("java.io.tmpdir", "/tmp");
    dir = new File(tmp, "socks." + (System.currentTimeMillis() +
        "." + (new Random().nextInt())));
    dir.mkdirs();
    FileUtil.setWritable(dir, true);
  }

  public File getDir() {
    return dir;
  }

  @Override
  public void close() throws IOException {
    if (dir != null) {
      FileUtils.deleteDirectory(dir);
      dir = null;
    }
  }

  protected void finalize() throws IOException {
    close();
  }
}
