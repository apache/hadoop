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

package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.util.NoSuchElementException;

import org.apache.hadoop.hdfs.protocol.CorruptFileBlocks;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

/**
 * Provides an iterator interface for listCorruptFileBlocks.
 * This class is used by DistributedFileSystem and Hdfs.
 */
public class CorruptFileBlockIterator implements RemoteIterator<Path> {
  private final DFSClient dfs;
  private final String path;

  private String[] files = null;
  private int fileIdx = 0;
  private String cookie = null;
  private Path nextPath = null;

  private int callsMade = 0;

  public CorruptFileBlockIterator(DFSClient dfs, Path path) throws IOException {
    this.dfs = dfs;
    this.path = path2String(path);
    loadNext();
  }

  /**
   * @return the number of calls made to the DFSClient.
   * This is for debugging and testing purposes.
   */
  public int getCallsMade() {
    return callsMade;
  }

  private String path2String(Path path) {
    return path.toUri().getPath();
  }

  private Path string2Path(String string) {
    return new Path(string);
  }

  private void loadNext() throws IOException {
    if (files == null || fileIdx >= files.length) {
      CorruptFileBlocks cfb = dfs.listCorruptFileBlocks(path, cookie);
      files = cfb.getFiles();
      cookie = cfb.getCookie();
      fileIdx = 0;
      callsMade++;
    }

    if (fileIdx >= files.length) {
      // received an empty response
      // there are no more corrupt file blocks
      nextPath = null;
    } else {
      nextPath = string2Path(files[fileIdx]);
      fileIdx++;
    }
  }

  
  @Override
  public boolean hasNext() {
    return nextPath != null;
  }

  
  @Override
  public Path next() throws IOException {
    if (!hasNext()) {
      throw new NoSuchElementException("No more corrupt file blocks");
    }

    Path result = nextPath;
    loadNext();

    return result;
  }
}