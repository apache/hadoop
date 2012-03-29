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
package org.apache.hadoop.hdfs.protocol;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

/**
 * Contains a list of paths corresponding to corrupt files and a cookie
 * used for iterative calls to NameNode.listCorruptFileBlocks.
 *
 */
public class CorruptFileBlocks implements Writable {
  // used for hashCode
  private static final int PRIME = 16777619;

  private String[] files;
  private String cookie;

  public CorruptFileBlocks() {
    this(new String[0], "");
  }

  public CorruptFileBlocks(String[] files, String cookie) {
    this.files = files;
    this.cookie = cookie;
  }

  public String[] getFiles() {
    return files;
  }

  public String getCookie() {
    return cookie;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    int fileCount = in.readInt();
    files = new String[fileCount];
    for (int i = 0; i < fileCount; i++) {
      files[i] = Text.readString(in);
    }
    cookie = Text.readString(in);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(files.length);
    for (int i = 0; i < files.length; i++) {
      Text.writeString(out, files[i]);
    }
    Text.writeString(out, cookie);
  }

  /**
   * {@inheritDoc}
   */
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof CorruptFileBlocks)) {
      return false;
    }
    CorruptFileBlocks other = (CorruptFileBlocks) obj;
    return cookie.equals(other.cookie) &&
      Arrays.equals(files, other.files);
  }

  /**
   * {@inheritDoc}
   */
  public int hashCode() {
    int result = cookie.hashCode();

    for (String file : files) {
      result = PRIME * result + file.hashCode();
    }

    return result;
  }
}
