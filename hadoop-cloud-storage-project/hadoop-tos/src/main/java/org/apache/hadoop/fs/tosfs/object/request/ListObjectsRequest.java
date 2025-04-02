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

package org.apache.hadoop.fs.tosfs.object.request;

public final class ListObjectsRequest {
  private final String prefix;
  private final String startAfter;
  private final int maxKeys;
  private final String delimiter;

  private ListObjectsRequest(String prefix, String startAfter, int maxKeys, String delimiter) {
    this.prefix = prefix;
    this.startAfter = startAfter;
    this.maxKeys = maxKeys;
    this.delimiter = delimiter;
  }

  public String prefix() {
    return prefix;
  }

  public String startAfter() {
    return startAfter;
  }

  public int maxKeys() {
    return maxKeys;
  }

  public String delimiter() {
    return delimiter;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private String prefix;
    private String startAfter;
    // -1 means list all object keys
    private int maxKeys = -1;
    private String delimiter;

    public Builder prefix(String prefixInput) {
      this.prefix = prefixInput;
      return this;
    }

    public Builder startAfter(String startAfterInput) {
      this.startAfter = startAfterInput;
      return this;
    }

    public Builder maxKeys(int maxKeysInput) {
      this.maxKeys = maxKeysInput;
      return this;
    }

    public Builder delimiter(String delimiterInput) {
      this.delimiter = delimiterInput;
      return this;
    }

    public ListObjectsRequest build() {
      return new ListObjectsRequest(prefix, startAfter, maxKeys, delimiter);
    }
  }
}
