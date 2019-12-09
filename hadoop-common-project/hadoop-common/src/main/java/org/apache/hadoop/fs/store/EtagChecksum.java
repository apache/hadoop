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

package org.apache.hadoop.fs.store;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.fs.FileChecksum;

/**
 * An etag as a checksum.
 * Consider these suitable for checking if an object has changed, but
 * not suitable for comparing two different objects for equivalence,
 * especially between object stores.
 */
public class EtagChecksum extends FileChecksum {

  /** The algorithm name: {@value}. */
  private static final String ETAG = "etag";

  /**
   * Etag string.
   */
  private String eTag = "";

  /**
   * Create with an empty etag.
   */
  public EtagChecksum() {
  }

  /**
   * Create with a string etag.
   * @param eTag etag
   */
  public EtagChecksum(String eTag) {
    this.eTag = eTag;
  }

  @Override
  public String getAlgorithmName() {
    return ETAG;
  }

  @Override
  public int getLength() {
    return eTag.getBytes(StandardCharsets.UTF_8).length;
  }

  @Override
  public byte[] getBytes() {
    return eTag != null
        ? eTag.getBytes(StandardCharsets.UTF_8)
        : new byte[0];
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(eTag != null ? eTag : "");
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    eTag = in.readUTF();
  }

  @Override
  public String toString() {
    return "etag: \"" + eTag  + '"';
  }

}
