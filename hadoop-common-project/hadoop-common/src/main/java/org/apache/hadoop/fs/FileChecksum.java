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

import java.util.Arrays;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.io.Writable;

/** An abstract class representing file checksums for files. */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class FileChecksum implements Writable {
  /**
   * The checksum algorithm name.
   *
   * @return algorithm name.
   */
  public abstract String getAlgorithmName();

  /**
   * The length of the checksum in bytes.
   *
   * @return length.
   */
  public abstract int getLength();

  /**
   * The value of the checksum in bytes.
   *
   * @return byte array.
   */
  public abstract byte[] getBytes();

  public ChecksumOpt getChecksumOpt() {
    return null;
  }

  /**
   * Return true if both the algorithms and the values are the same.
   *
   * @param other other.
   * @return if equal true, not false.
   */
  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if (!(other instanceof FileChecksum)) {
      return false;
    }

    final FileChecksum that = (FileChecksum)other;
    return this.getAlgorithmName().equals(that.getAlgorithmName())
      && Arrays.equals(this.getBytes(), that.getBytes());
  }

  @Override
  public int hashCode() {
    return getAlgorithmName().hashCode() ^ Arrays.hashCode(getBytes());
  }
}