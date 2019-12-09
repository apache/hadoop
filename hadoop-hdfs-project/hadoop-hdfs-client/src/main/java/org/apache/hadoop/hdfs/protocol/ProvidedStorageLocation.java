/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;

import javax.annotation.Nonnull;
import java.util.Arrays;

/**
 * ProvidedStorageLocation is a location in an external storage system
 * containing the data for a block (~Replica).
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ProvidedStorageLocation {
  private final Path path;
  private final long offset;
  private final long length;
  private final byte[] nonce;

  public ProvidedStorageLocation(Path path, long offset, long length,
      byte[] nonce) {
    this.path = path;
    this.offset = offset;
    this.length = length;
    this.nonce = Arrays.copyOf(nonce, nonce.length);
  }

  public @Nonnull Path getPath() {
    return path;
  }

  public long getOffset() {
    return offset;
  }

  public long getLength() {
    return length;
  }

  public @Nonnull byte[] getNonce() {
    // create a copy of the nonce and return it.
    return Arrays.copyOf(nonce, nonce.length);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ProvidedStorageLocation that = (ProvidedStorageLocation) o;

    if ((offset != that.offset) || (length != that.length)
        || !path.equals(that.path)) {
      return false;
    }
    return Arrays.equals(nonce, that.nonce);
  }

  @Override
  public int hashCode() {
    int result = path.hashCode();
    result = 31 * result + (int) (offset ^ (offset >>> 32));
    result = 31 * result + (int) (length ^ (length >>> 32));
    result = 31 * result + Arrays.hashCode(nonce);
    return result;
  }
}
