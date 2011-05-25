/*
 * Copyright 2011 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.util;

import java.util.Arrays;

/**
 * This class encapsulates a byte array and overrides hashCode and equals so
 * that it's identity is based on the data rather than the array instance.
 */
public class HashedBytes {

  private final byte[] bytes;
  private final int hashCode;

  public HashedBytes(byte[] bytes) {
    this.bytes = bytes;
    hashCode = Bytes.hashCode(bytes);
  }

  public byte[] getBytes() {
    return bytes;
  }

  @Override
  public int hashCode() {
    return hashCode;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null || getClass() != obj.getClass())
      return false;
    HashedBytes other = (HashedBytes) obj;
    return Arrays.equals(bytes, other.bytes);
  }

  @Override
  public String toString() {
    return Bytes.toStringBinary(bytes);
  }
}