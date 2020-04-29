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
package org.apache.hadoop.fs;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Byte array backed part handle.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class BBPartHandle implements PartHandle {

  private static final long serialVersionUID = 0x23ce3eb1;

  private final byte[] bytes;

  private BBPartHandle(ByteBuffer byteBuffer){
    this.bytes = byteBuffer.array();
  }

  public static PartHandle from(ByteBuffer byteBuffer) {
    return new BBPartHandle(byteBuffer);
  }

  @Override
  public ByteBuffer bytes() {
    return ByteBuffer.wrap(bytes);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(bytes);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof PartHandle)) {
      return false;

    }
    PartHandle o = (PartHandle) other;
    return bytes().equals(o.bytes());
  }
}
