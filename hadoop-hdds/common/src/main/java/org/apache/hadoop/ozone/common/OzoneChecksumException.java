/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.common;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;

/** Thrown for checksum errors. */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class OzoneChecksumException extends IOException {

  /**
   * OzoneChecksumException to throw when checksum verfication fails.
   * @param index checksum list index at which checksum match failed
   */
  public OzoneChecksumException(int index) {
    super(String.format("Checksum mismatch at index %d", index));
  }

  /**
   * OzoneChecksumException to throw when unrecognized checksumType is given.
   * @param unrecognizedChecksumType
   */
  public OzoneChecksumException(
      ContainerProtos.ChecksumType unrecognizedChecksumType) {
    super(String.format("Unrecognized ChecksumType: %s",
        unrecognizedChecksumType));
  }

  /**
   * OzoneChecksumException to wrap around NoSuchAlgorithmException.
   * @param algorithm name of algorithm
   * @param ex original exception thrown
   */
  public OzoneChecksumException(
      String algorithm, NoSuchAlgorithmException ex) {
    super(String.format("NoSuchAlgorithmException thrown while computing " +
        "SHA-256 checksum using algorithm %s", algorithm), ex);
  }

  /**
   * OzoneChecksumException to throw with custom message.
   */
  public OzoneChecksumException(String message) {
    super(message);
  }
}
