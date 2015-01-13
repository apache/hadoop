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
package org.apache.hadoop.hdfs.protocol.datatransfer.sasl;

import static org.apache.hadoop.hdfs.protocol.datatransfer.sasl.DataTransferSaslUtil.SASL_TRANSFER_MAGIC_NUMBER;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Indicates that SASL protocol negotiation expected to read a pre-defined magic
 * number, but the expected value was not seen.
 */
@InterfaceAudience.Private
public class InvalidMagicNumberException extends IOException {

  private static final long serialVersionUID = 1L;
  private final boolean handshake4Encryption;

  /**
   * Creates a new InvalidMagicNumberException.
   *
   * @param magicNumber expected value
   */
  public InvalidMagicNumberException(final int magicNumber, 
      final boolean handshake4Encryption) {
    super(String.format("Received %x instead of %x from client.",
        magicNumber, SASL_TRANSFER_MAGIC_NUMBER));
    this.handshake4Encryption = handshake4Encryption;
  }
  
  /**
   * Return true if it's handshake for encryption
   * 
   * @return boolean true if it's handshake for encryption
   */
  public boolean isHandshake4Encryption() {
    return handshake4Encryption;
  }
}
