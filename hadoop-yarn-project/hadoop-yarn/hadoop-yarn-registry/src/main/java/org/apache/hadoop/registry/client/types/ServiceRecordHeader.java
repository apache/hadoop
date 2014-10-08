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

package org.apache.hadoop.registry.client.types;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Service record header; access to the byte array kept private
 * to avoid findbugs warnings of mutability
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class ServiceRecordHeader {
  /**
   * Header of a service record:  "jsonservicerec"
   * By making this over 12 bytes long, we can auto-determine which entries
   * in a listing are too short to contain a record without getting their data
   */
  private static final byte[] RECORD_HEADER = {
      'j', 's', 'o', 'n',
      's', 'e', 'r', 'v', 'i', 'c', 'e',
      'r', 'e', 'c'
  };

  /**
   * Get the length of the record header
   * @return the header length
   */
  public static int getLength() {
    return RECORD_HEADER.length;
  }

  /**
   * Get a clone of the record header
   * @return the new record header.
   */
  public static byte[] getData() {
    byte[] h = new byte[RECORD_HEADER.length];
    System.arraycopy(RECORD_HEADER, 0, h, 0, RECORD_HEADER.length);
    return h;
  }
}
