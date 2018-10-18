/**
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

package org.apache.hadoop.ozone.web.response;

/**
 * KeyLocation class is used used for parsing json response
 * when KeyInfoDetails Call is made.
 */
public class KeyLocation {
  /**
   * Which container this key stored.
   */
  private final long containerID;
  /**
   * Which block this key stored inside a container.
   */
  private final long localID;
  /**
   * Data length of this key replica.
   */
  private final long length;
  /**
   * Offset of this key.
   */
  private final long offset;

  /**
   * Constructs KeyLocation.
   */
  public KeyLocation(long containerID, long localID,
                     long length, long offset) {
    this.containerID = containerID;
    this.localID = localID;
    this.length = length;
    this.offset = offset;
  }

  /**
   * Returns the containerID of this Key.
   */
  public long getContainerID() {
    return containerID;
  }

  /**
   * Returns the localID of this Key.
   */
  public long getLocalID() {
    return localID;
  }

  /**
   * Returns the length of this Key.
   */
  public long getLength() {
    return length;
  }

  /**
   * Returns the offset of this Key.
   */
  public long getOffset() {
    return offset;
  }

}
