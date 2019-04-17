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

package org.apache.hadoop.ozone.s3.util;

/**
 * Ranger Header class which hold startoffset, endoffset of the Range header
 * value provided as part of get object.
 *
 */
public class RangeHeader {
  private long startOffset;
  private long endOffset;
  private boolean readFull;
  private boolean inValidRange;


  /**
   * Construct RangeHeader object.
   * @param startOffset
   * @param endOffset
   */
  public RangeHeader(long startOffset, long endOffset, boolean full,
                     boolean invalid) {
    this.startOffset = startOffset;
    this.endOffset = endOffset;
    this.readFull = full;
    this.inValidRange = invalid;
  }

  /**
   * Return startOffset.
   *
   * @return startOffset
   */
  public long getStartOffset() {
    return startOffset;
  }

  /**
   * Return endoffset.
   *
   * @return endoffset
   */
  public long getEndOffset() {
    return endOffset;
  }

  /**
   * Return a flag whether after parsing range header, when the provided
   * values are with in a range, and whole file read is required.
   *
   * @return readFull
   */
  public boolean isReadFull() {
    return readFull;
  }

  /**
   * Return a flag, whether range header values are correct or not.
   *
   * @return isInValidRange
   */
  public boolean isInValidRange() {
    return inValidRange;
  }


  public String toString() {
    return "startOffset - [" + startOffset + "]" + "endOffset - [" +
        endOffset + "]" + " readFull - [ " + readFull + "]" + " invalidRange " +
        "- [ " + inValidRange + "]";
  }
}
