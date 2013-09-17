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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Opaque interface that identifies a disk location. Subclasses
 * should implement {@link Comparable} and override both equals and hashCode.
 */
@InterfaceStability.Unstable
@InterfaceAudience.Public
public interface VolumeId extends Comparable<VolumeId> {

  /**
   * Represents an invalid Volume ID (ID for unknown content).
   */
  public static final VolumeId INVALID_VOLUME_ID = new VolumeId() {
    
    @Override
    public int compareTo(VolumeId arg0) {
      // This object is equal only to itself;
      // It is greater than null, and
      // is always less than any other VolumeId:
      if (arg0 == null) {
        return 1;
      }
      if (arg0 == this) {
        return 0;
      } else {
        return -1;
      }
    }
    
    @Override
    public boolean equals(Object obj) {
      // this object is equal only to itself:
      return (obj == this);
    }
    
    @Override
    public int hashCode() {
      return Integer.MIN_VALUE;
    }
    
    @Override
    public boolean isValid() {
      return false;
    }
    
    @Override
    public String toString() {
      return "Invalid VolumeId";
    }
  };
  
  /**
   * Indicates if the disk identifier is valid. Invalid identifiers indicate
   * that the block was not present, or the location could otherwise not be
   * determined.
   * 
   * @return true if the disk identifier is valid
   */
  public boolean isValid();

  @Override
  abstract public int compareTo(VolumeId arg0);

  @Override
  abstract public int hashCode();

  @Override
  abstract public boolean equals(Object obj);

}
