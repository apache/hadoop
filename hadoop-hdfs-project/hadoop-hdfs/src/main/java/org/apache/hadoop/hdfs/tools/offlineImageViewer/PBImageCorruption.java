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
package org.apache.hadoop.hdfs.tools.offlineImageViewer;

import java.util.EnumSet;

/**
 * Class representing a corruption in the PBImageCorruptionDetector processor.
 */
public class PBImageCorruption {
  private static final String WITH = "With";

  /**
   * PBImageCorruptionType is a wrapper for getting a string output for
   * different types of corruption. Could be added more cases if
   * other types are revealed. Currently hasMissingChild and
   * isCorruptNode are the relevant cases.
   */
  private enum PBImageCorruptionType {
    CORRUPT_NODE("CorruptNode"),
    MISSING_CHILD("MissingChild");

    private final String name;

    PBImageCorruptionType(String s) {
      name = s;
    }

    @Override
    public String toString() {
      return this.name;
    }
  }

  private long id;
  private EnumSet<PBImageCorruptionType> type;
  private int numOfCorruptChildren;

  PBImageCorruption(long id, boolean missingChild, boolean corruptNode,
                    int numOfCorruptChildren) {
    if (!missingChild && !corruptNode) {
      throw new IllegalArgumentException(
          "Corruption must have at least one aspect!");
    }
    this.id = id;
    this.type = EnumSet.noneOf(PBImageCorruptionType.class);
    if (missingChild) {
      type.add(PBImageCorruptionType.MISSING_CHILD);
    }
    if (corruptNode) {
      type.add(PBImageCorruptionType.CORRUPT_NODE);
    }
    this.numOfCorruptChildren = numOfCorruptChildren;
  }

  void addMissingChildCorruption() {
    type.add(PBImageCorruptionType.MISSING_CHILD);
  }

  void addCorruptNodeCorruption() {
    type.add(PBImageCorruptionType.CORRUPT_NODE);
  }

  void setNumberOfCorruption(int numOfCorruption) {
    this.numOfCorruptChildren = numOfCorruption;
  }

  long getId() {
    return id;
  }

  String getType() {
    StringBuffer s = new StringBuffer();
    if (type.contains(PBImageCorruptionType.CORRUPT_NODE)) {
      s.append(PBImageCorruptionType.CORRUPT_NODE);
    }
    if (type.contains(PBImageCorruptionType.CORRUPT_NODE) &&
        type.contains(PBImageCorruptionType.MISSING_CHILD)) {
      s.append(WITH);
    }

    if (type.contains(PBImageCorruptionType.MISSING_CHILD)) {
      s.append(PBImageCorruptionType.MISSING_CHILD);
    }
    return s.toString();
  }

  int getNumOfCorruptChildren() {
    return numOfCorruptChildren;
  }

}
