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

package org.apache.hadoop.fs.azurebfs.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import org.apache.hadoop.fs.FileStatus;

/**
 * Utility class for List operations.
 */
public final class ListUtils {

  private ListUtils() {
    // Private constructor to prevent instantiation
  }

  /**
   * Utility method to remove duplicates from a list of FileStatus.
   * ListBlob API of blob endpoint can return duplicate entries.
   * @param originalList prone to have duplicates
   * @return rectified list with no duplicates.
   */
  public static List<FileStatus> getUniqueListResult(List<FileStatus> originalList) {
    if (originalList == null || originalList.isEmpty()) {
      return originalList;
    }

    TreeMap<String, FileStatus> nameToEntryMap = new TreeMap<>();
    String prefix = null;
    List<FileStatus> rectifiedFileStatusList = new ArrayList<>();

    for (FileStatus current : originalList) {
      String fileName = current.getPath().getName();

      if (prefix == null || !fileName.startsWith(prefix)) {
        // Prefix pattern breaks here. Reset Map and prefix.
        prefix = fileName;
        nameToEntryMap.clear();
      }

      // Add the current entry if it is not already added.
      if (!nameToEntryMap.containsKey(fileName)) {
        nameToEntryMap.put(fileName, current);
        rectifiedFileStatusList.add(current);
      }
    }

    return rectifiedFileStatusList;
  }
}
