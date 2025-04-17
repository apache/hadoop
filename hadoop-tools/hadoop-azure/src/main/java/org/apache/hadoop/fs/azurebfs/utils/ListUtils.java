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
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;

import org.apache.hadoop.fs.FileStatus;

public class ListUtils {

  public static List<FileStatus> getUniqueListResult(List<FileStatus> originalList) {
    if (originalList == null || originalList.isEmpty()) {
      return originalList;
    }
    TreeMap<String, FileStatus> nameToEntryMap = new TreeMap<>();
    String prefix;
    Iterator<FileStatus> iterator = originalList.iterator();
    List<FileStatus> rectifiedFileStatusList = new ArrayList<>();

    FileStatus curr = iterator.next();
    prefix = curr.getPath().getName();
    addToUniqueResult(nameToEntryMap, rectifiedFileStatusList, curr);

    while (iterator.hasNext()) {
      FileStatus next = iterator.next();
      if (next.getPath().getName().startsWith(prefix)) {
        /*
         * This is either a duplicate entry or a duplicate entry might follow.
         * Keep adding unique entries to map and final list
         */
        if (!nameToEntryMap.containsKey(next.getPath().getName())) {
          addToUniqueResult(nameToEntryMap, rectifiedFileStatusList, next);
        }
      } else {
        // The prefix pattern breaks here.
        prefix = next.getPath().getName();
        nameToEntryMap = new TreeMap<>();
        addToUniqueResult(nameToEntryMap, rectifiedFileStatusList, next);
      }
    }

    return rectifiedFileStatusList;
  }

  private static void addToUniqueResult(TreeMap<String, FileStatus> nameToEntryMap,
      List<FileStatus> rectifiedFileStatusList, FileStatus fileStatus) {
    nameToEntryMap.put(fileStatus.getPath().getName(), fileStatus);
    rectifiedFileStatusList.add(fileStatus);
  }
}
