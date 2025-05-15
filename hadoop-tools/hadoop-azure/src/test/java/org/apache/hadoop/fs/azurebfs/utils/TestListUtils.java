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

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

/**
 * Test class for ListUtils.
 */
public class TestListUtils {

  /**
   * Test method to check the removal of duplicates from a list of FileStatus.
   */
  @Test
  public void testRemoveDuplicates() {
    List<FileStatus> originalList = new ArrayList<>();
    validateList(originalList, 0);

    originalList = new ArrayList<>();
    originalList.add(getFileStatusObject(new Path("/A")));
    validateList(originalList, 1);

    originalList = new ArrayList<>();
    originalList.add(getFileStatusObject(new Path("/A")));
    originalList.add(getFileStatusObject(new Path("/A")));
    validateList(originalList, 1);

    originalList = new ArrayList<>();
    originalList.add(getFileStatusObject(new Path("/a")));
    originalList.add(getFileStatusObject(new Path("/a.bak1")));
    originalList.add(getFileStatusObject(new Path("/a.bak1.bak2")));
    originalList.add(getFileStatusObject(new Path("/a.bak1.bak2")));
    originalList.add(getFileStatusObject(new Path("/a.bak1")));
    originalList.add(getFileStatusObject(new Path("/a")));
    originalList.add(getFileStatusObject(new Path("/abc")));
    originalList.add(getFileStatusObject(new Path("/abc.bak1")));
    originalList.add(getFileStatusObject(new Path("/abc")));
    validateList(originalList, 5);

    originalList = new ArrayList<>();
    originalList.add(getFileStatusObject(new Path("/a")));
    originalList.add(getFileStatusObject(new Path("/a")));
    originalList.add(getFileStatusObject(new Path("/a_bak1")));
    originalList.add(getFileStatusObject(new Path("/a_bak1")));
    originalList.add(getFileStatusObject(new Path("/a_bak1_bak2")));
    originalList.add(getFileStatusObject(new Path("/a_bak1_bak2")));
    originalList.add(getFileStatusObject(new Path("/abc")));
    originalList.add(getFileStatusObject(new Path("/abc")));
    originalList.add(getFileStatusObject(new Path("/abc_bak1")));
    validateList(originalList, 5);

    originalList = new ArrayList<>();
    originalList.add(getFileStatusObject(new Path("/a")));
    originalList.add(getFileStatusObject(new Path("/b")));
    validateList(originalList, 2);

    originalList = new ArrayList<>();
    originalList.add(getFileStatusObject(new Path("/a")));
    originalList.add(getFileStatusObject(new Path("/b")));
    originalList.add(getFileStatusObject(new Path("/b")));
    validateList(originalList, 2);
  }

  /**
   * Validate the size of the list after removing duplicates.
   * @param originalList list having duplicates
   * @param expectedSize number of unique entries expected
   */
  private void validateList(List<FileStatus> originalList, int expectedSize) {
    List<FileStatus> uniqueList = ListUtils.getUniqueListResult(originalList);
    Assertions.assertThat(uniqueList)
        .describedAs("List Size is not as expected after duplicate removal")
        .hasSize(expectedSize);
  }

  /**
   * Create a FileStatus object with the given path.
   * @param path path to be set in the FileStatus object
   * @return FileStatus object with the given path
   */
  private FileStatus getFileStatusObject(Path path) {
    FileStatus status = new FileStatus();
    status.setPath(path);
    return status;
  }
}
