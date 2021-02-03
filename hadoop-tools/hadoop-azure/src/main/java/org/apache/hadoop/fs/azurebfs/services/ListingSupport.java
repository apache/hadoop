/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface ListingSupport {

  /**
   * @param path The list path.
   * @return the entries in the path.
   * @throws IOException in case of error
   */
  FileStatus[] listStatus(Path path) throws IOException;

  /**
   * @param path      Path the list path.
   * @param startFrom The entry name that list results should start with.
   *                  For example, if folder "/folder" contains four
   *                  files: "afile", "bfile", "hfile", "ifile". Then
   *                  listStatus(Path("/folder"), "hfile") will return
   *                  "/folder/hfile" and "folder/ifile" Notice that if
   *                  startFrom is a non-existent entry name, then the
   *                  list response contains all entries after this
   *                  non-existent entry in lexical order: listStatus
   *                  (Path("/folder"), "cfile") will return
   *                  "/folder/hfile" and "/folder/ifile".
   * @return the entries in the path start from  "startFrom" in lexical order.
   * @throws IOException in case of error
   */
  FileStatus[] listStatus(Path path, String startFrom) throws IOException;

  /**
   * @param path         The list path
   * @param startFrom    The entry name that list results should start with.
   *                     For example, if folder "/folder" contains four
   *                     files: "afile", "bfile", "hfile", "ifile". Then
   *                     listStatus(Path("/folder"), "hfile") will return
   *                     "/folder/hfile" and "folder/ifile" Notice that if
   *                     startFrom is a non-existent entry name, then the
   *                     list response contains all entries after this
   *                     non-existent entry in lexical order: listStatus
   *                     (Path("/folder"), "cfile") will return
   *                     "/folder/hfile" and "/folder/ifile".
   * @param fileStatuses This list has to be filled with the FileStatus objects
   * @param fetchAll     flag to indicate if the above list needs to be
   *                     filled with just one page os results or the entire
   *                     result.
   * @param continuation Contiuation token. null means start rom the begining.
   * @return Continuation tokem
   * @throws IOException in case of error
   */
  String listStatus(Path path, String startFrom, List<FileStatus> fileStatuses,
      boolean fetchAll, String continuation) throws IOException;
}
