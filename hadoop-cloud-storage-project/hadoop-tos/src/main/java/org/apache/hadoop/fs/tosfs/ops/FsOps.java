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

package org.apache.hadoop.fs.tosfs.ops;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.tosfs.RawFileStatus;

import java.io.IOException;
import java.util.function.Predicate;

public interface FsOps {

  /**
   * Rename file from source path to dest path.
   *
   * @param src    the source path.
   * @param dst    the dest path.
   * @param length the length of source file.
   * @throws IOException if any io error happen.
   */
  void renameFile(Path src, Path dst, long length) throws IOException;

  /**
   * Rename dir from source path to dest path.
   *
   * @param src the source path.
   * @param dst the dest path.
   * @throws IOException if any io error happen.
   */
  void renameDir(Path src, Path dst) throws IOException;

  /**
   * Delete the given file.
   *
   * @param file the given file path.
   * @throws IOException if any io error happen.
   */
  void deleteFile(Path file) throws IOException;

  /**
   * Delete the given dir.
   *
   * @param dir       the given dir path.
   * @param recursive indicate whether delete dir recursively.
   * @throws IOException if any io error happen.
   */
  void deleteDir(Path dir, boolean recursive) throws IOException;

  /**
   * List the sub dirs and files with given dir.
   * Return empty collection if the path doesn't exist, or is a file, or is an empty dir.
   *
   * @param dir        the listed path.
   * @param recursive  indicated whether list all sub dirs/files or not.
   * @param postFilter filter the result after getting listing response.
   * @return the status of sub dirs and files.
   */
  Iterable<RawFileStatus> listDir(Path dir, boolean recursive, Predicate<String> postFilter);

  /**
   * Check is the input dir empty.
   *
   * @param dir the dir to check.
   * @return true if path don't have any children.
   */
  boolean isEmptyDirectory(Path dir);

  /**
   * Create dir and parent dirs if don't exist.
   *
   * @param dir the dir to be created.
   * @throws IOException if any io error happen.
   */
  void mkdirs(Path dir) throws IOException;
}
