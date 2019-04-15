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

package org.apache.hadoop.fs;

/**
 * Standard strings to use in exception messages in filesystems
 * HDFS is used as the reference source of the strings
 */
public class FSExceptionMessages {

  /**
   * The operation failed because the stream is closed: {@value}.
   */
  public static final String STREAM_IS_CLOSED = "Stream is closed!";

  /**
   * Negative offset seek forbidden : {@value}.
   */
  public static final String NEGATIVE_SEEK =
    "Cannot seek to a negative offset";

  /**
   * Seeks : {@value}.
   */
  public static final String CANNOT_SEEK_PAST_EOF =
      "Attempted to seek or read past the end of the file";

  public static final String EOF_IN_READ_FULLY =
      "End of file reached before reading fully.";

  public static final String TOO_MANY_BYTES_FOR_DEST_BUFFER
      = "Requested more bytes than destination buffer size";

  public static final String PERMISSION_DENIED = "Permission denied";

  public static final String PERMISSION_DENIED_BY_STICKY_BIT =
      "Permission denied by sticky bit";

  /**
   * A call was made to abort(), but it is not supported.
   */
  public static final String ABORTABLE_UNSUPPORTED =
      "Abortable.abort() is not supported";

  /**
   * Renaming a destination under source is forbidden.
   * <p></p>
   * {@value}.
   */
  public static final String RENAME_DEST_UNDER_SOURCE =
      "Rename destination %s is a directory or file under source %s";

  /**
   * Renaming a destination to source is forbidden.
   * <p></p>
   * {@value}.
   */
  public static final String RENAME_DEST_EQUALS_SOURCE =
      "The source %s and destination %s are the same";

  /**
   * Renaming to root is forbidden.
   * <p></p>
   * {@value}.
   */
  public static final String RENAME_DEST_IS_ROOT =
      "Rename destination cannot be the root path \"/\"";

  /**
   * The parent of a rename destination is not found.
   * <p></p>
   * {@value}.
   */
  public static final String RENAME_DEST_NO_PARENT_OF =
      "Rename destination parent of %s not found";

  /**
   * The parent of a rename destination is not found.
   * This is a format string, taking the parent path of the destination
   * <p></p>
   * {@value}.
   */
  public static final String RENAME_DEST_NO_PARENT =
      "Rename destination parent %s not found";

  /**
   * The parent of a rename destination is not a directory.
   * <p></p>
   * {@value}.
   */
  public static final String RENAME_DEST_PARENT_NOT_DIRECTORY =
      "Rename destination parent %s is a file";

  /**
   * The rename destination is not an empty directory.
   * <p></p>
   * {@value}.
   */
  public static final String RENAME_DEST_NOT_EMPTY =
      "Rename destination directory is not empty: %s";

  /**
   * The rename destination is not an empty directory.
   * <p></p>
   * {@value}.
   */
  public static final String RENAME_DEST_EXISTS =
      "Rename destination %s already exists";

  /**
   * The rename source doesn't exist.
   * <p></p>
   * {@value}.
   */
  public static final String RENAME_SOURCE_NOT_FOUND =
      "Rename source %s is not found";

  /**
   * The rename source and dest are of different types.
   * <p></p>
   * {@value}.
   */
  public static final String RENAME_SOURCE_DEST_DIFFERENT_TYPE =
      "Rename source %s and destination %s are of different types";

  /**
   * The rename source doesn't exist.
   * <p></p>
   * {@value}.
   */
  public static final String RENAME_SOURCE_IS_ROOT =
      "Rename source cannot be the root";

  /**
   * The rename failed for an unknown reason.
   * <p></p>
   * {@value}.
   */
  public static final String RENAME_FAILED = "Rename from %s to %s failed";

}
