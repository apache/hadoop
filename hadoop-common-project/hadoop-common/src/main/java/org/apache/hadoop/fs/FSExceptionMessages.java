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
   * The operation failed because the stream is closed: {@value}
   */
  public static final String STREAM_IS_CLOSED = "Stream is closed!";

  /**
   * Negative offset seek forbidden : {@value}
   */
  public static final String NEGATIVE_SEEK =
    "Cannot seek to a negative offset";

  /**
   * Seeks : {@value}
   */
  public static final String CANNOT_SEEK_PAST_EOF =
      "Attempted to seek or read past the end of the file";
}
