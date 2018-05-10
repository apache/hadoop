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

package org.apache.hadoop.fs.ozone;

/**
 * Constants for Ozone FileSystem implementation.
 */
public final class Constants {

  public static final String OZONE_DEFAULT_USER = "hdfs";

  public static final String OZONE_USER_DIR = "/user";

  /** Local buffer directory. */
  public static final String BUFFER_DIR_KEY = "fs.ozone.buffer.dir";

  /** Temporary directory. */
  public static final String BUFFER_TMP_KEY = "hadoop.tmp.dir";

  /** Page size for Ozone listing operation. */
  public static final int LISTING_PAGE_SIZE = 1024;

  private Constants() {

  }
}
