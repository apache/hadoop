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

package org.apache.hadoop.fs.s3a.impl;

/**
 * Structure for options when putting/writing objects.
 * The init set of options is minimal, but as it will
 * be passed around, making it a structure makes it
 * easier to add more.
 */
public final class PutObjectOptions {

  /**
   * Can the operation skip marker deletion
   * afterwards?
   */
  private final boolean keepMarkers;

  public PutObjectOptions(final boolean keepMarkers) {
    this.keepMarkers = keepMarkers;
  }

  public boolean isKeepMarkers() {
    return keepMarkers;
  }

  @Override
  public String toString() {
    return "PutObjectOptions{" +
        "keepMarkers=" + keepMarkers +
        '}';
  }

  private static final PutObjectOptions KEEP_DIRS = new PutObjectOptions(true);
  private static final PutObjectOptions DELETE_DIRS = new PutObjectOptions(false);

  /**
   * Get the options to keep directories.
   * @return an instance which keeps dirs
   */
  public static PutObjectOptions keepingDirs() {
    return KEEP_DIRS;
  }

  /**
   * Get the options to delete directory markers.
   * @return an instance which deletes dirs
   */
  public static PutObjectOptions deletingDirs() {
    return DELETE_DIRS;
  }
}