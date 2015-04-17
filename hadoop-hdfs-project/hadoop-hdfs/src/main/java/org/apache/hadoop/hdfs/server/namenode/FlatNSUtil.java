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
package org.apache.hadoop.hdfs.server.namenode;

import com.google.common.base.Preconditions;

public class FlatNSUtil {
  public static String getNextComponent(String src, int offset) {
    if (offset >= src.length()) {
      return null;
    }

    assert src.charAt(offset) == '/';

    int next = src.indexOf('/', offset + 1);
    if (next == -1) {
      next = src.length();
    }
    return src.substring(offset + 1, next);
  }

  public static boolean hasNextLevelInPath(String src, int offset) {
    return src.indexOf('/', offset + 1) != -1;
  }

  public static String getLastComponent(String src) {
    int next = src.lastIndexOf('/');
    Preconditions.checkState(next != -1);
    return getNextComponent(src, next);
  }
}
