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

import java.io.*;
import java.util.*;

import org.apache.hadoop.classification.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.server.blockmanagement.*;
import org.apache.hadoop.security.AccessControlException;

public class NameNodeRaidTestUtil {
  public static void readLock(final FSDirectory dir) {
    dir.readLock();
  }

  public static void readUnLock(final FSDirectory dir) {
    dir.readUnlock();
  }

  public static FSInodeInfo getNode(final FSDirectory dir,
      final String src, final boolean resolveLink
      ) throws UnresolvedLinkException {
    return dir.rootDir.getNode(src, resolveLink);
  }

  public static NavigableMap<String, DatanodeDescriptor> getDatanodeMap(
      final FSNamesystem namesystem) {
    return namesystem.datanodeMap;
  }
}

