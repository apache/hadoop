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

import org.apache.hadoop.fs.UnresolvedLinkException;

public class NameNodeRaidTestUtil {
  public static FSInodeInfo[] getFSInodeInfo(final FSNamesystem namesystem,
      final String... files) throws UnresolvedLinkException {
    final FSInodeInfo[] inodes = new FSInodeInfo[files.length];
    final FSDirectory dir = namesystem.dir; 
    dir.readLock();
    try {
      for(int i = 0; i < files.length; i++) {
        inodes[i] = dir.rootDir.getNode(files[i], true);
      }
      return inodes;
    } finally {
      dir.readUnlock();
    }
  }
}
