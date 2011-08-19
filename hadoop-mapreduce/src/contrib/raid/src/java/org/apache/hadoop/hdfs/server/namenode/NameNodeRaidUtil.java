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

import org.apache.hadoop.classification.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.security.AccessControlException;

/** Utilities used by RAID for accessing NameNode. */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class NameNodeRaidUtil {
  /** Accessing FSDirectory.getFileInfo(..) */
  public static HdfsFileStatus getFileInfo(final FSDirectory dir,
      final String src, final boolean resolveLink
      ) throws UnresolvedLinkException {
    return dir.getFileInfo(src, resolveLink);
  }

  /** Accessing FSNamesystem.getFileInfo(..) */
  public static HdfsFileStatus getFileInfo(final FSNamesystem namesystem,
      final String src, final boolean resolveLink
      ) throws AccessControlException, UnresolvedLinkException {
    return namesystem.getFileInfo(src, resolveLink);
  }

  /** Accessing FSNamesystem.getBlockLocations(..) */
  public static LocatedBlocks getBlockLocations(final FSNamesystem namesystem,
      final String src, final long offset, final long length,
      final boolean doAccessTime, final boolean needBlockToken
      ) throws FileNotFoundException, UnresolvedLinkException, IOException {
    return namesystem.getBlockLocations(src, offset, length,
        doAccessTime, needBlockToken);
  }
}

