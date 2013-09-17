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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;

import org.apache.commons.logging.*;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;

/**
 * The aspects here are used for testing HDFS implementation of iterative
 * directory listing functionality. A directory is deleted right after
 * the first listPath RPC. 
 */
public privileged aspect ListPathAspects {
  public static final Log LOG = LogFactory.getLog(ListPathAspects.class);

  pointcut callGetListing(FSNamesystem fd, String src,
                          byte[] startAfter, boolean needLocation) : 
    call(DirectoryListing FSNamesystem.getListing(String, byte[], boolean))
    && target(fd)
    && args(src, startAfter, needLocation);

  after(FSNamesystem fd, String src, byte[] startAfter, boolean needLocation) 
    throws IOException, UnresolvedLinkException: 
      callGetListing(fd, src, startAfter, needLocation) {
    LOG.info("FI: callGetListing");
    fd.delete(src, true);
  }
}