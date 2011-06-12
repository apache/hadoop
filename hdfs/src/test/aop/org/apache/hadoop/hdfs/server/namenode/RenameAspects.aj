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

import org.apache.commons.logging.*;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.TestFiRename;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;

/**
 * The aspects here are used for testing HDFS implementation of rename
 * functionality. Failure is introduced during rename to test the atomicity of
 * rename.
 */
public privileged aspect RenameAspects {
  public static final Log LOG = LogFactory.getLog(RenameAspects.class);

  /** When removeChild is called during rename, throw exception */
  pointcut callRemove(INode[] inodes, int pos) : 
    call(* FSDirectory.removeChild(INode[], int))
    && args(inodes, pos)
    && withincode (* FSDirectory.unprotectedRenameTo(String, 
        String, long, Rename...));

  before(INode[] inodes, int pos) throws RuntimeException :
    callRemove(inodes, pos) {
    LOG.info("FI: callRenameRemove");
    if (TestFiRename.throwExceptionOnRemove(inodes[pos].getLocalName())) {
      throw new RuntimeException("RenameAspects - on remove " + 
          inodes[pos].getLocalName());
    }
  }

  /** When addChildNoQuotaCheck is called during rename, throw exception */
  pointcut callAddChildNoQuotaCheck(INode[] inodes, int pos, INode node, long diskspace, boolean flag) :
    call(* FSDirectory.addChildNoQuotaCheck(INode[], int, INode, long, boolean)) 
    && args(inodes, pos, node, diskspace, flag)
    && withincode (* FSDirectory.unprotectedRenameTo(String, 
        String, long, Rename...));

  before(INode[] inodes, int pos, INode node, long diskspace, boolean flag)
      throws RuntimeException : 
      callAddChildNoQuotaCheck(inodes, pos, node, diskspace, flag) {
    LOG.info("FI: callAddChildNoQuotaCheck");
    if (TestFiRename.throwExceptionOnAdd(inodes[pos].getLocalName())) {
      throw new RuntimeException("RenameAspects on add " + 
          inodes[pos].getLocalName());
    }
  }
}
