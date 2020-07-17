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

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.Test;

public class TestFsImageValidation {
  static {
    FsImageValidation.Util.setLogLevel(FsImageValidation.class, Level.TRACE);
    FsImageValidation.Util.setLogLevel(INodeReferenceValidation.class, Level.TRACE);
    FsImageValidation.Util.setLogLevel(INode.class, Level.TRACE);
  }

  /**
   * Run validation as a unit test.
   * The path of the fsimage file being tested is specified
   * by the environment variable FS_IMAGE_FILE.
   */
  @Test
  public void testINodeReference() throws Exception {
    FsImageValidation.initLogLevels();

    try {
      final FsImageValidation validation = FsImageValidation.newInstance();
      final int errorCount = validation.checkINodeReference(new Configuration());
      Assert.assertEquals("Error Count: " + errorCount, 0, errorCount);
    } catch (HadoopIllegalArgumentException e) {
      FsImageValidation.Cli.printError("The environment variable "
          + FsImageValidation.FS_IMAGE + " is not set.", e);
    }
  }
}