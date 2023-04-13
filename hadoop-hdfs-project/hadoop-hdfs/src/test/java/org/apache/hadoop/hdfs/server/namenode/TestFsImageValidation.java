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
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

public class TestFsImageValidation {
  static final Logger LOG = LoggerFactory.getLogger(
      TestFsImageValidation.class);

  static {
    final Level t = Level.TRACE;
    GenericTestUtils.setLogLevel(
        LoggerFactory.getLogger(FsImageValidation.class), t);
    GenericTestUtils.setLogLevel(
        LoggerFactory.getLogger(INodeReferenceValidation.class), t);
    GenericTestUtils.setLogLevel(LoggerFactory.getLogger(INode.class), t);
  }

  /**
   * Run validation as a unit test.
   * The path of the fsimage file being tested is specified
   * by the environment variable FS_IMAGE_FILE.
   */
  @Test
  public void testValidation() throws Exception {
    FsImageValidation.initLogLevels();

    try {
      final int errorCount = FsImageValidation.newInstance().run();
      Assert.assertEquals("Error Count: " + errorCount, 0, errorCount);
    } catch (HadoopIllegalArgumentException e) {
      LOG.warn("The environment variable " + FsImageValidation.FS_IMAGE
          + " is not set", e);
    }
  }

  @Test
  public void testHaConf() {
    final Configuration conf = new Configuration();
    final String nsId = "cluster0";
    FsImageValidation.setHaConf(nsId, conf);
    Assert.assertTrue(HAUtil.isHAEnabled(conf, nsId));
  }

  @Test
  public void testToCommaSeparatedNumber() {
    for(long b = 1; b < Integer.MAX_VALUE;) {
      for (long n = b; n < Integer.MAX_VALUE; n *= 10) {
        runTestToCommaSeparatedNumber(n);
      }
      b = b == 1? 11: 10*(b-1) + 1;
    }
  }

  static void runTestToCommaSeparatedNumber(long n) {
    final String s = FsImageValidation.Util.toCommaSeparatedNumber(n);
    LOG.info("{} ?= {}", n, s);
    for(int i = s.length(); i > 0;) {
      for(int j = 0; j < 3 && i > 0; j++) {
        Assert.assertTrue(Character.isDigit(s.charAt(--i)));
      }
      if (i > 0) {
        Assert.assertEquals(',', s.charAt(--i));
      }
    }

    Assert.assertNotEquals(0, s.length()%4);
    Assert.assertEquals(n, Long.parseLong(s.replaceAll(",", "")));
  }
}