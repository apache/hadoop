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

package org.apache.hadoop.fs.tosfs.contract;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractBondedFSContract;
import org.apache.hadoop.fs.tosfs.TestEnv;
import org.apache.hadoop.fs.tosfs.util.TestUtility;
import org.apache.hadoop.fs.tosfs.util.UUIDUtils;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TosContract extends AbstractBondedFSContract {
  private static final Logger LOG = LoggerFactory.getLogger(TosContract.class);
  private final String testDir;

  public TosContract(Configuration conf) {
    super(conf);
    addConfResource("contract/tos.xml");
    // Set the correct contract test path if there is a provided bucket name from environment.
    if (StringUtils.isNoneEmpty(TestUtility.bucket())) {
      conf.set("fs.contract.test.fs.tos", String.format("tos://%s/", TestUtility.bucket()));
    }

    testDir = "/test-" + UUIDUtils.random();
  }

  @BeforeAll
  public static void before() {
    Assumptions.assumeTrue(TestEnv.checkTestEnabled());
  }

  @Override
  public String getScheme() {
    return "tos";
  }

  @Override
  public Path getTestPath() {
    LOG.info("the test dir is: {}", testDir);
    return new Path(testDir);
  }
}
