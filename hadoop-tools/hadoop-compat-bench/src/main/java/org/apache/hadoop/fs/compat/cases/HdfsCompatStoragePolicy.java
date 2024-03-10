/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.compat.cases;

import org.apache.hadoop.fs.BlockStoragePolicySpi;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.compat.common.*;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

@HdfsCompatCaseGroup(name = "StoragePolicy")
public class HdfsCompatStoragePolicy extends AbstractHdfsCompatCase {
  private static final Logger LOG =
      LoggerFactory.getLogger(HdfsCompatStoragePolicy.class);
  private static final Random RANDOM = new Random();
  private Path dir;
  private Path file;
  private String[] policies;
  private String defaultPolicyName;
  private String policyName;

  @HdfsCompatCaseSetUp
  public void setUp() throws IOException {
    policies = getStoragePolicyNames();
  }

  @HdfsCompatCasePrepare
  public void prepare() throws IOException {
    this.dir = makePath("dir");
    this.file = new Path(this.dir, "file");
    HdfsCompatUtil.createFile(fs(), file, 0);

    BlockStoragePolicySpi policy = fs().getStoragePolicy(this.dir);
    this.defaultPolicyName = (policy == null) ? null : policy.getName();

    List<String> differentPolicies = new ArrayList<>();
    for (String name : policies) {
      if (!name.equalsIgnoreCase(defaultPolicyName)) {
        differentPolicies.add(name);
      }
    }
    if (differentPolicies.isEmpty()) {
      LOG.warn("There is only one storage policy: " +
          (defaultPolicyName == null ? "null" : defaultPolicyName));
      this.policyName = defaultPolicyName;
    } else {
      this.policyName = differentPolicies.get(
          RANDOM.nextInt(differentPolicies.size()));
    }
  }

  @HdfsCompatCaseCleanup
  public void cleanup() {
    HdfsCompatUtil.deleteQuietly(fs(), this.dir, true);
  }

  @HdfsCompatCase
  public void setStoragePolicy() throws IOException {
    fs().setStoragePolicy(dir, policyName);
    BlockStoragePolicySpi policy = fs().getStoragePolicy(dir);
    Assert.assertEquals(policyName, policy.getName());
  }

  @HdfsCompatCase
  public void unsetStoragePolicy() throws IOException {
    fs().setStoragePolicy(dir, policyName);
    fs().unsetStoragePolicy(dir);
    BlockStoragePolicySpi policy = fs().getStoragePolicy(dir);
    String policyNameAfterUnset = (policy == null) ? null : policy.getName();
    Assert.assertEquals(defaultPolicyName, policyNameAfterUnset);
  }

  @HdfsCompatCase(ifDef = "org.apache.hadoop.fs.FileSystem#satisfyStoragePolicy")
  public void satisfyStoragePolicy() throws IOException {
    fs().setStoragePolicy(dir, policyName);
    fs().satisfyStoragePolicy(dir);
  }

  @HdfsCompatCase
  public void getStoragePolicy() throws IOException {
    BlockStoragePolicySpi policy = fs().getStoragePolicy(file);
    String initialPolicyName = (policy == null) ? null : policy.getName();
    Assert.assertEquals(defaultPolicyName, initialPolicyName);
  }
}