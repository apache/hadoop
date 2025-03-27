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

import org.apache.hadoop.fs.compat.common.AbstractHdfsCompatCase;
import org.apache.hadoop.fs.compat.common.HdfsCompatCase;
import org.apache.hadoop.fs.compat.common.HdfsCompatUtil;

import java.util.ArrayList;

public class HdfsCompatAclTestCases extends AbstractHdfsCompatCase {
  @HdfsCompatCase
  public void modifyAclEntries() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().modifyAclEntries(makePath("modifyAclEntries"), new ArrayList<>())
    );
  }

  @HdfsCompatCase
  public void removeAclEntries() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().removeAclEntries(makePath("removeAclEntries"), new ArrayList<>())
    );
  }

  @HdfsCompatCase
  public void removeDefaultAcl() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().removeDefaultAcl(makePath("removeDefaultAcl"))
    );
  }

  @HdfsCompatCase
  public void removeAcl() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().removeAcl(makePath("removeAcl"))
    );
  }

  @HdfsCompatCase
  public void setAcl() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().setAcl(makePath("setAcl"), new ArrayList<>())
    );
  }

  @HdfsCompatCase
  public void getAclStatus() {
    HdfsCompatUtil.checkImplementation(() ->
        fs().getAclStatus(makePath("getAclStatus"))
    );
  }
}