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


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.compat.common.*;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.junit.Assert;

import java.io.IOException;
import java.util.List;

@HdfsCompatCaseGroup(name = "ACL")
public class HdfsCompatAcl extends AbstractHdfsCompatCase {
  private static final String INIT_FILE_ACL =
      "user::rwx,group::rwx,other::rwx,user:foo:rwx";
  private static final String INIT_DIR_ACL =
      "default:user::rwx,default:group::rwx,default:other::rwx";
  private Path dir;
  private Path file;

  @HdfsCompatCasePrepare
  public void prepare() throws IOException {
    this.dir = makePath("dir");
    this.file = new Path(this.dir, "file");
    HdfsCompatUtil.createFile(fs(), this.file, 0);
    List<AclEntry> entries = AclEntry.parseAclSpec(INIT_DIR_ACL, true);
    fs().setAcl(dir, entries);
    entries = AclEntry.parseAclSpec(INIT_FILE_ACL, true);
    fs().setAcl(file, entries);
  }

  @HdfsCompatCaseCleanup
  public void cleanup() throws IOException {
    HdfsCompatUtil.deleteQuietly(fs(), this.dir, true);
  }

  @HdfsCompatCase
  public void modifyAclEntries() throws IOException {
    List<AclEntry> entries = AclEntry.parseAclSpec("user:foo:---", true);
    fs().modifyAclEntries(file, entries);
    List<AclEntry> acls = fs().getAclStatus(file).getEntries();
    long count = 0;
    for (AclEntry acl : acls) {
      if ("foo".equals(acl.getName())) {
        ++count;
        Assert.assertEquals(FsAction.NONE, acl.getPermission());
      }
    }
    Assert.assertEquals(1, count);
  }

  @HdfsCompatCase
  public void removeAclEntries() throws IOException {
    List<AclEntry> entries = AclEntry.parseAclSpec("user:bar:---", true);
    fs().modifyAclEntries(file, entries);
    entries = AclEntry.parseAclSpec("user:foo:---", true);
    fs().removeAclEntries(file, entries);
    List<AclEntry> acls = fs().getAclStatus(file).getEntries();
    Assert.assertTrue(acls.stream().noneMatch(e -> "foo".equals(e.getName())));
    Assert.assertTrue(acls.stream().anyMatch(e -> "bar".equals(e.getName())));
  }

  @HdfsCompatCase
  public void removeDefaultAcl() throws IOException {
    fs().removeDefaultAcl(dir);
    List<AclEntry> acls = fs().getAclStatus(dir).getEntries();
    Assert.assertTrue(acls.stream().noneMatch(
        e -> (e.getScope() == AclEntryScope.DEFAULT)));
  }

  @HdfsCompatCase
  public void removeAcl() throws IOException {
    fs().removeAcl(file);
    List<AclEntry> acls = fs().getAclStatus(file).getEntries();
    Assert.assertTrue(acls.stream().noneMatch(e -> "foo".equals(e.getName())));
  }

  @HdfsCompatCase
  public void setAcl() throws IOException {
    List<AclEntry> acls = fs().getAclStatus(file).getEntries();
    Assert.assertTrue(acls.stream().anyMatch(e -> "foo".equals(e.getName())));
  }

  @HdfsCompatCase
  public void getAclStatus() throws IOException {
    AclStatus status = fs().getAclStatus(dir);
    Assert.assertFalse(status.getOwner().isEmpty());
    Assert.assertFalse(status.getGroup().isEmpty());
    List<AclEntry> acls = status.getEntries();
    Assert.assertTrue(acls.stream().anyMatch(e ->
        e.getScope() == AclEntryScope.DEFAULT));

    status = fs().getAclStatus(file);
    Assert.assertFalse(status.getOwner().isEmpty());
    Assert.assertFalse(status.getGroup().isEmpty());
    acls = status.getEntries();
    Assert.assertTrue(acls.stream().anyMatch(e ->
        e.getScope() == AclEntryScope.ACCESS));
  }
}