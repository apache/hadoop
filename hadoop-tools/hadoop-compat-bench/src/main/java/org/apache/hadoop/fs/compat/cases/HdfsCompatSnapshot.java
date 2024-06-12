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
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

@HdfsCompatCaseGroup(name = "Snapshot")
public class HdfsCompatSnapshot extends AbstractHdfsCompatCase {
  private static final Logger LOG = LoggerFactory.getLogger(HdfsCompatSnapshot.class);
  private final String snapshotName = "s-name";
  private final String fileName = "file";
  private Path base;
  private Path dir;
  private Path snapshot;
  private Method allow;
  private Method disallow;

  private static Path getSnapshotPath(Path path, String snapshotName) {
    return new Path(path, ".snapshot/" + snapshotName);
  }

  @HdfsCompatCaseSetUp
  public void setUp() throws Exception {
    this.base = getUniquePath();
    fs().mkdirs(this.base);
    try {
      Method allowSnapshotMethod = fs().getClass()
          .getMethod("allowSnapshot", Path.class);
      allowSnapshotMethod.setAccessible(true);
      allowSnapshotMethod.invoke(fs(), this.base);
      this.allow = allowSnapshotMethod;

      Method disallowSnapshotMethod = fs().getClass()
          .getMethod("disallowSnapshot", Path.class);
      disallowSnapshotMethod.setAccessible(true);
      disallowSnapshotMethod.invoke(fs(), this.base);
      this.disallow = disallowSnapshotMethod;
    } catch (InvocationTargetException e) {
      // Method exists but the invocation throws an exception.
      Throwable cause = e.getCause();
      if (cause instanceof Exception) {
        throw (Exception) cause;
      } else {
        throw new RuntimeException(cause);
      }
    } catch (ReflectiveOperationException e) {
      if (this.allow == null) {
        LOG.warn("No allowSnapshot method found.");
      }
      if (this.disallow == null) {
        LOG.warn("No disallowSnapshot method found.");
      }
    }
  }

  @HdfsCompatCaseTearDown
  public void tearDown() throws ReflectiveOperationException {
    try {
      if (this.disallow != null) {
        disallow.invoke(fs(), this.base);
      }
    } finally {
      HdfsCompatUtil.deleteQuietly(fs(), this.base, true);
    }
  }

  @HdfsCompatCasePrepare
  public void prepare() throws IOException, ReflectiveOperationException {
    this.dir = getUniquePath(base);
    HdfsCompatUtil.createFile(fs(), new Path(this.dir, this.fileName), 0);
    if (this.allow != null) {
      allow.invoke(fs(), this.dir);
    }
    this.snapshot = fs().createSnapshot(this.dir, this.snapshotName);
  }

  @HdfsCompatCaseCleanup
  public void cleanup() throws ReflectiveOperationException {
    try {
      try {
        fs().deleteSnapshot(this.dir, this.snapshotName);
      } catch (IOException ignored) {
      }
      if (this.disallow != null) {
        disallow.invoke(fs(), this.dir);
      }
    } finally {
      HdfsCompatUtil.deleteQuietly(fs(), this.dir, true);
    }
  }

  @HdfsCompatCase
  public void createSnapshot() throws IOException {
    Assert.assertNotEquals(snapshot.toString(), dir.toString());
    Assert.assertTrue(fs().exists(snapshot));
    Assert.assertTrue(fs().exists(new Path(snapshot, fileName)));
  }

  @HdfsCompatCase
  public void renameSnapshot() throws IOException {
    fs().renameSnapshot(dir, snapshotName, "s-name2");
    Assert.assertFalse(fs().exists(new Path(snapshot, fileName)));
    snapshot = getSnapshotPath(dir, "s-name2");
    Assert.assertTrue(fs().exists(new Path(snapshot, fileName)));
    fs().renameSnapshot(dir, "s-name2", snapshotName);
  }

  @HdfsCompatCase
  public void deleteSnapshot() throws IOException {
    fs().deleteSnapshot(dir, snapshotName);
    Assert.assertFalse(fs().exists(snapshot));
    Assert.assertFalse(fs().exists(new Path(snapshot, fileName)));
  }
}