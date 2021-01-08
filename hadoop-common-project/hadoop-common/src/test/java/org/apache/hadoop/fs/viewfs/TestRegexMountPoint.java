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
package org.apache.hadoop.fs.viewfs;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test Regex Mount Point.
 */
public class TestRegexMountPoint {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(TestRegexMountPoint.class.getName());

  private InodeTree inodeTree;
  private Configuration conf;

  class TestRegexMountPointFileSystem {
    public URI getUri() {
      return uri;
    }

    private URI uri;

    TestRegexMountPointFileSystem(URI uri) {
      String uriStr = uri == null ? "null" : uri.toString();
      LOGGER.info("Create TestRegexMountPointFileSystem Via URI:" + uriStr);
      this.uri = uri;
    }
  }

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    ConfigUtil.addLink(conf, TestRegexMountPoint.class.getName(), "/mnt",
        URI.create("file:///"));

    inodeTree = new InodeTree<TestRegexMountPointFileSystem>(conf,
        TestRegexMountPoint.class.getName(), null, false) {
      @Override
      protected TestRegexMountPointFileSystem getTargetFileSystem(
          final URI uri) {
        return new TestRegexMountPointFileSystem(uri);
      }

      @Override
      protected TestRegexMountPointFileSystem getTargetFileSystem(
          final INodeDir<TestRegexMountPointFileSystem> dir) {
        return new TestRegexMountPointFileSystem(null);
      }

      @Override
      protected TestRegexMountPointFileSystem getTargetFileSystem(
          final String settings, final URI[] mergeFsURIList) {
        return new TestRegexMountPointFileSystem(null);
      }
    };
  }

  @After
  public void tearDown() throws Exception {
    inodeTree = null;
  }

  @Test
  public void testGetVarListInString() throws IOException {
    String srcRegex = "/(\\w+)";
    String target = "/$0/${1}/$1/${2}/${2}";
    RegexMountPoint regexMountPoint =
        new RegexMountPoint(inodeTree, srcRegex, target, null);
    regexMountPoint.initialize();
    Map<String, Set<String>> varMap = regexMountPoint.getVarInDestPathMap();
    Assert.assertEquals(varMap.size(), 3);
    Assert.assertEquals(varMap.get("0").size(), 1);
    Assert.assertTrue(varMap.get("0").contains("$0"));
    Assert.assertEquals(varMap.get("1").size(), 2);
    Assert.assertTrue(varMap.get("1").contains("${1}"));
    Assert.assertTrue(varMap.get("1").contains("$1"));
    Assert.assertEquals(varMap.get("2").size(), 1);
    Assert.assertTrue(varMap.get("2").contains("${2}"));
  }

  @Test
  public void testResolve() throws IOException {
    String regexStr = "^/user/(?<username>\\w+)";
    String dstPathStr = "/namenode1/testResolve/$username";
    String settingsStr = null;
    RegexMountPoint regexMountPoint =
        new RegexMountPoint(inodeTree, regexStr, dstPathStr, settingsStr);
    regexMountPoint.initialize();
    InodeTree.ResolveResult resolveResult =
        regexMountPoint.resolve("/user/hadoop/file1", true);
    Assert.assertEquals(resolveResult.kind, InodeTree.ResultKind.EXTERNAL_DIR);
    Assert.assertTrue(
        resolveResult.targetFileSystem
            instanceof TestRegexMountPointFileSystem);
    Assert.assertEquals("/user/hadoop", resolveResult.resolvedPath);
    Assert.assertTrue(
        resolveResult.targetFileSystem
            instanceof TestRegexMountPointFileSystem);
    Assert.assertEquals("/namenode1/testResolve/hadoop",
        ((TestRegexMountPointFileSystem) resolveResult.targetFileSystem)
            .getUri().toString());
    Assert.assertEquals("/file1", resolveResult.remainingPath.toString());
  }

  @Test
  public void testResolveWithInterceptor() throws IOException {
    String regexStr = "^/user/(?<username>\\w+)";
    String dstPathStr = "/namenode1/testResolve/$username";
    // Replace "_" with "-"
    RegexMountPointResolvedDstPathReplaceInterceptor interceptor =
        new RegexMountPointResolvedDstPathReplaceInterceptor("_", "-");
    // replaceresolvedpath:_:-
    String settingsStr = interceptor.serializeToString();
    RegexMountPoint regexMountPoint =
        new RegexMountPoint(inodeTree, regexStr, dstPathStr, settingsStr);
    regexMountPoint.initialize();
    InodeTree.ResolveResult resolveResult =
        regexMountPoint.resolve("/user/hadoop_user1/file_index", true);
    Assert.assertEquals(resolveResult.kind, InodeTree.ResultKind.EXTERNAL_DIR);
    Assert.assertTrue(
        resolveResult.targetFileSystem
            instanceof TestRegexMountPointFileSystem);
    Assert.assertEquals("/user/hadoop_user1", resolveResult.resolvedPath);
    Assert.assertTrue(
        resolveResult.targetFileSystem
            instanceof TestRegexMountPointFileSystem);
    Assert.assertEquals("/namenode1/testResolve/hadoop-user1",
        ((TestRegexMountPointFileSystem) resolveResult.targetFileSystem)
            .getUri().toString());
    Assert.assertEquals("/file_index",
        resolveResult.remainingPath.toString());
  }
}
