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

import java.net.URI;
import java.util.List;
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


/**
 * Unit test of nested mount point support in INodeTree
 */
public class TestNestedMountPoint {
  private InodeTree inodeTree;
  private Configuration conf;
  private String mtName;
  private URI fsUri;

  static class TestNestMountPointFileSystem {
    public URI getUri() {
      return uri;
    }

    private URI uri;

    TestNestMountPointFileSystem(URI uri) {
      this.uri = uri;
    }
  }

  static class TestNestMountPointInternalFileSystem extends TestNestMountPointFileSystem {
    TestNestMountPointInternalFileSystem(URI uri) {
      super(uri);
    }
  }

  private static final URI LINKFALLBACK_TARGET = URI.create("hdfs://nn00");
  private static final URI NN1_TARGET = URI.create("hdfs://nn01/a/b");
  private static final URI NN2_TARGET = URI.create("hdfs://nn02/a/b/e");
  private static final URI NN3_TARGET = URI.create("hdfs://nn03/a/b/c/d");
  private static final URI NN4_TARGET = URI.create("hdfs://nn04/a/b/c/d/e");
  private static final URI NN5_TARGET = URI.create("hdfs://nn05/b/c/d/e");
  private static final URI NN6_TARGET = URI.create("hdfs://nn06/b/c/d/e/f");

  @BeforeEach
  public void setUp() throws Exception {
    conf = new Configuration();
    mtName = TestNestedMountPoint.class.getName();
    ConfigUtil.setIsNestedMountPointSupported(conf, true);
    ConfigUtil.addLink(conf, mtName, "/a/b", NN1_TARGET);
    ConfigUtil.addLink(conf, mtName, "/a/b/e", NN2_TARGET);
    ConfigUtil.addLink(conf, mtName, "/a/b/c/d", NN3_TARGET);
    ConfigUtil.addLink(conf, mtName, "/a/b/c/d/e", NN4_TARGET);
    ConfigUtil.addLink(conf, mtName, "/b/c/d/e", NN5_TARGET);
    ConfigUtil.addLink(conf, mtName, "/b/c/d/e/f", NN6_TARGET);
    ConfigUtil.addLinkFallback(conf, mtName, LINKFALLBACK_TARGET);

    fsUri = new URI(FsConstants.VIEWFS_SCHEME, mtName, "/", null, null);

    inodeTree = new InodeTree<TestNestedMountPoint.TestNestMountPointFileSystem>(conf,
        mtName, fsUri, false) {
      @Override
      protected Function<URI, TestNestedMountPoint.TestNestMountPointFileSystem> initAndGetTargetFs() {
        return new Function<URI, TestNestMountPointFileSystem>() {
          @Override
          public TestNestedMountPoint.TestNestMountPointFileSystem apply(URI uri) {
            return new TestNestMountPointFileSystem(uri);
          }
        };
      }

      // For intenral dir fs
      @Override
      protected TestNestedMountPoint.TestNestMountPointInternalFileSystem getTargetFileSystem(
          final INodeDir<TestNestedMountPoint.TestNestMountPointFileSystem> dir) {
        return new TestNestMountPointInternalFileSystem(fsUri);
      }

      @Override
      protected TestNestedMountPoint.TestNestMountPointInternalFileSystem getTargetFileSystem(
          final String settings, final URI[] mergeFsURIList) {
        return new TestNestMountPointInternalFileSystem(null);
      }
    };
  }

  @AfterEach
  public void tearDown() throws Exception {
    inodeTree = null;
  }

  @Test
  public void testPathResolveToLink() throws Exception {
    // /a/b/c/d/e/f resolves to /a/b/c/d/e and /f
    InodeTree.ResolveResult resolveResult = inodeTree.resolve("/a/b/c/d/e/f", true);
    Assertions.assertEquals(InodeTree.ResultKind.EXTERNAL_DIR, resolveResult.kind);
    Assertions.assertEquals("/a/b/c/d/e", resolveResult.resolvedPath);
    Assertions.assertEquals(new Path("/f"), resolveResult.remainingPath);
    Assertions.assertTrue(resolveResult.targetFileSystem instanceof TestNestMountPointFileSystem);
    Assertions.assertEquals(NN4_TARGET, ((TestNestMountPointFileSystem) resolveResult.targetFileSystem).getUri());
    Assertions.assertTrue(resolveResult.isLastInternalDirLink());

    // /a/b/c/d/e resolves to /a/b/c/d/e and /
    InodeTree.ResolveResult resolveResult2 = inodeTree.resolve("/a/b/c/d/e", true);
    Assertions.assertEquals(InodeTree.ResultKind.EXTERNAL_DIR, resolveResult2.kind);
    Assertions.assertEquals("/a/b/c/d/e", resolveResult2.resolvedPath);
    Assertions.assertEquals(new Path("/"), resolveResult2.remainingPath);
    Assertions.assertTrue(resolveResult2.targetFileSystem instanceof TestNestMountPointFileSystem);
    Assertions.assertEquals(NN4_TARGET, ((TestNestMountPointFileSystem) resolveResult2.targetFileSystem).getUri());
    Assertions.assertTrue(resolveResult2.isLastInternalDirLink());

    // /a/b/c/d/e/f/g/h/i resolves to /a/b/c/d/e and /f/g/h/i
    InodeTree.ResolveResult resolveResult3 = inodeTree.resolve("/a/b/c/d/e/f/g/h/i", true);
    Assertions.assertEquals(InodeTree.ResultKind.EXTERNAL_DIR, resolveResult3.kind);
    Assertions.assertEquals("/a/b/c/d/e", resolveResult3.resolvedPath);
    Assertions.assertEquals(new Path("/f/g/h/i"), resolveResult3.remainingPath);
    Assertions.assertTrue(resolveResult3.targetFileSystem instanceof TestNestMountPointFileSystem);
    Assertions.assertEquals(NN4_TARGET, ((TestNestMountPointFileSystem) resolveResult3.targetFileSystem).getUri());
    Assertions.assertTrue(resolveResult3.isLastInternalDirLink());
  }

  @Test
  public void testPathResolveToLinkNotResolveLastComponent() throws Exception {
    // /a/b/c/d/e/f resolves to /a/b/c/d/e and /f
    InodeTree.ResolveResult resolveResult = inodeTree.resolve("/a/b/c/d/e/f", false);
    Assertions.assertEquals(InodeTree.ResultKind.EXTERNAL_DIR, resolveResult.kind);
    Assertions.assertEquals("/a/b/c/d/e", resolveResult.resolvedPath);
    Assertions.assertEquals(new Path("/f"), resolveResult.remainingPath);
    Assertions.assertTrue(resolveResult.targetFileSystem instanceof TestNestMountPointFileSystem);
    Assertions.assertEquals(NN4_TARGET, ((TestNestMountPointFileSystem) resolveResult.targetFileSystem).getUri());
    Assertions.assertTrue(resolveResult.isLastInternalDirLink());

    // /a/b/c/d/e resolves to /a/b/c/d and /e
    InodeTree.ResolveResult resolveResult2 = inodeTree.resolve("/a/b/c/d/e", false);
    Assertions.assertEquals(InodeTree.ResultKind.EXTERNAL_DIR, resolveResult2.kind);
    Assertions.assertEquals("/a/b/c/d", resolveResult2.resolvedPath);
    Assertions.assertEquals(new Path("/e"), resolveResult2.remainingPath);
    Assertions.assertTrue(resolveResult2.targetFileSystem instanceof TestNestMountPointFileSystem);
    Assertions.assertEquals(NN3_TARGET, ((TestNestMountPointFileSystem) resolveResult2.targetFileSystem).getUri());
    Assertions.assertTrue(resolveResult2.isLastInternalDirLink());

    // /a/b/c/d/e/f/g/h/i resolves to /a/b/c/d/e and /f/g/h/i
    InodeTree.ResolveResult resolveResult3 = inodeTree.resolve("/a/b/c/d/e/f/g/h/i", false);
    Assertions.assertEquals(InodeTree.ResultKind.EXTERNAL_DIR, resolveResult3.kind);
    Assertions.assertEquals("/a/b/c/d/e", resolveResult3.resolvedPath);
    Assertions.assertEquals(new Path("/f/g/h/i"), resolveResult3.remainingPath);
    Assertions.assertTrue(resolveResult3.targetFileSystem instanceof TestNestMountPointFileSystem);
    Assertions.assertEquals(NN4_TARGET, ((TestNestMountPointFileSystem) resolveResult3.targetFileSystem).getUri());
    Assertions.assertTrue(resolveResult3.isLastInternalDirLink());
  }

  @Test
  public void testPathResolveToDirLink() throws Exception {
    // /a/b/c/d/f resolves to /a/b/c/d, /f
    InodeTree.ResolveResult resolveResult = inodeTree.resolve("/a/b/c/d/f", true);
    Assertions.assertEquals(InodeTree.ResultKind.EXTERNAL_DIR, resolveResult.kind);
    Assertions.assertEquals("/a/b/c/d", resolveResult.resolvedPath);
    Assertions.assertEquals(new Path("/f"), resolveResult.remainingPath);
    Assertions.assertTrue(resolveResult.targetFileSystem instanceof TestNestMountPointFileSystem);
    Assertions.assertEquals(NN3_TARGET, ((TestNestMountPointFileSystem) resolveResult.targetFileSystem).getUri());
    Assertions.assertTrue(resolveResult.isLastInternalDirLink());

    // /a/b/c/d resolves to /a/b/c/d and /
    InodeTree.ResolveResult resolveResult2 = inodeTree.resolve("/a/b/c/d", true);
    Assertions.assertEquals(InodeTree.ResultKind.EXTERNAL_DIR, resolveResult2.kind);
    Assertions.assertEquals("/a/b/c/d", resolveResult2.resolvedPath);
    Assertions.assertEquals(new Path("/"), resolveResult2.remainingPath);
    Assertions.assertTrue(resolveResult2.targetFileSystem instanceof TestNestMountPointFileSystem);
    Assertions.assertEquals(NN3_TARGET, ((TestNestMountPointFileSystem) resolveResult2.targetFileSystem).getUri());
    Assertions.assertTrue(resolveResult2.isLastInternalDirLink());

    // /a/b/c/d/f/g/h/i resolves to /a/b/c/d and /f/g/h/i
    InodeTree.ResolveResult resolveResult3 = inodeTree.resolve("/a/b/c/d/f/g/h/i", true);
    Assertions.assertEquals(InodeTree.ResultKind.EXTERNAL_DIR, resolveResult3.kind);
    Assertions.assertEquals("/a/b/c/d", resolveResult3.resolvedPath);
    Assertions.assertEquals(new Path("/f/g/h/i"), resolveResult3.remainingPath);
    Assertions.assertTrue(resolveResult3.targetFileSystem instanceof TestNestMountPointFileSystem);
    Assertions.assertEquals(NN3_TARGET, ((TestNestMountPointFileSystem) resolveResult3.targetFileSystem).getUri());
    Assertions.assertTrue(resolveResult3.isLastInternalDirLink());
  }

  @Test
  public void testPathResolveToDirLinkNotResolveLastComponent() throws Exception {
    // /a/b/c/d/f resolves to /a/b/c/d, /f
    InodeTree.ResolveResult resolveResult = inodeTree.resolve("/a/b/c/d/f", false);
    Assertions.assertEquals(InodeTree.ResultKind.EXTERNAL_DIR, resolveResult.kind);
    Assertions.assertEquals("/a/b/c/d", resolveResult.resolvedPath);
    Assertions.assertEquals(new Path("/f"), resolveResult.remainingPath);
    Assertions.assertTrue(resolveResult.targetFileSystem instanceof TestNestMountPointFileSystem);
    Assertions.assertEquals(NN3_TARGET, ((TestNestMountPointFileSystem) resolveResult.targetFileSystem).getUri());
    Assertions.assertTrue(resolveResult.isLastInternalDirLink());

    // /a/b/c/d resolves to /a/b and /c/d
    InodeTree.ResolveResult resolveResult2 = inodeTree.resolve("/a/b/c/d", false);
    Assertions.assertEquals(InodeTree.ResultKind.EXTERNAL_DIR, resolveResult2.kind);
    Assertions.assertEquals("/a/b", resolveResult2.resolvedPath);
    Assertions.assertEquals(new Path("/c/d"), resolveResult2.remainingPath);
    Assertions.assertTrue(resolveResult2.targetFileSystem instanceof TestNestMountPointFileSystem);
    Assertions.assertEquals(NN1_TARGET, ((TestNestMountPointFileSystem) resolveResult2.targetFileSystem).getUri());
    Assertions.assertTrue(resolveResult2.isLastInternalDirLink());

    // /a/b/c/d/f/g/h/i resolves to /a/b/c/d and /f/g/h/i
    InodeTree.ResolveResult resolveResult3 = inodeTree.resolve("/a/b/c/d/f/g/h/i", false);
    Assertions.assertEquals(InodeTree.ResultKind.EXTERNAL_DIR, resolveResult3.kind);
    Assertions.assertEquals("/a/b/c/d", resolveResult3.resolvedPath);
    Assertions.assertEquals(new Path("/f/g/h/i"), resolveResult3.remainingPath);
    Assertions.assertTrue(resolveResult3.targetFileSystem instanceof TestNestMountPointFileSystem);
    Assertions.assertEquals(NN3_TARGET, ((TestNestMountPointFileSystem) resolveResult3.targetFileSystem).getUri());
    Assertions.assertTrue(resolveResult3.isLastInternalDirLink());
  }

  @Test
  public void testMultiNestedMountPointsPathResolveToDirLink() throws Exception {
    // /a/b/f resolves to /a/b and /f
    InodeTree.ResolveResult resolveResult = inodeTree.resolve("/a/b/f", true);
    Assertions.assertEquals(InodeTree.ResultKind.EXTERNAL_DIR, resolveResult.kind);
    Assertions.assertEquals("/a/b", resolveResult.resolvedPath);
    Assertions.assertEquals(new Path("/f"), resolveResult.remainingPath);
    Assertions.assertTrue(resolveResult.targetFileSystem instanceof TestNestMountPointFileSystem);
    Assertions.assertEquals(NN1_TARGET, ((TestNestMountPointFileSystem) resolveResult.targetFileSystem).getUri());
    Assertions.assertTrue(resolveResult.isLastInternalDirLink());

    // /a/b resolves to /a/b and /
    InodeTree.ResolveResult resolveResult2 = inodeTree.resolve("/a/b", true);
    Assertions.assertEquals(InodeTree.ResultKind.EXTERNAL_DIR, resolveResult2.kind);
    Assertions.assertEquals("/a/b", resolveResult2.resolvedPath);
    Assertions.assertEquals(new Path("/"), resolveResult2.remainingPath);
    Assertions.assertTrue(resolveResult2.targetFileSystem instanceof TestNestMountPointFileSystem);
    Assertions.assertEquals(NN1_TARGET, ((TestNestMountPointFileSystem) resolveResult2.targetFileSystem).getUri());
    Assertions.assertTrue(resolveResult2.isLastInternalDirLink());
  }

  @Test
  public void testMultiNestedMountPointsPathResolveToDirLinkNotResolveLastComponent() throws Exception {
    // /a/b/f resolves to /a/b and /f
    InodeTree.ResolveResult resolveResult = inodeTree.resolve("/a/b/f", false);
    Assertions.assertEquals(InodeTree.ResultKind.EXTERNAL_DIR, resolveResult.kind);
    Assertions.assertEquals("/a/b", resolveResult.resolvedPath);
    Assertions.assertEquals(new Path("/f"), resolveResult.remainingPath);
    Assertions.assertTrue(resolveResult.targetFileSystem instanceof TestNestMountPointFileSystem);
    Assertions.assertEquals(NN1_TARGET, ((TestNestMountPointFileSystem) resolveResult.targetFileSystem).getUri());
    Assertions.assertTrue(resolveResult.isLastInternalDirLink());

    // /a/b resolves to /a and /b
    InodeTree.ResolveResult resolveResult2 = inodeTree.resolve("/a/b", false);
    Assertions.assertEquals(InodeTree.ResultKind.INTERNAL_DIR, resolveResult2.kind);
    Assertions.assertEquals("/a", resolveResult2.resolvedPath);
    Assertions.assertEquals(new Path("/b"), resolveResult2.remainingPath);
    Assertions.assertTrue(resolveResult2.targetFileSystem instanceof TestNestMountPointInternalFileSystem);
    Assertions.assertEquals(fsUri, ((TestNestMountPointInternalFileSystem) resolveResult2.targetFileSystem).getUri());
    Assertions.assertFalse(resolveResult2.isLastInternalDirLink());
  }

  @Test
  public void testPathResolveToDirLinkLastComponentInternalDir() throws Exception {
    // /a/b/c resolves to /a/b and /c
    InodeTree.ResolveResult resolveResult = inodeTree.resolve("/a/b/c", true);
    Assertions.assertEquals(InodeTree.ResultKind.EXTERNAL_DIR, resolveResult.kind);
    Assertions.assertEquals("/a/b", resolveResult.resolvedPath);
    Assertions.assertEquals(new Path("/c"), resolveResult.remainingPath);
    Assertions.assertTrue(resolveResult.targetFileSystem instanceof TestNestMountPointFileSystem);
    Assertions.assertEquals(NN1_TARGET, ((TestNestMountPointFileSystem) resolveResult.targetFileSystem).getUri());
    Assertions.assertTrue(resolveResult.isLastInternalDirLink());
  }

  @Test
  public void testPathResolveToDirLinkLastComponentInternalDirNotResolveLastComponent() throws Exception {
    // /a/b/c resolves to /a/b and /c
    InodeTree.ResolveResult resolveResult = inodeTree.resolve("/a/b/c", false);
    Assertions.assertEquals(InodeTree.ResultKind.EXTERNAL_DIR, resolveResult.kind);
    Assertions.assertEquals("/a/b", resolveResult.resolvedPath);
    Assertions.assertEquals(new Path("/c"), resolveResult.remainingPath);
    Assertions.assertTrue(resolveResult.targetFileSystem instanceof TestNestMountPointFileSystem);
    Assertions.assertEquals(NN1_TARGET, ((TestNestMountPointFileSystem) resolveResult.targetFileSystem).getUri());
    Assertions.assertTrue(resolveResult.isLastInternalDirLink());
  }

  @Test
  public void testPathResolveToLinkFallBack() throws Exception {
    // /a/e resolves to linkfallback
    InodeTree.ResolveResult resolveResult = inodeTree.resolve("/a/e", true);
    Assertions.assertEquals(InodeTree.ResultKind.EXTERNAL_DIR, resolveResult.kind);
    Assertions.assertEquals("/", resolveResult.resolvedPath);
    Assertions.assertEquals(new Path("/a/e"), resolveResult.remainingPath);
    Assertions.assertTrue(resolveResult.targetFileSystem instanceof TestNestMountPointFileSystem);
    Assertions.assertEquals(LINKFALLBACK_TARGET, ((TestNestMountPointFileSystem) resolveResult.targetFileSystem).getUri());
    Assertions.assertFalse(resolveResult.isLastInternalDirLink());
  }

  @Test
  public void testPathNotResolveToLinkFallBackNotResolveLastComponent() throws Exception {
    // /a/e resolves to internalDir instead of linkfallback
    InodeTree.ResolveResult resolveResult = inodeTree.resolve("/a/e", false);
    Assertions.assertEquals(InodeTree.ResultKind.INTERNAL_DIR, resolveResult.kind);
    Assertions.assertEquals("/a", resolveResult.resolvedPath);
    Assertions.assertEquals(new Path("/e"), resolveResult.remainingPath);
    Assertions.assertTrue(resolveResult.targetFileSystem instanceof TestNestMountPointInternalFileSystem);
    Assertions.assertEquals(fsUri, ((TestNestMountPointInternalFileSystem) resolveResult.targetFileSystem).getUri());
    Assertions.assertFalse(resolveResult.isLastInternalDirLink());
  }

  @Test
  public void testPathResolveToInternalDir() throws Exception {
    // /b/c resolves to internal dir
    InodeTree.ResolveResult resolveResult = inodeTree.resolve("/b/c", true);
    Assertions.assertEquals(InodeTree.ResultKind.INTERNAL_DIR, resolveResult.kind);
    Assertions.assertEquals("/b/c", resolveResult.resolvedPath);
    Assertions.assertEquals(new Path("/"), resolveResult.remainingPath);
    Assertions.assertTrue(resolveResult.targetFileSystem instanceof TestNestMountPointInternalFileSystem);
    Assertions.assertEquals(fsUri, ((TestNestMountPointInternalFileSystem) resolveResult.targetFileSystem).getUri());
    Assertions.assertFalse(resolveResult.isLastInternalDirLink());
  }

  @Test
  public void testPathResolveToInternalDirNotResolveLastComponent() throws Exception {
    // /b/c resolves to internal dir
    InodeTree.ResolveResult resolveResult = inodeTree.resolve("/b/c", false);
    Assertions.assertEquals(InodeTree.ResultKind.INTERNAL_DIR, resolveResult.kind);
    Assertions.assertEquals("/b", resolveResult.resolvedPath);
    Assertions.assertEquals(new Path("/c"), resolveResult.remainingPath);
    Assertions.assertTrue(resolveResult.targetFileSystem instanceof TestNestMountPointInternalFileSystem);
    Assertions.assertEquals(fsUri, ((TestNestMountPointInternalFileSystem) resolveResult.targetFileSystem).getUri());
    Assertions.assertFalse(resolveResult.isLastInternalDirLink());
  }

  @Test
  public void testSlashResolveToInternalDir() throws Exception {
    // / resolves to internal dir
    InodeTree.ResolveResult resolveResult = inodeTree.resolve("/", true);
    Assertions.assertEquals(InodeTree.ResultKind.INTERNAL_DIR, resolveResult.kind);
    Assertions.assertEquals("/", resolveResult.resolvedPath);
    Assertions.assertEquals(new Path("/"), resolveResult.remainingPath);
    Assertions.assertTrue(resolveResult.targetFileSystem instanceof TestNestMountPointInternalFileSystem);
    Assertions.assertFalse(resolveResult.isLastInternalDirLink());
  }

  @Test
  public void testInodeTreeMountPoints() throws Exception {
    List<InodeTree.MountPoint<FileSystem>> mountPoints = inodeTree.getMountPoints();
    Assertions.assertEquals(6, mountPoints.size());
  }
}
