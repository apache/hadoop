/**
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

import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static junit.framework.TestCase.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestAuthorizationContext {

  private String fsOwner = "hdfs";
  private String superGroup = "hdfs";
  private UserGroupInformation ugi = UserGroupInformation.createUserForTesting(fsOwner, new String[] {superGroup});

  private INodeAttributes[] emptyINodeAttributes = new INodeAttributes[] {};
  private INodesInPath iip = mock(INodesInPath.class);
  private int snapshotId = 0;
  private INode[] inodes = new INode[] {};
  private byte[][] components = new byte[][] {};
  private String path = "";
  private int ancestorIndex = inodes.length - 2;

  public static class MyAuthorizationProvider extends INodeAttributeProvider {
    AccessControlEnforcer mockEnforcer = mock(AccessControlEnforcer.class);


    public static class MyAccessControlEnforcer implements AccessControlEnforcer {
      AccessControlEnforcer ace;

      public MyAccessControlEnforcer(AccessControlEnforcer defaultEnforcer) {
        this.ace = defaultEnforcer;

      }

      @Override
      public void checkPermission(String fsOwner, String supergroup,
          UserGroupInformation ugi, INodeAttributes[] inodeAttrs,
          INode[] inodes, byte[][] pathByNameArr, int snapshotId, String path,
          int ancestorIndex, boolean doCheckOwner, FsAction ancestorAccess,
          FsAction parentAccess, FsAction access, FsAction subAccess,
          boolean ignoreEmptyDir) throws AccessControlException {
        if (ancestorIndex > 1
            && inodes[1].getLocalName().equals("user")
            && inodes[2].getLocalName().equals("acl")) {
          this.ace.checkPermission(fsOwner, supergroup, ugi, inodeAttrs, inodes,
              pathByNameArr, snapshotId, path, ancestorIndex, doCheckOwner,
              ancestorAccess, parentAccess, access, subAccess, ignoreEmptyDir);
        }
      }

      @Override
      public void checkPermissionWithContext(
          AuthorizationContext authzContext) throws AccessControlException {
        if (authzContext.ancestorIndex > 1
            && authzContext.inodes[1].getLocalName().equals("user")
            && authzContext.inodes[2].getLocalName().equals("acl")) {
          this.ace.checkPermissionWithContext(authzContext);
        }
      }

      public void abc() {}
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }

    @Override
    public INodeAttributes getAttributes(String[] pathElements,
        final INodeAttributes inode) {
      return null;
    }

    @Override
    public AccessControlEnforcer getExternalAccessControlEnforcer(
        AccessControlEnforcer defaultEnforcer) {
      return mockEnforcer;
    }

  }

  @Before
  public void setUp() throws IOException {
    int snapshotId = 0;
    when(iip.getPathSnapshotId()).thenReturn(snapshotId);
    when(iip.getINodesArray()).thenReturn(inodes);
    when(iip.getPathComponents()).thenReturn(components);
    when(iip.getPath()).thenReturn(path);
  }

  @Test
  public void testBuilder() throws IOException {
    String opType = "test";
    CallerContext.setCurrent(new CallerContext.Builder(
        "TestAuthorizationContext").build());

    INodeAttributeProvider.AuthorizationContext.Builder builder =
        new INodeAttributeProvider.AuthorizationContext.Builder();
    builder.fsOwner(fsOwner).
        supergroup(superGroup).
        callerUgi(ugi).
        inodeAttrs(emptyINodeAttributes).
        inodes(inodes).
        pathByNameArr(components).
        snapshotId(snapshotId).
        path(path).
        ancestorIndex(ancestorIndex).
        doCheckOwner(true).
        ancestorAccess(null).
        parentAccess(null).
        access(null).
        subAccess(null).
        ignoreEmptyDir(true).
        operationName(opType).
        callerContext(CallerContext.getCurrent());

    INodeAttributeProvider.AuthorizationContext authzContext = builder.build();
    assertEquals(authzContext.fsOwner, fsOwner);
    assertEquals(authzContext.supergroup, superGroup);
    assertEquals(authzContext.callerUgi, ugi);
    assertEquals(authzContext.inodeAttrs, emptyINodeAttributes);
    assertEquals(authzContext.inodes, inodes);
    assertEquals(authzContext.pathByNameArr, components);
    assertEquals(authzContext.snapshotId, snapshotId);
    assertEquals(authzContext.path, path);
    assertEquals(authzContext.ancestorIndex, ancestorIndex);
    assertEquals(authzContext.operationName, opType);
    assertEquals(authzContext.callerContext, CallerContext.getCurrent());
  }

  @Test
  public void testLegacyAPI() throws IOException {
    MyAuthorizationProvider authzProvider = new MyAuthorizationProvider();
    FSPermissionChecker checker = new FSPermissionChecker(
        fsOwner, superGroup, ugi, authzProvider);

    // set operation type to null to force using the legacy API.
    FSPermissionChecker.setOperationType(null);

    INodesInPath iip = mock(INodesInPath.class);
    int snapshotId = 0;
    when(iip.getPathSnapshotId()).thenReturn(snapshotId);
    INode[] inodes = new INode[] {};
    when(iip.getINodesArray()).thenReturn(inodes);
    byte[][] components = new byte[][] {};
    when(iip.getPathComponents()).thenReturn(components);
    String path = "";
    when(iip.getPath()).thenReturn(path);

    checker.checkPermission(iip, true, null, null, null, null, true);

    INodeAttributes[] emptyINodeAttributes = new INodeAttributes[] {};
    INodeAttributeProvider.AccessControlEnforcer enforcer =
        authzProvider.getExternalAccessControlEnforcer(null);
    verify(enforcer).checkPermission(fsOwner, superGroup, ugi,
        emptyINodeAttributes, inodes, components, snapshotId, path,
        ancestorIndex, true, null, null, null, null, true);
  }

  @Test
  public void testCheckPermissionWithContextAPI() throws IOException {
    INodeAttributeProvider.AccessControlEnforcer
        mockEnforcer = mock(INodeAttributeProvider.AccessControlEnforcer.class);

    INodeAttributeProvider iNodeAttributeProvider = mock(INodeAttributeProvider.class);

    MyAuthorizationProvider authzProvider = new MyAuthorizationProvider();
    FSPermissionChecker checker = new FSPermissionChecker(
        fsOwner, superGroup, ugi, authzProvider);

    // force it to use the new, checkPermissionWithContext API.
    String operationName = "abc";
    FSPermissionChecker.setOperationType(operationName);

    checker.checkPermission(iip, true,
        null, null, null,
        null, true);

    INodeAttributes[] emptyINodeAttributes = new INodeAttributes[] {};
    INodeAttributeProvider.AccessControlEnforcer enforcer =
        authzProvider.getExternalAccessControlEnforcer(null);

    INodeAttributeProvider.AuthorizationContext.Builder builder =
        new INodeAttributeProvider.AuthorizationContext.Builder();
    builder.fsOwner(fsOwner).
        supergroup(superGroup).
        callerUgi(ugi).
        inodeAttrs(emptyINodeAttributes).
        inodes(inodes).
        pathByNameArr(components).
        snapshotId(snapshotId).
        path(path).
        ancestorIndex(ancestorIndex).
        doCheckOwner(true).
        ancestorAccess(null).
        parentAccess(null).
        access(null).
        subAccess(null).
        ignoreEmptyDir(true).
        operationName(operationName).
        callerContext(CallerContext.getCurrent());
    INodeAttributeProvider.AuthorizationContext context = builder.build();
    // the AuthorizationContext.equals() method is override to always return
    // true as long as it is compared with another AuthorizationContext object.
    verify(enforcer).checkPermissionWithContext(context);
  }
}
