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

import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static junit.framework.TestCase.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestAuthorizationContext {

  private String fsOwner = "hdfs";
  private String superGroup = "hdfs";
  private UserGroupInformation ugi = UserGroupInformation.
      createUserForTesting(fsOwner, new String[] {superGroup});

  private INodeAttributes[] emptyINodeAttributes = new INodeAttributes[] {};
  private INodesInPath iip = mock(INodesInPath.class);
  private int snapshotId = 0;
  private INode[] inodes = new INode[] {};
  private byte[][] components = new byte[][] {};
  private String path = "";
  private int ancestorIndex = inodes.length - 2;

  @Before
  public void setUp() throws IOException {
    int snapshotId = 0;
    when(iip.getPathSnapshotId()).thenReturn(snapshotId);
    when(iip.getINodesArray()).thenReturn(inodes);
    when(iip.getPathComponents()).thenReturn(components);
    when(iip.getPath()).thenReturn(path);
  }

  @Test
  public void testBuilder() {
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
    INodeAttributeProvider.AccessControlEnforcer
        mockEnforcer = mock(INodeAttributeProvider.AccessControlEnforcer.class);
    INodeAttributeProvider mockINodeAttributeProvider =
        mock(INodeAttributeProvider.class);
    when(mockINodeAttributeProvider.getExternalAccessControlEnforcer(any())).
        thenReturn(mockEnforcer);

    FSPermissionChecker checker = new FSPermissionChecker(
        fsOwner, superGroup, ugi, mockINodeAttributeProvider);

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
    verify(mockEnforcer).checkPermission(fsOwner, superGroup, ugi,
        emptyINodeAttributes, inodes, components, snapshotId, path,
        ancestorIndex, true, null, null, null, null, true);
  }

  @Test
  public void testCheckPermissionWithContextAPI() throws IOException {
    INodeAttributeProvider.AccessControlEnforcer
        mockEnforcer = mock(INodeAttributeProvider.AccessControlEnforcer.class);
    INodeAttributeProvider mockINodeAttributeProvider =
        mock(INodeAttributeProvider.class);
    when(mockINodeAttributeProvider.getExternalAccessControlEnforcer(any())).
        thenReturn(mockEnforcer);

    FSPermissionChecker checker = new FSPermissionChecker(
        fsOwner, superGroup, ugi, mockINodeAttributeProvider);

    // force it to use the new, checkPermissionWithContext API.
    String operationName = "abc";
    FSPermissionChecker.setOperationType(operationName);

    checker.checkPermission(iip, true,
        null, null, null,
        null, true);

    INodeAttributes[] emptyINodeAttributes = new INodeAttributes[] {};

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
    verify(mockEnforcer).checkPermissionWithContext(context);
  }
}
