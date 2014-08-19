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
package org.apache.hadoop.hdfs.nfs.nfs3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import java.io.IOException;

import org.apache.hadoop.nfs.NfsFileType;
import org.apache.hadoop.nfs.nfs3.Nfs3FileAttributes;

import org.mockito.Mockito;

public class TestNfs3Utils {
  @Test
  public void testGetAccessRightsForUserGroup() throws IOException {
    Nfs3FileAttributes attr = Mockito.mock(Nfs3FileAttributes.class);
    Mockito.when(attr.getUid()).thenReturn(2);
    Mockito.when(attr.getGid()).thenReturn(3);
    Mockito.when(attr.getMode()).thenReturn(448); // 700
    Mockito.when(attr.getType()).thenReturn(NfsFileType.NFSREG.toValue());
    assertEquals("No access should be allowed as UID does not match attribute over mode 700",
      0, Nfs3Utils.getAccessRightsForUserGroup(3, 3, null, attr));
    Mockito.when(attr.getUid()).thenReturn(2);
    Mockito.when(attr.getGid()).thenReturn(3);
    Mockito.when(attr.getMode()).thenReturn(56); // 070
    Mockito.when(attr.getType()).thenReturn(NfsFileType.NFSREG.toValue());
    assertEquals("No access should be allowed as GID does not match attribute over mode 070",
      0, Nfs3Utils.getAccessRightsForUserGroup(2, 4, null, attr));
    Mockito.when(attr.getUid()).thenReturn(2);
    Mockito.when(attr.getGid()).thenReturn(3);
    Mockito.when(attr.getMode()).thenReturn(7); // 007
    Mockito.when(attr.getType()).thenReturn(NfsFileType.NFSREG.toValue());
    assertEquals("Access should be allowed as mode is 007 and UID/GID do not match",
      61 /* RWX */, Nfs3Utils.getAccessRightsForUserGroup(1, 4, new int[] {5, 6}, attr));
    Mockito.when(attr.getUid()).thenReturn(2);
    Mockito.when(attr.getGid()).thenReturn(10);
    Mockito.when(attr.getMode()).thenReturn(288); // 440
    Mockito.when(attr.getType()).thenReturn(NfsFileType.NFSREG.toValue());
    assertEquals("Access should be allowed as mode is 440 and Aux GID does match",
      1 /* R */, Nfs3Utils.getAccessRightsForUserGroup(3, 4, new int[] {5, 16, 10}, attr));
    Mockito.when(attr.getUid()).thenReturn(2);
    Mockito.when(attr.getGid()).thenReturn(10);
    Mockito.when(attr.getMode()).thenReturn(448); // 700
    Mockito.when(attr.getType()).thenReturn(NfsFileType.NFSDIR.toValue());
    assertEquals("Access should be allowed for dir as mode is 700 and UID does match",
      31 /* Lookup */, Nfs3Utils.getAccessRightsForUserGroup(2, 4, new int[] {5, 16, 10}, attr));
    assertEquals("No access should be allowed for dir as mode is 700 even though GID does match",
      0, Nfs3Utils.getAccessRightsForUserGroup(3, 10, new int[] {5, 16, 4}, attr));
    assertEquals("No access should be allowed for dir as mode is 700 even though AuxGID does match",
      0, Nfs3Utils.getAccessRightsForUserGroup(3, 20, new int[] {5, 10}, attr));
    
    Mockito.when(attr.getUid()).thenReturn(2);
    Mockito.when(attr.getGid()).thenReturn(10);
    Mockito.when(attr.getMode()).thenReturn(457); // 711
    Mockito.when(attr.getType()).thenReturn(NfsFileType.NFSDIR.toValue());
    assertEquals("Access should be allowed for dir as mode is 711 and GID matches",
        2 /* Lookup */, Nfs3Utils.getAccessRightsForUserGroup(3, 10, new int[] {5, 16, 11}, attr));
  }
}
