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

package org.apache.hadoop.hdfs.server.datanode;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_ENABLED_OPS_FILEIO_FAULT_INJECTION_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_ENABLE_FILEIO_FAULT_INJECTION_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_FILEIO_FAULT_PERCENTAGE_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.datanode.FaultInjectorFileIoEvents.InjectedFileIOFaultException;
import org.apache.hadoop.hdfs.server.datanode.FileIoProvider.OPERATION;
import org.junit.jupiter.api.Test;

public class TestFaultInjectorFileIoEvents {

  private FaultInjectorFileIoEvents createFaultInjector(boolean enabled, String ops,
      int percentage) {
    Configuration conf = new Configuration();
    conf.setBoolean(DFS_DATANODE_ENABLE_FILEIO_FAULT_INJECTION_KEY, enabled);
    conf.set(DFS_DATANODE_ENABLED_OPS_FILEIO_FAULT_INJECTION_KEY, ops);
    conf.setInt(DFS_DATANODE_FILEIO_FAULT_PERCENTAGE_KEY, percentage);
    return new FaultInjectorFileIoEvents(conf);
  }

  private int getFaultMaxRange(int propertyValue) {
    return (int) ((double) propertyValue / 100 * Integer.MAX_VALUE);
  }

  private Set<OPERATION> createExpected(OPERATION... ops) {
    Set<OPERATION> result = new HashSet<>();
    for (OPERATION o : ops) {
      result.add(o);
    }
    return result;
  }

  @Test
  public void testDisabled() throws InjectedFileIOFaultException {
    FaultInjectorFileIoEvents injector = createFaultInjector(false, "", 32);
    assertEquals(false, injector.isEnabled());
    assertEquals(createExpected(), injector.getOperations());
    assertEquals(0, injector.getFaultRangeMax());
    injector.beforeMetadataOp(null, OPERATION.DELETE);
    injector.beforeFileIo(null, OPERATION.WRITE, 0);
  }

  @Test
  public void testEnabledZeroPercentage() throws InjectedFileIOFaultException {
    FaultInjectorFileIoEvents injector = createFaultInjector(true, OPERATION.DELETE.name(), 0);
    assertEquals(true, injector.isEnabled());
    assertEquals(createExpected(OPERATION.DELETE), injector.getOperations());
    assertEquals(getFaultMaxRange(0), injector.getFaultRangeMax());
    injector.beforeMetadataOp(null, OPERATION.DELETE);
    injector.beforeFileIo(null, OPERATION.WRITE, 0);
  }

  @Test
  public void testEnabled() throws InjectedFileIOFaultException {
    FaultInjectorFileIoEvents injector = createFaultInjector(true, OPERATION.DELETE.name(), 100);
    assertEquals(true, injector.isEnabled());
    assertEquals(createExpected(OPERATION.DELETE), injector.getOperations());
    assertEquals(getFaultMaxRange(100), injector.getFaultRangeMax());
    assertThrows(InjectedFileIOFaultException .class, () -> injector.beforeMetadataOp(null,
      OPERATION.DELETE));
    assertThrows(InjectedFileIOFaultException .class, () -> injector.beforeFileIo(null,
      OPERATION.DELETE, 0));
    injector.beforeMetadataOp(null, OPERATION.WRITE);
    injector.beforeFileIo(null, OPERATION.WRITE, 0);
  }

  @Test
  public void testEnabledMulti() throws InjectedFileIOFaultException {
    String ops = OPERATION.DELETE.name() + "," + OPERATION.WRITE.name();
    FaultInjectorFileIoEvents injector = createFaultInjector(true, ops, 100);
    assertEquals(true, injector.isEnabled());
    assertEquals(createExpected(OPERATION.DELETE, OPERATION.WRITE), injector.getOperations());
    assertEquals(getFaultMaxRange(100), injector.getFaultRangeMax());
    assertThrows(InjectedFileIOFaultException .class, () -> injector.beforeMetadataOp(null,
      OPERATION.DELETE));
    assertThrows(InjectedFileIOFaultException .class, () -> injector.beforeFileIo(null,
      OPERATION.DELETE, 0));
    assertThrows(InjectedFileIOFaultException .class, () -> injector.beforeMetadataOp(null,
      OPERATION.WRITE));
    assertThrows(InjectedFileIOFaultException .class, () -> injector.beforeFileIo(null,
      OPERATION.WRITE, 0));
    injector.beforeMetadataOp(null, OPERATION.READ);
    injector.beforeFileIo(null, OPERATION.READ, 0);
  }

  @Test
  public void testEnabledMultiInvalidEntry() throws InjectedFileIOFaultException {
    String ops = OPERATION.DELETE.name() + "," + OPERATION.WRITE.name() + ",foo";
    FaultInjectorFileIoEvents injector = createFaultInjector(true, ops, 100);
    assertEquals(true, injector.isEnabled());
    assertEquals(createExpected(OPERATION.DELETE, OPERATION.WRITE), injector.getOperations());
    assertEquals(getFaultMaxRange(100), injector.getFaultRangeMax());
    assertThrows(InjectedFileIOFaultException .class, () -> injector.beforeMetadataOp(null,
      OPERATION.DELETE));
    assertThrows(InjectedFileIOFaultException .class, () -> injector.beforeFileIo(null,
      OPERATION.DELETE, 0));
    assertThrows(InjectedFileIOFaultException .class, () -> injector.beforeMetadataOp(null,
      OPERATION.WRITE));
    assertThrows(InjectedFileIOFaultException .class, () -> injector.beforeFileIo(null,
      OPERATION.WRITE, 0));
    injector.beforeMetadataOp(null, OPERATION.READ);
    injector.beforeFileIo(null, OPERATION.READ, 0);
  }

  @Test
  public void testEnabledMultiInvalidEntryLower() throws InjectedFileIOFaultException {
    String ops = OPERATION.DELETE.name() + "," + OPERATION.WRITE.name() + ",foo";
    FaultInjectorFileIoEvents injector = createFaultInjector(true, ops.toLowerCase(), 100);
    assertEquals(true, injector.isEnabled());
    assertEquals(createExpected(OPERATION.DELETE, OPERATION.WRITE), injector.getOperations());
    assertEquals(getFaultMaxRange(100), injector.getFaultRangeMax());
    assertThrows(InjectedFileIOFaultException .class, () -> injector.beforeMetadataOp(null,
      OPERATION.DELETE));
    assertThrows(InjectedFileIOFaultException .class, () -> injector.beforeFileIo(null,
      OPERATION.DELETE, 0));
    assertThrows(InjectedFileIOFaultException .class, () -> injector.beforeMetadataOp(null,
      OPERATION.WRITE));
    assertThrows(InjectedFileIOFaultException .class, () -> injector.beforeFileIo(null,
      OPERATION.WRITE, 0));
    injector.beforeMetadataOp(null, OPERATION.READ);
    injector.beforeFileIo(null, OPERATION.READ, 0);
  }
}
