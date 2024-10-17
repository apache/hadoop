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

import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.mockito.ArgumentMatcher;
import org.mockito.ArgumentMatchers;
import org.mockito.stubbing.Answer;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerTestUtil;
import org.apache.hadoop.hdfs.server.namenode.ha.EditLogTailer;
import org.apache.hadoop.test.Whitebox;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

/**
 * This is a Mockito based utility class to expose NameNode functionality for unit tests.
 */
public final class NameNodeAdapterMockitoUtil {

  private NameNodeAdapterMockitoUtil() {
  }

  public static BlockManager spyOnBlockManager(NameNode nn) {
    BlockManager bmSpy = spy(nn.getNamesystem().getBlockManager());
    nn.getNamesystem().setBlockManagerForTesting(bmSpy);
    return bmSpy;
  }

  public static ReentrantReadWriteLock spyOnFsLock(FSNamesystem fsn) {
    ReentrantReadWriteLock spy = spy(fsn.getFsLockForTests());
    fsn.setFsLockForTests(spy);
    return spy;
  }

  public static FSImage spyOnFsImage(NameNode nn1) {
    FSNamesystem fsn = nn1.getNamesystem();
    FSImage spy = spy(fsn.getFSImage());
    Whitebox.setInternalState(fsn, "fsImage", spy);
    return spy;
  }

  public static JournalSet spyOnJournalSet(NameNode nn) {
    FSEditLog editLog = nn.getFSImage().getEditLog();
    JournalSet js = spy(editLog.getJournalSet());
    editLog.setJournalSetForTesting(js);
    return js;
  }

  public static FSNamesystem spyOnNamesystem(NameNode nn) {
    FSNamesystem fsnSpy = spy(nn.getNamesystem());
    FSNamesystem fsnOld = nn.namesystem;
    fsnOld.writeLock();
    fsnSpy.writeLock();
    nn.namesystem = fsnSpy;
    try {
      FieldUtils.writeDeclaredField(nn.getRpcServer(), "namesystem", fsnSpy, true);
      FieldUtils.writeDeclaredField(
          fsnSpy.getBlockManager(), "namesystem", fsnSpy, true);
      FieldUtils.writeDeclaredField(
          fsnSpy.getLeaseManager(), "fsnamesystem", fsnSpy, true);
      FieldUtils.writeDeclaredField(
          fsnSpy.getBlockManager().getDatanodeManager(),
          "namesystem", fsnSpy, true);
      FieldUtils.writeDeclaredField(
          BlockManagerTestUtil.getHeartbeatManager(fsnSpy.getBlockManager()),
          "namesystem", fsnSpy, true);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Cannot set spy FSNamesystem", e);
    } finally {
      fsnSpy.writeUnlock();
      fsnOld.writeUnlock();
    }
    return fsnSpy;
  }

  public static FSEditLog spyOnEditLog(NameNode nn) {
    FSEditLog spyEditLog = spy(nn.getNamesystem().getFSImage().getEditLog());
    DFSTestUtil.setEditLogForTesting(nn.getNamesystem(), spyEditLog);
    EditLogTailer tailer = nn.getNamesystem().getEditLogTailer();
    if (tailer != null) {
      tailer.setEditLog(spyEditLog);
    }
    return spyEditLog;
  }

  /**
   * Spy on EditLog to delay execution of doEditTransaction() for MkdirOp.
   */
  public static FSEditLog spyDelayMkDirTransaction(
      final NameNode nn, final long delay) {
    FSEditLog realEditLog = nn.getFSImage().getEditLog();
    FSEditLogAsync spyEditLog = (FSEditLogAsync) spy(realEditLog);
    DFSTestUtil.setEditLogForTesting(nn.getNamesystem(), spyEditLog);
    Answer<Boolean> ans = invocation -> {
      Thread.sleep(delay);
      return (Boolean) invocation.callRealMethod();
    };
    ArgumentMatcher<FSEditLogOp> am = argument -> argument.opCode == FSEditLogOpCodes.OP_MKDIR;
    doAnswer(ans).when(spyEditLog).doEditTransaction(ArgumentMatchers.argThat(am));
    return spyEditLog;
  }
}
