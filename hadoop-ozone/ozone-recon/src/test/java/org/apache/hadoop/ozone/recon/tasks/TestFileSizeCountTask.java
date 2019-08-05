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

package org.apache.hadoop.ozone.recon.tasks;

import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.recon.AbstractOMMetadataManagerTest;
import org.apache.hadoop.utils.db.TypedTable;
import org.junit.Test;

import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 * Unit test for Container Key mapper task.
 */
@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "javax.net.ssl.*"})
@PrepareForTest(OmKeyInfo.class)

public class TestFileSizeCountTask extends AbstractOMMetadataManagerTest {
  @Test
  public void testCalculateBinIndex() {
    FileSizeCountTask fileSizeCountTask = mock(FileSizeCountTask.class);

    when(fileSizeCountTask.getMaxFileSizeUpperBound()).
        thenReturn(1125899906842624L);    // 1 PB
    when(fileSizeCountTask.getOneKB()).thenReturn(1024L);
    when(fileSizeCountTask.getMaxBinSize()).thenReturn(42);

    doCallRealMethod().when(fileSizeCountTask).setMaxBinSize();
    when(fileSizeCountTask.calculateBinIndex(anyLong())).thenCallRealMethod();

    fileSizeCountTask.setMaxBinSize();

    long fileSize = 1024L;            // 1 KB
    int binIndex = fileSizeCountTask.calculateBinIndex(fileSize);
    assertEquals(1L, binIndex);

    fileSize = 1023L;
    binIndex = fileSizeCountTask.calculateBinIndex(fileSize);
    assertEquals(0L, binIndex);

    fileSize = 562949953421312L;      // 512 TB
    binIndex = fileSizeCountTask.calculateBinIndex(fileSize);
    assertEquals(40L, binIndex);

    fileSize = 562949953421313L;      // (512 TB + 1B)
    binIndex = fileSizeCountTask.calculateBinIndex(fileSize);
    assertEquals(40L, binIndex);

    fileSize = 562949953421311L;      // (512 TB - 1B)
    binIndex = fileSizeCountTask.calculateBinIndex(fileSize);
    assertEquals(39L, binIndex);

    fileSize = 1125899906842624L;      // 1 PB - last (extra) bin
    binIndex = fileSizeCountTask.calculateBinIndex(fileSize);
    assertEquals(41L, binIndex);

    fileSize = 100000L;
    binIndex = fileSizeCountTask.calculateBinIndex(fileSize);
    assertEquals(7L, binIndex);
  }

  @Test
  public void testFileCountBySizeReprocess() throws IOException {
    OmKeyInfo omKeyInfo1 = mock(OmKeyInfo.class);
    given(omKeyInfo1.getKeyName()).willReturn("key1");
    given(omKeyInfo1.getDataSize()).willReturn(1000L);

    OMMetadataManager omMetadataManager = mock(OmMetadataManagerImpl.class);
    TypedTable<String, OmKeyInfo> keyTable = mock(TypedTable.class);


    TypedTable.TypedTableIterator mockKeyIter = mock(TypedTable
        .TypedTableIterator.class);
    TypedTable.TypedKeyValue mockKeyValue = mock(
        TypedTable.TypedKeyValue.class);

    when(keyTable.iterator()).thenReturn(mockKeyIter);
    when(omMetadataManager.getKeyTable()).thenReturn(keyTable);
    when(mockKeyIter.hasNext()).thenReturn(true).thenReturn(false);
    when(mockKeyIter.next()).thenReturn(mockKeyValue);
    when(mockKeyValue.getValue()).thenReturn(omKeyInfo1);

    FileSizeCountTask fileSizeCountTask = mock(FileSizeCountTask.class);

    when(fileSizeCountTask.getMaxFileSizeUpperBound()).
        thenReturn(4096L);
    when(fileSizeCountTask.getOneKB()).thenReturn(1024L);
    when(fileSizeCountTask.getMaxBinSize()).thenReturn(3);

    when(fileSizeCountTask.reprocess(omMetadataManager)).thenCallRealMethod();
    doCallRealMethod().when(fileSizeCountTask).
        fetchUpperBoundCount("reprocess");
    doCallRealMethod().when(fileSizeCountTask).setMaxBinSize();

    //call reprocess()
    fileSizeCountTask.reprocess(omMetadataManager);

    // verify invocation of calls for reprocess()
    verify(fileSizeCountTask, times(1)).
        fetchUpperBoundCount("reprocess");
    verify(fileSizeCountTask,
        times(1)).countFileSize(omKeyInfo1);
    verify(fileSizeCountTask,
        times(1)).populateFileCountBySizeDB();
  }
}
