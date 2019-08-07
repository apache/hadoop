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

package org.apache.hadoop.ozone.recon.api;

import org.apache.hadoop.ozone.recon.ReconUtils;
import org.hadoop.ozone.recon.schema.tables.daos.FileCountBySizeDao;
import org.hadoop.ozone.recon.schema.tables.pojos.FileCountBySize;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Test for File size count service.
 */
@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "javax.net.ssl.*"})
@PrepareForTest(ReconUtils.class)
public class TestUtilizationService {
  private UtilizationService utilizationService;
  @Mock private FileCountBySizeDao fileCountBySizeDao;
  private List<FileCountBySize> resultList = new ArrayList<>();
  private int oneKb = 1024;
  private int maxBinSize = 42;

  public void setUpResultList() {
    for (int i = 0; i < maxBinSize; i++) {
      if (i == maxBinSize - 1) {
        // for last bin file count is 41.
        resultList.add(new FileCountBySize(Long.MAX_VALUE, (long) i));
      } else {
        // count of files of upperBound is equal to it's index.
        resultList.add(new FileCountBySize((long) Math.pow(2, (10+i)),
            (long) i));
      }
    }
  }

  @Test
  public void testGetFileCounts() throws IOException {
    setUpResultList();

    utilizationService = mock(UtilizationService.class);
    when(utilizationService.getFileCounts()).thenCallRealMethod();
    when(utilizationService.getDao()).thenReturn(fileCountBySizeDao);
    when(fileCountBySizeDao.findAll()).thenReturn(resultList);

    utilizationService.getFileCounts();
    verify(utilizationService, times(1)).getFileCounts();
    verify(fileCountBySizeDao, times(1)).findAll();

    assertEquals(maxBinSize, resultList.size());
    long fileSize = 4096L;              // 4KB
    int index =  findIndex(fileSize);
    long count = resultList.get(index).getCount();
    assertEquals(index, count);

    fileSize = 1125899906842624L;       // 1PB
    index = findIndex(fileSize);
    count = resultList.get(index).getCount();
    assertEquals(maxBinSize - 1, index);
    assertEquals(index, count);

    fileSize = 1025L;                   // 1 KB + 1B
    index = findIndex(fileSize);
    count = resultList.get(index).getCount(); //last extra bin for files >= 1PB
    assertEquals(index, count);

    fileSize = 25L;
    index = findIndex(fileSize);
    count = resultList.get(index).getCount();
    assertEquals(index, count);

    fileSize = 1125899906842623L;       // 1PB - 1B
    index = findIndex(fileSize);
    count = resultList.get(index).getCount();
    assertEquals(index, count);

    fileSize = 1125899906842624L * 4;       // 4 PB
    index = findIndex(fileSize);
    count = resultList.get(index).getCount();
    assertEquals(maxBinSize - 1, index);
    assertEquals(index, count);
  }

  public int findIndex(long dataSize) {
    if (dataSize > Math.pow(2, (maxBinSize + 10 - 2))) {  // 1 PB = 2 ^ 50
      return maxBinSize - 1;
    }
    int index = 0;
    while(dataSize != 0) {
      dataSize >>= 1;
      index += 1;
    }
    // The smallest file size being tracked for count
    // is 1 KB i.e. 1024 = 2 ^ 10.
    return index < 10 ? 0 : index - 10;
  }
}
