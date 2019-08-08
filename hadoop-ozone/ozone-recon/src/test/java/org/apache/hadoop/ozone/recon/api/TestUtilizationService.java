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
import org.apache.hadoop.ozone.recon.tasks.FileSizeCountTask;
import org.hadoop.ozone.recon.schema.tables.daos.FileCountBySizeDao;
import org.hadoop.ozone.recon.schema.tables.pojos.FileCountBySize;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyLong;
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
  private int maxBinSize = 42;

  private List<FileCountBySize> setUpResultList() {
    List<FileCountBySize> resultList = new ArrayList<>();
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
    return resultList;
  }

  @Test
  public void testGetFileCounts() throws IOException {
    List<FileCountBySize> resultList = setUpResultList();

    utilizationService = mock(UtilizationService.class);
    when(utilizationService.getFileCounts()).thenCallRealMethod();
    when(utilizationService.getDao()).thenReturn(fileCountBySizeDao);
    when(fileCountBySizeDao.findAll()).thenReturn(resultList);

    Response response = utilizationService.getFileCounts();
    // get result list from Response entity
    List<FileCountBySize> responseList =
        (List<FileCountBySize>) response.getEntity();

    verify(utilizationService, times(1)).getFileCounts();
    verify(fileCountBySizeDao, times(1)).findAll();

    FileSizeCountTask fileSizeCountTask = mock(FileSizeCountTask.class);
    when(fileSizeCountTask.getMaxFileSizeUpperBound()).
        thenReturn(1125899906842624L);
    when(fileSizeCountTask.getMaxBinSize()).thenReturn(maxBinSize);
    when(fileSizeCountTask.calculateBinIndex(anyLong())).thenCallRealMethod();
    assertEquals(maxBinSize, responseList.size());

    long fileSize = 4096L;              // 4KB
    int index =  fileSizeCountTask.calculateBinIndex(fileSize);

    long count = responseList.get(index).getCount();
    assertEquals(index, count);

    fileSize = 1125899906842624L;       // 1PB
    index = fileSizeCountTask.calculateBinIndex(fileSize);
    count = responseList.get(index).getCount();
    //last extra bin for files >= 1PB
    assertEquals(maxBinSize - 1, index);
    assertEquals(index, count);

    fileSize = 1025L;                   // 1 KB + 1B
    index = fileSizeCountTask.calculateBinIndex(fileSize);
    count = responseList.get(index).getCount();
    assertEquals(index, count);

    fileSize = 25L;
    index = fileSizeCountTask.calculateBinIndex(fileSize);
    count = responseList.get(index).getCount();
    assertEquals(index, count);

    fileSize = 1125899906842623L;       // 1PB - 1B
    index = fileSizeCountTask.calculateBinIndex(fileSize);
    count = responseList.get(index).getCount();
    assertEquals(index, count);

    fileSize = 1125899906842624L * 4;       // 4 PB
    index = fileSizeCountTask.calculateBinIndex(fileSize);
    count = responseList.get(index).getCount();
    //last extra bin for files >= 1PB
    assertEquals(maxBinSize - 1, index);
    assertEquals(index, count);

    fileSize = Long.MAX_VALUE;    // extra bin
    index = fileSizeCountTask.calculateBinIndex(fileSize);
    count = responseList.get(index).getCount();
    //last extra bin for files >= 1PB
    assertEquals(maxBinSize - 1, index);
    assertEquals(index, count);
  }
}
