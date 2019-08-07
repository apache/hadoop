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
  private int maxBinSize = 41;

  public void setUpResultList() {
    for(int i = 0; i < 41; i++){
      resultList.add(new FileCountBySize((long) Math.pow(2, (10+i)), (long) i));
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

    assertEquals(41, resultList.size());
    long fileSize = 4096L;
    int index =  findIndex(fileSize);
    long count = resultList.get(index).getCount();
    assertEquals(index, count);

    fileSize = 1125899906842624L;
    index = findIndex(fileSize);
    if (index == Integer.MIN_VALUE) {
      throw new IOException("File Size larger than permissible file size");
    }

    fileSize = 1025L;
    index = findIndex(fileSize);
    count = resultList.get(index).getCount();
    assertEquals(index, count);

    fileSize = 25L;
    index = findIndex(fileSize);
    count = resultList.get(index).getCount();
    assertEquals(index, count);
  }

  public int findIndex(long dataSize) {
    int logValue = (int) Math.ceil(Math.log(dataSize)/Math.log(2));
    if (logValue < 10) {
      return 0;
    } else {
      int index = logValue - 10;
      if (index > maxBinSize) {
        return Integer.MIN_VALUE;
      }
      return (dataSize % oneKb == 0) ? index + 1 : index;
    }
  }
}
