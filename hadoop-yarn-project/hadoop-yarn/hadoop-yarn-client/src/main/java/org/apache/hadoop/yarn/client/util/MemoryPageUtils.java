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
package org.apache.hadoop.yarn.client.util;

import java.util.ArrayList;
import java.util.List;

/**
 * This is a memory paging utility that is used to paginate a dataset.
 *
 * This class is designed to support batch entry queue policies.
 */
public class MemoryPageUtils<T> {
  private List<T> dataList;
  private int pageSize;

  /**
   * MemoryPageUtils constructor.
   *
   * @param pageSize Number of records returned per page.
   */
  public MemoryPageUtils(int pageSize) {
    this.pageSize = pageSize;
    this.dataList = new ArrayList<>();
  }

  public void addToMemory(T data) {
    dataList.add(data);
  }

  public List<T> readFromMemory(int pageNumber) {
    int startIndex = pageNumber * pageSize;
    int endIndex = Math.min(startIndex + pageSize, dataList.size());
    if (startIndex >= dataList.size()) {
      return null;
    }
    return dataList.subList(startIndex, endIndex);
  }

  public int getPages() {
    return (dataList.size() / pageSize + 1);
  }
}
