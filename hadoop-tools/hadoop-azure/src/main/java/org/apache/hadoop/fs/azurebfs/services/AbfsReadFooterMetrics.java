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
package org.apache.hadoop.fs.azurebfs.services;

import java.util.concurrent.atomic.AtomicLong;

public class AbfsReadFooterMetrics {
  private boolean isParquetFile;
  private String sizeReadByFirstRead;
  private String offsetDiffBetweenFirstAndSecondRead;
  private AtomicLong fileLength;
  private double avgFileLength;

  public AbfsReadFooterMetrics() {
    this.fileLength = new AtomicLong();
  }

  public boolean getIsParquetFile() {
    return isParquetFile;
  }

  public void setParquetFile(final boolean parquetFile) {
    isParquetFile = parquetFile;
  }

  public String getSizeReadByFirstRead() {
    return sizeReadByFirstRead;
  }

  public void setSizeReadByFirstRead(final String sizeReadByFirstRead) {
    this.sizeReadByFirstRead = sizeReadByFirstRead;
  }

  public String getOffsetDiffBetweenFirstAndSecondRead() {
    return offsetDiffBetweenFirstAndSecondRead;
  }

  public void setOffsetDiffBetweenFirstAndSecondRead(final String offsetDiffBetweenFirstAndSecondRead) {
    this.offsetDiffBetweenFirstAndSecondRead
        = offsetDiffBetweenFirstAndSecondRead;
  }

  public AtomicLong getFileLength() {
    return fileLength;
  }

  public double getAvgFileLength() {
    return avgFileLength;
  }

  public void setAvgFileLength(final double avgFileLength) {
    this.avgFileLength = avgFileLength;
  }
}
