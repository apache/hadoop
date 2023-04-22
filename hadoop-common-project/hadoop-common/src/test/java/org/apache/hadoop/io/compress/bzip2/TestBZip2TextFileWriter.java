/*
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
package org.apache.hadoop.io.compress.bzip2;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.hadoop.io.compress.bzip2.BZip2TextFileWriter.BLOCK_SIZE;
import static org.junit.Assert.assertEquals;

public final class TestBZip2TextFileWriter {

  private static final byte[] DELIMITER = new byte[] {'\0'};

  private ByteArrayOutputStream rawOut;
  private BZip2TextFileWriter writer;

  @Before
  public void setUp() throws Exception {
    rawOut = new ByteArrayOutputStream();
    writer = new BZip2TextFileWriter(rawOut);
  }

  @After
  public void tearDown() throws Exception {
    rawOut = null;
    writer.close();
  }

  @Test
  public void writingSingleBlockSizeOfData() throws Exception {
    writer.writeRecord(BLOCK_SIZE, DELIMITER);
    writer.close();

    List<Long> nextBlocks = getNextBlockMarkerOffsets();
    assertEquals(0, nextBlocks.size());
  }

  @Test
  public void justExceedingBeyondBlockSize() throws Exception {
    writer.writeRecord(BLOCK_SIZE + 1, DELIMITER);
    writer.close();

    List<Long> nextBlocks = getNextBlockMarkerOffsets();
    assertEquals(1, nextBlocks.size());
  }

  @Test
  public void writingTwoBlockSizesOfData() throws Exception {
    writer.writeRecord(2 * BLOCK_SIZE, DELIMITER);
    writer.close();

    List<Long> nextBlocks = getNextBlockMarkerOffsets();
    assertEquals(1, nextBlocks.size());
  }

  @Test
  public void justExceedingBeyondTwoBlocks() throws Exception {
    writer.writeRecord(2 * BLOCK_SIZE + 1, DELIMITER);
    writer.close();

    List<Long> nextBlocks = getNextBlockMarkerOffsets();
    assertEquals(2, nextBlocks.size());
  }

  private List<Long> getNextBlockMarkerOffsets() throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(rawOut.toByteArray());
    return BZip2Utils.getNextBlockMarkerOffsets(in);
  }
}