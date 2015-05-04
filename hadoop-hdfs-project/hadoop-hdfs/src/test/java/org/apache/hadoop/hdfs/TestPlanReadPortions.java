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
package org.apache.hadoop.hdfs;

import org.junit.Test;

import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import static org.apache.hadoop.hdfs.util.StripedBlockUtil.ReadPortion;
import static org.junit.Assert.*;

public class TestPlanReadPortions {

  // We only support this as num of data blocks. It might be good enough for now
  // for the purpose, even not flexible yet for any number in a schema.
  private final short GROUP_SIZE = 3;
  private final int CELLSIZE = 128 * 1024;

  private void testPlanReadPortions(int startInBlk, int length,
      int bufferOffset, int[] readLengths, int[] offsetsInBlock,
      int[][] bufferOffsets, int[][] bufferLengths) {
    ReadPortion[] results = StripedBlockUtil.planReadPortions(GROUP_SIZE,
        CELLSIZE, startInBlk, length, bufferOffset);
    assertEquals(GROUP_SIZE, results.length);

    for (int i = 0; i < GROUP_SIZE; i++) {
      assertEquals(readLengths[i], results[i].getReadLength());
      assertEquals(offsetsInBlock[i], results[i].getStartOffsetInBlock());
      final int[] bOffsets = results[i].getOffsets();
      assertArrayEquals(bufferOffsets[i], bOffsets);
      final int[] bLengths = results[i].getLengths();
      assertArrayEquals(bufferLengths[i], bLengths);
    }
  }

  /**
   * Test {@link StripedBlockUtil#planReadPortions}
   */
  @Test
  public void testPlanReadPortions() {
    /**
     * start block offset is 0, read cellSize - 10
     */
    testPlanReadPortions(0, CELLSIZE - 10, 0,
        new int[]{CELLSIZE - 10, 0, 0}, new int[]{0, 0, 0},
        new int[][]{new int[]{0}, new int[]{}, new int[]{}},
        new int[][]{new int[]{CELLSIZE - 10}, new int[]{}, new int[]{}});

    /**
     * start block offset is 0, read 3 * cellSize
     */
    testPlanReadPortions(0, GROUP_SIZE * CELLSIZE, 0,
        new int[]{CELLSIZE, CELLSIZE, CELLSIZE}, new int[]{0, 0, 0},
        new int[][]{new int[]{0}, new int[]{CELLSIZE}, new int[]{CELLSIZE * 2}},
        new int[][]{new int[]{CELLSIZE}, new int[]{CELLSIZE}, new int[]{CELLSIZE}});

    /**
     * start block offset is 0, read cellSize + 10
     */
    testPlanReadPortions(0, CELLSIZE + 10, 0,
        new int[]{CELLSIZE, 10, 0}, new int[]{0, 0, 0},
        new int[][]{new int[]{0}, new int[]{CELLSIZE}, new int[]{}},
        new int[][]{new int[]{CELLSIZE}, new int[]{10}, new int[]{}});

    /**
     * start block offset is 0, read 5 * cellSize + 10, buffer start offset is 100
     */
    testPlanReadPortions(0, 5 * CELLSIZE + 10, 100,
        new int[]{CELLSIZE * 2, CELLSIZE * 2, CELLSIZE + 10}, new int[]{0, 0, 0},
        new int[][]{new int[]{100, 100 + CELLSIZE * GROUP_SIZE},
            new int[]{100 + CELLSIZE, 100 + CELLSIZE * 4},
            new int[]{100 + CELLSIZE * 2, 100 + CELLSIZE * 5}},
        new int[][]{new int[]{CELLSIZE, CELLSIZE},
            new int[]{CELLSIZE, CELLSIZE},
            new int[]{CELLSIZE, 10}});

    /**
     * start block offset is 2, read 3 * cellSize
     */
    testPlanReadPortions(2, GROUP_SIZE * CELLSIZE, 100,
        new int[]{CELLSIZE, CELLSIZE, CELLSIZE},
        new int[]{2, 0, 0},
        new int[][]{new int[]{100, 100 + GROUP_SIZE * CELLSIZE - 2},
            new int[]{100 + CELLSIZE - 2},
            new int[]{100 + CELLSIZE * 2 - 2}},
        new int[][]{new int[]{CELLSIZE - 2, 2},
            new int[]{CELLSIZE},
            new int[]{CELLSIZE}});

    /**
     * start block offset is 2, read 3 * cellSize + 10
     */
    testPlanReadPortions(2, GROUP_SIZE * CELLSIZE + 10, 0,
        new int[]{CELLSIZE + 10, CELLSIZE, CELLSIZE},
        new int[]{2, 0, 0},
        new int[][]{new int[]{0, GROUP_SIZE * CELLSIZE - 2},
            new int[]{CELLSIZE - 2},
            new int[]{CELLSIZE * 2 - 2}},
        new int[][]{new int[]{CELLSIZE - 2, 12},
            new int[]{CELLSIZE},
            new int[]{CELLSIZE}});

    /**
     * start block offset is cellSize * 2 - 1, read 5 * cellSize + 10
     */
    testPlanReadPortions(CELLSIZE * 2 - 1, 5 * CELLSIZE + 10, 0,
        new int[]{CELLSIZE * 2, CELLSIZE + 10, CELLSIZE * 2},
        new int[]{CELLSIZE, CELLSIZE - 1, 0},
        new int[][]{new int[]{CELLSIZE + 1, 4 * CELLSIZE + 1},
            new int[]{0, 2 * CELLSIZE + 1, 5 * CELLSIZE + 1},
            new int[]{1, 3 * CELLSIZE + 1}},
        new int[][]{new int[]{CELLSIZE, CELLSIZE},
            new int[]{1, CELLSIZE, 9},
            new int[]{CELLSIZE, CELLSIZE}});

    /**
     * start block offset is cellSize * 6 - 1, read 7 * cellSize + 10
     */
    testPlanReadPortions(CELLSIZE * 6 - 1, 7 * CELLSIZE + 10, 0,
        new int[]{CELLSIZE * 3, CELLSIZE * 2 + 9, CELLSIZE * 2 + 1},
        new int[]{CELLSIZE * 2, CELLSIZE * 2, CELLSIZE * 2 - 1},
        new int[][]{new int[]{1, 3 * CELLSIZE + 1, 6 * CELLSIZE + 1},
            new int[]{CELLSIZE + 1, 4 * CELLSIZE + 1, 7 * CELLSIZE + 1},
            new int[]{0, 2 * CELLSIZE + 1, 5 * CELLSIZE + 1}},
        new int[][]{new int[]{CELLSIZE, CELLSIZE, CELLSIZE},
            new int[]{CELLSIZE, CELLSIZE, 9},
            new int[]{1, CELLSIZE, CELLSIZE}});
  }
}
