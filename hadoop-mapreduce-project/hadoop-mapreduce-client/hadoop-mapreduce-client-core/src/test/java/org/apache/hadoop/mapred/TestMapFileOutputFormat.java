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

package org.apache.hadoop.mapred;

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.MapFile.Reader;
import org.junit.After;
import org.junit.Test;
import org.mockito.Mockito;

public class TestMapFileOutputFormat {

  @SuppressWarnings("static-access")
  @Test
  public void testPartitionerShouldNotBeCalledWhenOneReducerIsPresent()
      throws Exception {
    MapFileOutputFormat outputFormat = new MapFileOutputFormat();
    Reader reader = Mockito.mock(Reader.class);
    Reader[] readers = new Reader[]{reader};
    outputFormat.getEntry(readers, new MyPartitioner(), new Text(), new Text());
    assertTrue(!MyPartitioner.isGetPartitionCalled());
  }

  @After
  public void tearDown() throws Exception {
    MyPartitioner.setGetPartitionCalled(false);
  }
  private static class MyPartitioner
      implements
        Partitioner<WritableComparable, Writable> {
    private static boolean getPartitionCalled = false;

    @Override
    public int getPartition(WritableComparable key, Writable value,
        int numPartitions) {
      setGetPartitionCalled(true);
      return -1;
    }

    public static boolean isGetPartitionCalled() {
      return getPartitionCalled;
    }

    @Override
    public void configure(JobConf job) {
    }

    public static void setGetPartitionCalled(boolean getPartitionCalled) {
      MyPartitioner.getPartitionCalled = getPartitionCalled;
    }
  }
}
