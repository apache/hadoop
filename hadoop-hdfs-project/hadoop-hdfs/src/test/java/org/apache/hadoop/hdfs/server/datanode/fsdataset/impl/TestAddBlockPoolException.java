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
package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.junit.Test;

/**
 * Tests to ensure AddBlockPoolException behaves correctly when additional
 * exceptions are merged, as this exception is a wrapper for multiple
 * exceptions and hence carries some additional logic.
 */
public class TestAddBlockPoolException {

  @Test
  public void testHasExeceptionsReturnsCorrectValue() {
    AddBlockPoolException e = new AddBlockPoolException();
    assertFalse(e.hasExceptions());

    FsVolumeImpl fakeVol = mock(FsVolumeImpl.class);
    ConcurrentHashMap<FsVolumeSpi, IOException> vols =
        new ConcurrentHashMap<FsVolumeSpi, IOException>();
    vols.put(fakeVol, new IOException("Error 1"));
    e = new AddBlockPoolException(vols);
    assertTrue(e.hasExceptions());
  }

  @Test
  public void testExceptionsCanBeMerged() {
    FsVolumeImpl vol1 = mock(FsVolumeImpl.class);
    FsVolumeImpl vol2 = mock(FsVolumeImpl.class);

    ConcurrentHashMap<FsVolumeSpi, IOException> first =
        new ConcurrentHashMap<FsVolumeSpi, IOException>();
    ConcurrentHashMap<FsVolumeSpi, IOException> second =
        new ConcurrentHashMap<FsVolumeSpi, IOException>();
    first.put(vol1, new IOException("First Error"));
    second.put(vol1, new IOException("Second Error"));
    second.put(vol2, new IOException("V2 Error"));

    AddBlockPoolException e = new AddBlockPoolException(first);
    e.mergeException(new AddBlockPoolException(second));

    // Ensure there are two exceptions in the map
    assertEquals(e.getFailingVolumes().size(), 2);
    // Ensure the first exception added for a volume is the one retained
    // when multiple errors
    assertEquals(e.getFailingVolumes().get(vol1).getMessage(), "First Error");
    assertEquals(e.getFailingVolumes().get(vol2).getMessage(), "V2 Error");
  }

  @Test
  public void testEmptyExceptionsCanBeMerged() {
    AddBlockPoolException e = new AddBlockPoolException();
    e.mergeException(new AddBlockPoolException());
    assertFalse(e.hasExceptions());
  }

}