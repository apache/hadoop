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
package org.apache.hadoop.hdfs.server.federation.store.driver;

import static org.apache.hadoop.hdfs.server.federation.store.driver.impl.StateStoreFileBaseImpl.isOldTempRecord;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.util.Time;
import org.junit.Test;

/**
 * Tests for the State Store file based implementation.
 */
public class TestStateStoreFileBase {

  @Test
  public void testTempOld() {
    assertFalse(isOldTempRecord("test.txt"));
    assertFalse(isOldTempRecord("testfolder/test.txt"));

    long tnow = Time.now();
    String tmpFile1 = "test." + tnow + ".tmp";
    assertFalse(isOldTempRecord(tmpFile1));

    long told = Time.now() - TimeUnit.MINUTES.toMillis(1);
    String tmpFile2 = "test." + told + ".tmp";
    assertTrue(isOldTempRecord(tmpFile2));
  }
}