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
package org.apache.hadoop.hbase.util.hbck;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.HBaseFsck;
import org.apache.hadoop.hbase.util.HBaseFsck.ErrorReporter.ERROR_CODE;

public class HbckTestingUtil {
  public static HBaseFsck doFsck(Configuration conf, boolean fix) throws Exception {
    HBaseFsck fsck = new HBaseFsck(conf);
    fsck.connect();
    fsck.displayFullReport(); // i.e. -details
    fsck.setTimeLag(0);
    fsck.setFixErrors(fix);
    fsck.doWork();
    return fsck;
  }

  public static void assertNoErrors(HBaseFsck fsck) throws Exception {
    List<ERROR_CODE> errs = fsck.getErrors().getErrorList();
    assertEquals(0, errs.size());
  }

  public static void assertErrors(HBaseFsck fsck, ERROR_CODE[] expectedErrors) {
    List<ERROR_CODE> errs = fsck.getErrors().getErrorList();
    assertEquals(Arrays.asList(expectedErrors), errs);
  }
}
