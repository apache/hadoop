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
package org.apache.hadoop.fs;

import static org.junit.Assert.*;

import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.util.DataChecksum;

import org.junit.Test;

public class TestFsOptions {

  @Test
  public void testProcessChecksumOpt() {
    ChecksumOpt defaultOpt = new ChecksumOpt(DataChecksum.Type.CRC32, 512);
    ChecksumOpt finalOpt;

    // Give a null 
    finalOpt = ChecksumOpt.processChecksumOpt(defaultOpt, null);
    checkParams(defaultOpt, finalOpt);

    // null with bpc
    finalOpt = ChecksumOpt.processChecksumOpt(defaultOpt, null, 1024);
    checkParams(DataChecksum.Type.CRC32, 1024, finalOpt);

    ChecksumOpt myOpt = new ChecksumOpt();

    // custom with unspecified parameters
    finalOpt = ChecksumOpt.processChecksumOpt(defaultOpt, myOpt);
    checkParams(defaultOpt, finalOpt);

    myOpt = new ChecksumOpt(DataChecksum.Type.CRC32C, 2048);

    // custom config
    finalOpt = ChecksumOpt.processChecksumOpt(defaultOpt, myOpt);
    checkParams(DataChecksum.Type.CRC32C, 2048, finalOpt);

    // custom config + bpc
    finalOpt = ChecksumOpt.processChecksumOpt(defaultOpt, myOpt, 4096);
    checkParams(DataChecksum.Type.CRC32C, 4096, finalOpt);
  }

  private void checkParams(ChecksumOpt expected, ChecksumOpt obtained) {
    assertEquals(expected.getChecksumType(), obtained.getChecksumType());
    assertEquals(expected.getBytesPerChecksum(), obtained.getBytesPerChecksum());
  }

  private void checkParams(DataChecksum.Type type, int bpc, ChecksumOpt obtained) {
    assertEquals(type, obtained.getChecksumType());
    assertEquals(bpc, obtained.getBytesPerChecksum());
  }
}
