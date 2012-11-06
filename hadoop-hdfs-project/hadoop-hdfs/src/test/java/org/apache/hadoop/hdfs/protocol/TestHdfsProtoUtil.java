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
package org.apache.hadoop.hdfs.protocol;

import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.util.DataChecksum;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestHdfsProtoUtil {
  @Test
  public void testChecksumTypeProto() {
    assertEquals(DataChecksum.Type.NULL,
        HdfsProtoUtil.fromProto(HdfsProtos.ChecksumTypeProto.CHECKSUM_NULL));
    assertEquals(DataChecksum.Type.CRC32,
        HdfsProtoUtil.fromProto(HdfsProtos.ChecksumTypeProto.CHECKSUM_CRC32));
    assertEquals(DataChecksum.Type.CRC32C,
        HdfsProtoUtil.fromProto(HdfsProtos.ChecksumTypeProto.CHECKSUM_CRC32C));
    assertEquals(HdfsProtoUtil.toProto(DataChecksum.Type.NULL),
        HdfsProtos.ChecksumTypeProto.CHECKSUM_NULL);
    assertEquals(HdfsProtoUtil.toProto(DataChecksum.Type.CRC32),
        HdfsProtos.ChecksumTypeProto.CHECKSUM_CRC32);
    assertEquals(HdfsProtoUtil.toProto(DataChecksum.Type.CRC32C),
        HdfsProtos.ChecksumTypeProto.CHECKSUM_CRC32C);
  }
}
