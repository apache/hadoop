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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.MD5MD5CRC32CastagnoliFileChecksum;
import org.apache.hadoop.fs.MD5MD5CRC32GzipFileChecksum;
import org.apache.hadoop.fs.CompositeCrcFileChecksum;
import org.apache.hadoop.fs.Options.ChecksumCombineMode;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;


import org.apache.hadoop.util.DataChecksum;

@RunWith(Parameterized.class)
public class TestFileChecksumHelper {

    private final ChecksumCombineMode combineMode;
    private final DataChecksum.Type crcType;
    private final Class<? extends FileChecksum> expectedChecksumClass;

    public TestFileChecksumHelper(ChecksumCombineMode combineMode,
                                  DataChecksum.Type crcType,
                                  Class<? extends FileChecksum> expectedChecksumClass) {
        this.combineMode = combineMode;
        this.crcType = crcType;
        this.expectedChecksumClass = expectedChecksumClass;
    }

    @Parameterized.Parameters(name = "{index}: Mode={0}, CRC={1}, Expect={2}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {ChecksumCombineMode.MD5MD5CRC, DataChecksum.Type.CRC32,  MD5MD5CRC32GzipFileChecksum.class},
                {ChecksumCombineMode.MD5MD5CRC, DataChecksum.Type.CRC32C, MD5MD5CRC32CastagnoliFileChecksum.class},
                {ChecksumCombineMode.COMPOSITE_CRC, DataChecksum.Type.CRC32, CompositeCrcFileChecksum.class},
                {ChecksumCombineMode.COMPOSITE_CRC, DataChecksum.Type.CRC32C, CompositeCrcFileChecksum.class},
        });
    }

    @Test
    public void testComputeReturnsCorrectChecksumForEmptyBlocks() throws Exception {
        Configuration conf = new Configuration();
        conf.set("dfs.checksum.combine.mode", combineMode.toString());
        conf.set("dfs.checksum.type", crcType.toString());

        LocatedBlocks emptyBlocks = new LocatedBlocks(); // No blocks

        DFSClient mockClient = mock(DFSClient.class);
        ClientProtocol mockNamenode = mock(ClientProtocol.class);

        FileChecksumHelper.ReplicatedFileChecksumComputer checker =
                new FileChecksumHelper.ReplicatedFileChecksumComputer(
                        "/empty-file", 0L, emptyBlocks,
                        mockNamenode, mockClient, combineMode
                );

        checker.setCrcType(crcType);
        checker.setBytesPerCRC(512);
        checker.compute();
        FileChecksum checksum = checker.getFileChecksum();

        assertNotNull("Checksum must not be null", checksum);
        assertEquals("Unexpected checksum class", expectedChecksumClass, checksum.getClass());
    }
}
