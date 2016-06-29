package org.apache.hadoop.hdfs;

import com.google.common.collect.Lists;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.hadoop.hdfs.inotify.EventBatch;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
public class TestBlockLevelCopying {

  private MiniDFSCluster clusterA;
  private int blockSize;

  @Before
  public void setUp() throws Exception {
    blockSize = 64;
    Configuration conf =  new Configuration();
    conf.setInt("dfs.namenode.fs-limits.min-block-size", 1);
    conf.setInt("dfs.bytes-per-checksum", blockSize);
    conf.setInt("dfs.blocksize", blockSize);
    clusterA = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
  }

  @Test
  public void testCopyingABlockOverWithINotify() throws Exception {
    DFSClient clientA = new DFSClient(clusterA.getURI(), clusterA.getConfiguration(0));
    DFSInotifyEventInputStream stream = clientA.getInotifyEventStream();

    String randomFileName = UUID.randomUUID().toString();
    Path path = new Path("/" + randomFileName);
    createFile(clusterA, path);
    DistributedFileSystem clusterAFS = clusterA.getFileSystem();
    FileStatus fileStatus = clusterAFS.getFileStatus(path);
    BlockLocation[] locations = clusterAFS.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
    assertEquals(2, locations.length); // should be two blocks



    // now get the inotify stream to get extended blocks
    List<ExtendedBlock> extendedBlocks = getAddedBlocksFromEventStream(stream);
    assertEquals(2, extendedBlocks.size()); // still have 2 blocks


    // lets grab the Located Block from the NN and see what fun can happen
    List<LocatedBlock> locatedBlocks = Lists.newArrayList();
    for (ExtendedBlock extendedBlock : extendedBlocks) {
      locatedBlocks.add(clusterA.getNameNode().getRpcServer().getBlockLocation(extendedBlock));
    }
    assertEquals(2, locatedBlocks.size()); // still have 2 blocks

    // lets grab the first blocks data
    DFSInputStream dfsInputStream = clientA.open(path.toString(), blockSize, true);
    byte[] result = new byte[blockSize];
    dfsInputStream.read(result);
    String expected = Hex.encodeHexString(result);

    // Now here is where the shit show starts, cover your kids ears and eyes
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    clientA.copyBlock(locatedBlocks.get(0), baos);
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    String actual = Hex.encodeHexString(IOUtils.toByteArray(bais));

    assertEquals(expected, actual);


  }

  private void createFile(MiniDFSCluster cluster, Path file) throws Exception {
    DistributedFileSystem fs = cluster.getFileSystem();
    FSDataOutputStream outputStream = fs.create(file, true, 4, (short) 3, blockSize);
    outputStream.close();
    outputStream = fs.append(file);
    int numInts = blockSize / 4;
    int i = 0;
    while (i < numInts) {
      outputStream.writeInt(0);
      i++;
    }
    int j = 0;
    while (j < numInts) {
      outputStream.writeInt(1);
      j++;
    }
    outputStream.close();
  }

  private List<ExtendedBlock> getAddedBlocksFromEventStream(DFSInotifyEventInputStream stream) throws Exception {
    List<ExtendedBlock> extendedBlocks = Lists.newArrayList();
    EventBatch eventBatch;
    while ((eventBatch = stream.poll()) != null) {
      for (Event event : eventBatch.getEvents()) {
        if (event.getEventType().equals(Event.EventType.ADD_BLOCK)) {
          Event.AddBlockEvent addBlockEvent = (Event.AddBlockEvent) event;
          extendedBlocks
              .add(new ExtendedBlock(addBlockEvent.getBlockPoolId(), addBlockEvent.getLastBlock()));
        }
      }
    }
    return extendedBlocks;
  }
}
