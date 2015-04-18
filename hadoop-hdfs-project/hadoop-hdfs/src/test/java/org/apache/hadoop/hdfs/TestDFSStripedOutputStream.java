package org.apache.hadoop.hdfs;

import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.net.TcpPeerServer;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;

import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.erasurecode.rawcoder.RSRawEncoder;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureEncoder;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.token.Token;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public class TestDFSStripedOutputStream {
  private int dataBlocks = HdfsConstants.NUM_DATA_BLOCKS;
  private int parityBlocks = HdfsConstants.NUM_PARITY_BLOCKS;

  private MiniDFSCluster cluster;
  private Configuration conf = new Configuration();
  private DistributedFileSystem fs;
  private final int cellSize = HdfsConstants.BLOCK_STRIPED_CELL_SIZE;
  private final int stripesPerBlock = 4;
  private final int blockSize = cellSize * stripesPerBlock;
  private final RawErasureEncoder encoder = new RSRawEncoder();

  @Before
  public void setup() throws IOException {
    int numDNs = dataBlocks + parityBlocks + 2;
    Configuration conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDNs).build();
    cluster.getFileSystem().getClient().createErasureCodingZone("/", null);
    fs = cluster.getFileSystem();
    encoder.initialize(dataBlocks, parityBlocks, cellSize);
  }

  @After
  public void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void TestFileEmpty() throws IOException {
    testOneFile("/EmptyFile", 0);
  }

  @Test
  public void TestFileSmallerThanOneCell1() throws IOException {
    testOneFile("/SmallerThanOneCell", 1);
  }

  @Test
  public void TestFileSmallerThanOneCell2() throws IOException {
    testOneFile("/SmallerThanOneCell", cellSize - 1);
  }

  @Test
  public void TestFileEqualsWithOneCell() throws IOException {
    testOneFile("/EqualsWithOneCell", cellSize);
  }

  @Test
  public void TestFileSmallerThanOneStripe1() throws IOException {
    testOneFile("/SmallerThanOneStripe", cellSize * dataBlocks - 1);
  }

  @Test
  public void TestFileSmallerThanOneStripe2() throws IOException {
    testOneFile("/SmallerThanOneStripe", cellSize + 123);
  }

  @Test
  public void TestFileEqualsWithOneStripe() throws IOException {
    testOneFile("/EqualsWithOneStripe", cellSize * dataBlocks);
  }

  @Test
  public void TestFileMoreThanOneStripe1() throws IOException {
    testOneFile("/MoreThanOneStripe1", cellSize * dataBlocks + 123);
  }

  @Test
  public void TestFileMoreThanOneStripe2() throws IOException {
    testOneFile("/MoreThanOneStripe2", cellSize * dataBlocks
            + cellSize * dataBlocks + 123);
  }

  @Test
  public void TestFileFullBlockGroup() throws IOException {
    testOneFile("/FullBlockGroup", blockSize * dataBlocks);
  }

  @Test
  public void TestFileMoreThanABlockGroup1() throws IOException {
    testOneFile("/MoreThanABlockGroup1", blockSize * dataBlocks + 123);
  }

  @Test
  public void TestFileMoreThanABlockGroup2() throws IOException {
    testOneFile("/MoreThanABlockGroup2", blockSize * dataBlocks + cellSize+ 123);
  }


  @Test
  public void TestFileMoreThanABlockGroup3() throws IOException {
    testOneFile("/MoreThanABlockGroup3",
        blockSize * dataBlocks * 3 + cellSize * dataBlocks
        + cellSize + 123);
  }

  private int stripeDataSize() {
    return cellSize * dataBlocks;
  }

  private byte[] generateBytes(int cnt) {
    byte[] bytes = new byte[cnt];
    for (int i = 0; i < cnt; i++) {
      bytes[i] = getByte(i);
    }
    return bytes;
  }

  private byte getByte(long pos) {
    int mod = 29;
    return (byte) (pos % mod + 1);
  }

  private void testOneFile(String src, int writeBytes)
      throws IOException {
    Path testPath = new Path(src);

    byte[] bytes = generateBytes(writeBytes);
    DFSTestUtil.writeFile(fs, testPath, new String(bytes));

    // check file length
    FileStatus status = fs.getFileStatus(testPath);
    long fileLength = status.getLen();
    Assert.assertEquals(writeBytes, fileLength);

    List<List<LocatedBlock>> blockGroupList = new ArrayList<>();
    LocatedBlocks lbs = fs.getClient().getLocatedBlocks(src, 0L);

    for (LocatedBlock firstBlock : lbs.getLocatedBlocks()) {
      Assert.assertTrue(firstBlock instanceof LocatedStripedBlock);
      LocatedBlock[] blocks = StripedBlockUtil.
          parseStripedBlockGroup((LocatedStripedBlock) firstBlock,
              cellSize, dataBlocks, parityBlocks);
      List<LocatedBlock> oneGroup = Arrays.asList(blocks);
      blockGroupList.add(oneGroup);
    }

    // test each block group
    for (int group = 0; group < blockGroupList.size(); group++) {
      //get the data of this block
      List<LocatedBlock> blockList = blockGroupList.get(group);
      byte[][] dataBlockBytes = new byte[dataBlocks][];
      byte[][] parityBlockBytes = new byte[parityBlocks][];

      // for each block, use BlockReader to read data
      for (int i = 0; i < blockList.size(); i++) {
        LocatedBlock lblock = blockList.get(i);
        if (lblock == null) {
          continue;
        }
        DatanodeInfo[] nodes = lblock.getLocations();
        ExtendedBlock block = lblock.getBlock();
        InetSocketAddress targetAddr = NetUtils.createSocketAddr(
            nodes[0].getXferAddr());

        byte[] blockBytes = new byte[(int)block.getNumBytes()];
        if (i < dataBlocks) {
          dataBlockBytes[i] = blockBytes;
        } else {
          parityBlockBytes[i - dataBlocks] = blockBytes;
        }

        if (block.getNumBytes() == 0) {
          continue;
        }

        BlockReader blockReader = new BlockReaderFactory(new DfsClientConf(conf)).
            setFileName(src).
            setBlock(block).
            setBlockToken(lblock.getBlockToken()).
            setInetSocketAddress(targetAddr).
            setStartOffset(0).
            setLength(block.getNumBytes()).
            setVerifyChecksum(true).
            setClientName("TestStripeLayoutWrite").
            setDatanodeInfo(nodes[0]).
            setCachingStrategy(CachingStrategy.newDefaultStrategy()).
            setClientCacheContext(ClientContext.getFromConf(conf)).
            setConfiguration(conf).
            setRemotePeerFactory(new RemotePeerFactory() {
              @Override
              public Peer newConnectedPeer(InetSocketAddress addr,
                                           Token<BlockTokenIdentifier> blockToken,
                                           DatanodeID datanodeId)
                  throws IOException {
                Peer peer = null;
                Socket sock = NetUtils.getDefaultSocketFactory(conf).createSocket();
                try {
                  sock.connect(addr, HdfsServerConstants.READ_TIMEOUT);
                  sock.setSoTimeout(HdfsServerConstants.READ_TIMEOUT);
                  peer = TcpPeerServer.peerFromSocket(sock);
                } finally {
                  if (peer == null) {
                    IOUtils.closeSocket(sock);
                  }
                }
                return peer;
              }
            }).build();

        blockReader.readAll(blockBytes, 0, (int) block.getNumBytes());
        blockReader.close();
      }

      // check if we write the data correctly
      for (int blkIdxInGroup = 0; blkIdxInGroup < dataBlockBytes.length;
           blkIdxInGroup++) {
        final byte[] actualBlkBytes = dataBlockBytes[blkIdxInGroup];
        if (actualBlkBytes == null) {
          continue;
        }
        for (int posInBlk = 0; posInBlk < actualBlkBytes.length; posInBlk++) {
          byte expected;
          // calculate the position of this byte in the file
          long posInFile = StripedBlockUtil.offsetInBlkToOffsetInBG(cellSize,
              dataBlocks, posInBlk, blkIdxInGroup) +
              group * blockSize * dataBlocks;
          if (posInFile >= writeBytes) {
            expected = 0;
          } else {
            expected = getByte(posInFile);
          }

          String s = "Unexpected byte " + actualBlkBytes[posInBlk]
              + ", expect " + expected
              + ". Block group index is " + group
              + ", stripe index is " + posInBlk / cellSize
              + ", cell index is " + blkIdxInGroup
              + ", byte index is " + posInBlk % cellSize;
          Assert.assertEquals(s, expected, actualBlkBytes[posInBlk]);
        }
      }

      // verify the parity blocks
      final ByteBuffer[] parityBuffers = new ByteBuffer[parityBlocks];
      final long groupSize = lbs.getLocatedBlocks().get(group).getBlockSize();
      int parityBlkSize = (int) StripedBlockUtil.getInternalBlockLength(groupSize,
          cellSize, dataBlocks, dataBlocks);
      for (int i = 0; i < parityBlocks; i++) {
        parityBuffers[i] = ByteBuffer.allocate(parityBlkSize);
      }
      final int numStripes = (int) (groupSize - 1) / stripeDataSize() + 1;
      for (int i = 0; i < numStripes; i++) {
        final int parityCellSize = i < numStripes - 1 || parityBlkSize % cellSize == 0
            ? cellSize : parityBlkSize % cellSize;
        ByteBuffer[] stripeBuf = new ByteBuffer[dataBlocks];
        for (int k = 0; k < stripeBuf.length; k++) {
          stripeBuf[k] = ByteBuffer.allocate(cellSize);
        }
        for (int j = 0; j < dataBlocks; j++) {
          if (dataBlockBytes[j] != null) {
            int length = Math.min(cellSize,
                dataBlockBytes[j].length - cellSize * i);
            if (length > 0) {
              stripeBuf[j].put(dataBlockBytes[j], cellSize * i, length);
            }
          }
          final long pos = stripeBuf[j].position();
          for (int k = 0; k < parityCellSize - pos; k++) {
            stripeBuf[j].put((byte) 0);
          }
          stripeBuf[j].flip();
        }
        ByteBuffer[] parityBuf = new ByteBuffer[parityBlocks];
        for (int j = 0; j < parityBlocks; j++) {
          parityBuf[j] = ByteBuffer.allocate(cellSize);
          for (int k = 0; k < parityCellSize; k++) {
            parityBuf[j].put((byte) 0);
          }
          parityBuf[j].flip();
        }

        encoder.encode(stripeBuf, parityBuf);
        for (int j = 0; j < parityBlocks; j++) {
          parityBuffers[j].put(parityBuf[j]);
        }
      }

      for (int i = 0; i < parityBlocks; i++) {
        Assert.assertArrayEquals(parityBuffers[i].array(), parityBlockBytes[i]);
      }
    }
  }

  private void testReadWriteOneFile(String src, int writeBytes)
      throws IOException {
    Path TestPath = new Path(src);
    byte[] bytes = generateBytes(writeBytes);
    DFSTestUtil.writeFile(fs, TestPath, new String(bytes));

    //check file length
    FileStatus status = fs.getFileStatus(TestPath);
    long fileLength = status.getLen();
    if (fileLength != writeBytes) {
      Assert.fail("File Length error: expect=" + writeBytes
          + ", actual=" + fileLength);
    }

    DFSStripedInputStream dis = new DFSStripedInputStream(
        fs.getClient(), src, true);
    byte[] buf = new byte[writeBytes + 100];
    int readLen = dis.read(0, buf, 0, buf.length);
    readLen = readLen >= 0 ? readLen : 0;
    if (readLen != writeBytes) {
      Assert.fail("The length of file is not correct.");
    }

    for (int i = 0; i < writeBytes; i++) {
      if (getByte(i) != buf[i]) {
        Assert.fail("Byte at i = " + i + " is wrongly written.");
      }
    }
  }
}
