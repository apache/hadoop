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
package org.apache.hadoop.hdfs.tools;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.HdfsBlockLocation;
import org.apache.hadoop.hdfs.BlockReader;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.client.impl.BlockReaderRemote;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.server.datanode.ReplicaNotFoundException;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.io.erasurecode.CodecUtil;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureEncoder;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.Uninterruptibles;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.server.datanode.BlockMetadataHeader;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetUtil;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.Timer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * This class implements debug operations on the HDFS command-line.
 *
 * These operations are only for debugging, and may change or disappear
 * between HDFS versions.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class DebugAdmin extends Configured implements Tool {
  /**
   * All the debug commands we can run.
   */
  private final DebugCommand[] DEBUG_COMMANDS = {
      new VerifyMetaCommand(),
      new ComputeMetaCommand(),
      new RecoverLeaseCommand(),
      new VerifyECCommand(),
      new VerifyReadableCommand(),
      new HelpCommand()
  };

  /**
   * The base class for debug commands.
   */
  private abstract static class DebugCommand {
    final String name;
    final String usageText;
    final String helpText;

    DebugCommand(String name, String usageText, String helpText) {
      this.name = name;
      this.usageText = usageText;
      this.helpText = helpText;
    }

    abstract int run(List<String> args) throws IOException;
  }

  private static int HEADER_LEN = 7;

  /**
   * The command for verifying a block metadata file and possibly block file.
   */
  private static class VerifyMetaCommand extends DebugCommand {
    VerifyMetaCommand() {
      super("verifyMeta",
          "verifyMeta -meta <metadata-file> [-block <block-file>]",
          "  Verify HDFS metadata and block files.  If a block file is specified, we" +
              System.lineSeparator() +
              "  will verify that the checksums in the metadata file match the block" +
              System.lineSeparator() +
              "  file.");
    }

    int run(List<String> args) throws IOException {
      if (args.size() == 0) {
        System.out.println(usageText);
        System.out.println(helpText + System.lineSeparator());
        return 1;
      }
      String blockFile = StringUtils.popOptionWithArgument("-block", args);
      String metaFile = StringUtils.popOptionWithArgument("-meta", args);
      if (metaFile == null) {
        System.err.println("You must specify a meta file with -meta");
        return 1;
      }

      FileInputStream metaStream = null, dataStream = null;
      FileChannel metaChannel = null, dataChannel = null;
      DataInputStream checksumStream = null;
      try {
        BlockMetadataHeader header;
        try {
          metaStream = new FileInputStream(metaFile);
          checksumStream = new DataInputStream(metaStream);
          header = BlockMetadataHeader.readHeader(checksumStream);
          metaChannel = metaStream.getChannel();
          metaChannel.position(HEADER_LEN);
        } catch (RuntimeException e) {
          System.err.println("Failed to read HDFS metadata file header for " +
              metaFile + ": " + StringUtils.stringifyException(e));
          return 1;
        } catch (IOException e) {
          System.err.println("Failed to read HDFS metadata file header for " +
              metaFile + ": " + StringUtils.stringifyException(e));
          return 1;
        }
        DataChecksum checksum = header.getChecksum();
        System.out.println("Checksum type: " + checksum.toString());
        if (blockFile == null) {
          return 0;
        }
        ByteBuffer metaBuf, dataBuf;
        try {
          dataStream = new FileInputStream(blockFile);
          dataChannel = dataStream.getChannel();
          final int CHECKSUMS_PER_BUF = 1024 * 32;
          metaBuf = ByteBuffer.allocate(checksum.
              getChecksumSize() * CHECKSUMS_PER_BUF);
          dataBuf = ByteBuffer.allocate(checksum.
              getBytesPerChecksum() * CHECKSUMS_PER_BUF);
        } catch (IOException e) {
          System.err.println("Failed to open HDFS block file for " +
              blockFile + ": " + StringUtils.stringifyException(e));
          return 1;
        }
        long offset = 0;
        while (true) {
          dataBuf.clear();
          int dataRead = -1;
          try {
            dataRead = dataChannel.read(dataBuf);
            if (dataRead < 0) {
              break;
            }
          } catch (IOException e) {
            System.err.println("Got I/O error reading block file " +
                blockFile + "from disk at offset " + dataChannel.position() +
                ": " + StringUtils.stringifyException(e));
            return 1;
          }
          try {
            int csumToRead =
                (((checksum.getBytesPerChecksum() - 1) + dataRead) /
                  checksum.getBytesPerChecksum()) *
                      checksum.getChecksumSize();
            metaBuf.clear();
            metaBuf.limit(csumToRead);
            metaChannel.read(metaBuf);
            dataBuf.flip();
            metaBuf.flip();
          } catch (IOException e) {
            System.err.println("Got I/O error reading metadata file " +
                metaFile + "from disk at offset " + metaChannel.position() +
                ": " +  StringUtils.stringifyException(e));
            return 1;
          }
          try {
            checksum.verifyChunkedSums(dataBuf, metaBuf,
                blockFile, offset);
          } catch (IOException e) {
            System.out.println("verifyChunkedSums error: " +
                StringUtils.stringifyException(e));
            return 1;
          }
          offset += dataRead;
        }
        System.out.println("Checksum verification succeeded on block file " +
            blockFile);
        return 0;
      } finally {
        IOUtils.cleanupWithLogger(null, metaStream, dataStream, checksumStream);
      }
    }
  }

  /**
   * The command for verifying a block metadata file and possibly block file.
   */
  private static class ComputeMetaCommand extends DebugCommand {
    ComputeMetaCommand() {
      super("computeMeta",
          "computeMeta -block <block-file> -out <output-metadata-file>",
          "  Compute HDFS metadata from the specified block file, and save it"
              + " to" + System.lineSeparator()
              + "  the specified output metadata file."
              + System.lineSeparator() + System.lineSeparator()
              + "**NOTE: Use at your own risk!" + System.lineSeparator()
              + " If the block file is corrupt"
              + " and you overwrite it's meta file, " + System.lineSeparator()
              + " it will show up"
              + " as good in HDFS, but you can't read the data."
              + System.lineSeparator()
              + " Only use as a last measure, and when you are 100% certain"
              + " the block file is good.");
    }

    private DataChecksum createChecksum(Options.ChecksumOpt opt) {
      DataChecksum dataChecksum = DataChecksum
          .newDataChecksum(opt.getChecksumType(), opt.getBytesPerChecksum());
      if (dataChecksum == null) {
        throw new HadoopIllegalArgumentException(
            "Invalid checksum type: userOpt=" + opt + ", default=" + opt
                + ", effective=null");
      }
      return dataChecksum;
    }

    int run(List<String> args) throws IOException {
      if (args.size() == 0) {
        System.out.println(usageText);
        System.out.println(helpText + System.lineSeparator());
        return 1;
      }
      final String name = StringUtils.popOptionWithArgument("-block", args);
      if (name == null) {
        System.err.println("You must specify a block file with -block");
        return 2;
      }
      final File blockFile = new File(name);
      if (!blockFile.exists() || !blockFile.isFile()) {
        System.err.println("Block file <" + name + "> does not exist "
            + "or is not a file");
        return 3;
      }
      final String outFile = StringUtils.popOptionWithArgument("-out", args);
      if (outFile == null) {
        System.err.println("You must specify a output file with -out");
        return 4;
      }
      final File srcMeta = new File(outFile);
      if (srcMeta.exists()) {
        System.err.println("output file already exists!");
        return 5;
      }

      DataOutputStream metaOut = null;
      try {
        final Configuration conf = new Configuration();
        final Options.ChecksumOpt checksumOpt =
            DfsClientConf.getChecksumOptFromConf(conf);
        final DataChecksum checksum = createChecksum(checksumOpt);

        final int smallBufferSize = DFSUtilClient.getSmallBufferSize(conf);
        metaOut = new DataOutputStream(
            new BufferedOutputStream(Files.newOutputStream(srcMeta.toPath()),
                smallBufferSize));
        BlockMetadataHeader.writeHeader(metaOut, checksum);
        metaOut.close();
        metaOut = null;
        FsDatasetUtil.computeChecksum(
            srcMeta, srcMeta, blockFile, smallBufferSize, conf);
        System.out.println(
            "Checksum calculation succeeded on block file " + name
                + " saved metadata to meta file " + outFile);
        return 0;
      } finally {
        IOUtils.cleanupWithLogger(null, metaOut);
      }
    }
  }

  /**
   * The command for recovering a file lease.
   */
  private class RecoverLeaseCommand extends DebugCommand {
    RecoverLeaseCommand() {
      super("recoverLease",
"recoverLease -path <path> [-retries <num-retries>]",
"  Recover the lease on the specified path.  The path must reside on an" +
    System.lineSeparator() +
"  HDFS filesystem.  The default number of retries is 1.");
    }

    private static final int TIMEOUT_MS = 5000;

    int run(List<String> args) throws IOException {
      if (args.size() == 0) {
        System.out.println(usageText);
        System.out.println(helpText + System.lineSeparator());
        return 1;
      }
      String pathStr = StringUtils.popOptionWithArgument("-path", args);
      String retriesStr = StringUtils.popOptionWithArgument("-retries", args);
      if (pathStr == null) {
        System.err.println("You must supply a -path argument to " +
            "recoverLease.");
        return 1;
      }
      int maxRetries = 1;
      if (retriesStr != null) {
        try {
          maxRetries = Integer.parseInt(retriesStr);
        } catch (NumberFormatException e) {
          System.err.println("Failed to parse the argument to -retries: " +
              StringUtils.stringifyException(e));
          return 1;
        }
      }
      FileSystem fs;
      try {
        fs = FileSystem.newInstance(new URI(pathStr), getConf(), null);
      } catch (URISyntaxException e) {
        System.err.println("URISyntaxException for " + pathStr + ":" +
            StringUtils.stringifyException(e));
        return 1;
      } catch (InterruptedException e) {
        System.err.println("InterruptedException for " + pathStr + ":" +
            StringUtils.stringifyException(e));
        return 1;
      }
      DistributedFileSystem dfs = null;
      try {
        dfs = (DistributedFileSystem) fs;
      } catch (ClassCastException e) {
        System.err.println("Invalid filesystem for path " + pathStr + ": " +
            "needed scheme hdfs, but got: " + fs.getScheme());
        return 1;
      }
      for (int retry = 0; true; ) {
        boolean recovered = false;
        IOException ioe = null;
        try {
          recovered = dfs.recoverLease(new Path(pathStr));
        } catch (FileNotFoundException e) {
          System.err.println("recoverLease got exception: " + e.getMessage());
          System.err.println("Giving up on recoverLease for " + pathStr +
              " after 1 try");
          return 1;
        } catch (IOException e) {
          ioe = e;
        }
        if (recovered) {
          System.out.println("recoverLease SUCCEEDED on " + pathStr); 
          return 0;
        }
        if (ioe != null) {
          System.err.println("recoverLease got exception: " +
              ioe.getMessage());
        } else {
          System.err.println("recoverLease returned false.");
        }
        retry++;
        if (retry >= maxRetries) {
          break;
        }
        System.err.println("Retrying in " + TIMEOUT_MS + " ms...");
        Uninterruptibles.sleepUninterruptibly(TIMEOUT_MS,
            TimeUnit.MILLISECONDS);
        System.err.println("Retry #" + retry);
      }
      System.err.println("Giving up on recoverLease for " + pathStr + " after " +
          maxRetries + (maxRetries == 1 ? " try." : " tries."));
      return 1;
    }
  }

  /**
   * The command for verifying the correctness of erasure coding on an erasure coded file.
   */
  private class VerifyECCommand extends DebugCommand {
    private DFSClient client;
    private int dataBlkNum;
    private int parityBlkNum;
    private int cellSize;
    private boolean useDNHostname;
    private CachingStrategy cachingStrategy;
    private int stripedReadBufferSize;
    private CompletionService<Integer> readService;
    private RawErasureEncoder encoder;
    private BlockReader[] blockReaders;


    VerifyECCommand() {
      super("verifyEC",
          "verifyEC -file <file> [-blockId <blk_Id>] [-skipFailureBlocks]",
          "  -file Verify HDFS erasure coding on all block groups of the file." +
              System.lineSeparator() +
          "  -skipFailureBlocks specify will skip any block group failures during verify," +
          "  and continues verify all block groups of the file," + System.lineSeparator() +
          "  the default is not to skip failure blocks." + System.lineSeparator() +
          "  -blockId specify blk_Id to verify for a specific one block group.");
    }

    int run(List<String> args) throws IOException {
      if (args.size() < 2) {
        System.out.println(usageText);
        System.out.println(helpText + System.lineSeparator());
        return 1;
      }
      String file = StringUtils.popOptionWithArgument("-file", args);
      Path path = new Path(file);
      DistributedFileSystem dfs = AdminHelper.getDFS(getConf());
      this.client = dfs.getClient();

      FileStatus fileStatus;
      try {
        fileStatus = dfs.getFileStatus(path);
      } catch (FileNotFoundException e) {
        System.err.println("File " + file + " does not exist.");
        return 1;
      }

      if (!fileStatus.isFile()) {
        System.err.println("File " + file + " is not a regular file.");
        return 1;
      }
      if (!dfs.isFileClosed(path)) {
        System.err.println("File " + file + " is not closed.");
        return 1;
      }
      this.useDNHostname = getConf().getBoolean(DFSConfigKeys.DFS_DATANODE_USE_DN_HOSTNAME,
          DFSConfigKeys.DFS_DATANODE_USE_DN_HOSTNAME_DEFAULT);
      this.cachingStrategy = CachingStrategy.newDefaultStrategy();
      this.stripedReadBufferSize = getConf().getInt(
          DFSConfigKeys.DFS_DN_EC_RECONSTRUCTION_STRIPED_READ_BUFFER_SIZE_KEY,
          DFSConfigKeys.DFS_DN_EC_RECONSTRUCTION_STRIPED_READ_BUFFER_SIZE_DEFAULT);

      LocatedBlocks locatedBlocks = client.getLocatedBlocks(file, 0, fileStatus.getLen());
      if (locatedBlocks.getErasureCodingPolicy() == null) {
        System.err.println("File " + file + " is not erasure coded.");
        return 1;
      }
      ErasureCodingPolicy ecPolicy = locatedBlocks.getErasureCodingPolicy();
      this.dataBlkNum = ecPolicy.getNumDataUnits();
      this.parityBlkNum = ecPolicy.getNumParityUnits();
      this.cellSize = ecPolicy.getCellSize();
      this.encoder = CodecUtil.createRawEncoder(getConf(), ecPolicy.getCodecName(),
          new ErasureCoderOptions(dataBlkNum, parityBlkNum));
      int blockNum = dataBlkNum + parityBlkNum;
      this.readService = new ExecutorCompletionService<>(
          DFSUtilClient.getThreadPoolExecutor(blockNum, blockNum, 60,
              new LinkedBlockingQueue<>(), "read-", false));
      this.blockReaders = new BlockReader[blockNum];

      String needToVerifyBlockId = StringUtils.popOptionWithArgument("-blockId", args);
      boolean skipFailureBlocks = StringUtils.popOption("-skipFailureBlocks", args);
      boolean isHealthy = true;

      for (LocatedBlock locatedBlock : locatedBlocks.getLocatedBlocks()) {
        String blockName = locatedBlock.getBlock().getBlockName();
        if (needToVerifyBlockId == null || needToVerifyBlockId.equals(blockName)) {
          System.out.println("Checking EC block group: " + blockName);
          LocatedStripedBlock blockGroup = (LocatedStripedBlock) locatedBlock;

          try {
            verifyBlockGroup(blockGroup);
            System.out.println("Status: OK");
          } catch (Exception e) {
            System.err.println("Status: ERROR, message: " + e.getMessage());
            isHealthy = false;
            if (!skipFailureBlocks) {
              break;
            }
          } finally {
            closeBlockReaders();
          }

          if (needToVerifyBlockId != null) {
            break;
          }
        }
      }
      if (isHealthy) {
        if (needToVerifyBlockId == null) {
          System.out.println("\nAll EC block group status: OK");
        }
        return 0;
      }
      return 1;
    }

    private void verifyBlockGroup(LocatedStripedBlock blockGroup) throws Exception {
      final LocatedBlock[] indexedBlocks = StripedBlockUtil.parseStripedBlockGroup(blockGroup,
          cellSize, dataBlkNum, parityBlkNum);

      int blockNumExpected = Math.min(dataBlkNum,
          (int) ((blockGroup.getBlockSize() - 1) / cellSize + 1)) + parityBlkNum;
      if (blockGroup.getBlockIndices().length < blockNumExpected) {
        throw new Exception("Block group is under-erasure-coded.");
      }

      long maxBlockLen = 0L;
      DataChecksum checksum = null;
      for (int i = 0; i < dataBlkNum + parityBlkNum; i++) {
        LocatedBlock block = indexedBlocks[i];
        if (block == null) {
          blockReaders[i] = null;
          continue;
        }
        if (block.getBlockSize() > maxBlockLen) {
          maxBlockLen = block.getBlockSize();
        }
        BlockReader blockReader = createBlockReader(block.getBlock(),
            block.getLocations()[0], block.getBlockToken());
        if (checksum == null) {
          checksum = blockReader.getDataChecksum();
        } else {
          assert checksum.equals(blockReader.getDataChecksum());
        }
        blockReaders[i] = blockReader;
      }
      assert checksum != null;
      int bytesPerChecksum = checksum.getBytesPerChecksum();
      int bufferSize = stripedReadBufferSize < bytesPerChecksum ? bytesPerChecksum :
          stripedReadBufferSize - stripedReadBufferSize % bytesPerChecksum;
      final ByteBuffer[] buffers = new ByteBuffer[dataBlkNum + parityBlkNum];
      final ByteBuffer[] outputs = new ByteBuffer[parityBlkNum];
      for (int i = 0; i < dataBlkNum + parityBlkNum; i++) {
        buffers[i] = ByteBuffer.allocate(bufferSize);
      }
      for (int i = 0; i < parityBlkNum; i++) {
        outputs[i] = ByteBuffer.allocate(bufferSize);
      }
      long positionInBlock = 0L;
      while (positionInBlock < maxBlockLen) {
        final int toVerifyLen = (int) Math.min(bufferSize, maxBlockLen - positionInBlock);
        List<Future<Integer>> futures = new ArrayList<>(dataBlkNum + parityBlkNum);
        for (int i = 0; i < dataBlkNum + parityBlkNum; i++) {
          final int fi = i;
          futures.add(this.readService.submit(() -> {
            BlockReader blockReader = blockReaders[fi];
            ByteBuffer buffer = buffers[fi];
            buffer.clear();
            buffer.limit(toVerifyLen);
            int readLen = 0;
            if (blockReader != null) {
              int toRead = buffer.remaining();
              while (readLen < toRead) {
                int nread = blockReader.read(buffer);
                if (nread <= 0) {
                  break;
                }
                readLen += nread;
              }
            }
            while (buffer.hasRemaining()) {
              buffer.put((byte) 0);
            }
            buffer.flip();
            return readLen;
          }));
        }
        for (int i = 0; i < dataBlkNum + parityBlkNum; i++) {
          futures.get(i).get(1, TimeUnit.MINUTES);
        }
        ByteBuffer[] inputs = new ByteBuffer[dataBlkNum];
        System.arraycopy(buffers, 0, inputs, 0, dataBlkNum);
        for (int i = 0; i < parityBlkNum; i++) {
          outputs[i].clear();
          outputs[i].limit(toVerifyLen);
        }
        this.encoder.encode(inputs, outputs);
        for (int i = 0; i < parityBlkNum; i++) {
          if (!buffers[dataBlkNum + i].equals(outputs[i])) {
            throw new Exception("EC compute result not match.");
          }
        }
        positionInBlock += toVerifyLen;
      }
    }

    private BlockReader createBlockReader(ExtendedBlock block, DatanodeInfo dnInfo,
                                          Token<BlockTokenIdentifier> token) throws IOException {
      InetSocketAddress dnAddress = NetUtils.createSocketAddr(dnInfo.getXferAddr(useDNHostname));
      Peer peer = client.newConnectedPeer(dnAddress, token, dnInfo);
      return BlockReaderRemote.newBlockReader(
          "dummy", block, token, 0,
          block.getNumBytes(), true, "", peer, dnInfo,
          null, cachingStrategy, -1, getConf());
    }

    private void closeBlockReaders() {
      for (int i = 0; i < blockReaders.length; i++) {
        if (blockReaders[i] != null) {
          IOUtils.closeStream(blockReaders[i]);
          blockReaders[i] = null;
        }
      }
    }

  }

  private class VerifyReadableCommand extends DebugCommand {
    DistributedFileSystem dfs;
    boolean suppressed = false;

    VerifyReadableCommand() {
      super("verifyReadable",
          "verifyReadable "
              + "[-path <path> | -input <input>] "
              + "[-output <output>] "
              + "[-concurrency <concurrency>] "
              + "[-suppressed]",
          "  Verify if one or multiple paths are fully readable and have no missing blocks.");
    }

    @Override
    int run(List<String> args) throws IOException {
      if (args.isEmpty()) {
        System.out.println(usageText);
        System.out.println(helpText + System.lineSeparator());
        return 1;
      }
      dfs = AdminHelper.getDFS(getConf());
      String pathStr = StringUtils.popOptionWithArgument("-path", args);
      String inputStr = StringUtils.popOptionWithArgument("-input", args);
      String outputStr = StringUtils.popOptionWithArgument("-output", args);
      String concurrencyStr = StringUtils.popOptionWithArgument("-concurrency", args);
      suppressed = StringUtils.popOption("-suppressed", args);
      if (pathStr == null && inputStr == null) {
        System.out.println("Either -path or -input must be present.");
        System.out.println(usageText);
        System.out.println(helpText + System.lineSeparator());
        return 1;
      }
      try {
        return handleArgs(pathStr, inputStr, outputStr, concurrencyStr);
      } catch (Exception e) {
        System.err.println(
            "Got IOE: " + StringUtils.stringifyException(e) + " for command: " + StringUtils.join(
                ",", args));
        return 1;
      }
    }

    private int handleArgs(String pathStr, String inputStr, String outputStr, String concurrencyStr)
        throws IOException, ExecutionException, InterruptedException {
      BufferedWriter writer = null;
      try {
        if (outputStr != null) {
          File output = new File(outputStr);
          writer = new BufferedWriter(new OutputStreamWriter(Files.newOutputStream(output.toPath()),
              StandardCharsets.UTF_8));
        }

        // -path takes priority over -input
        if (pathStr != null) {
          int result = handlePath(new Path(pathStr));
          writeToOutput(writer, pathStr, result);
          return result;
        }

        // -input must be defined by this point
        File input = new File(inputStr);
        if (!input.exists()) {
          return 1;
        }
        BufferedReader reader = new BufferedReader(
            new InputStreamReader(Files.newInputStream(input.toPath()), StandardCharsets.UTF_8));
        Set<Path> paths = new HashSet<>();
        String line;
        while ((line = reader.readLine()) != null) {
          paths.add(new Path(line.trim()));
        }
        reader.close();
        int concurrency = concurrencyStr == null ? 1 : Integer.parseInt(concurrencyStr);
        return handlePaths(paths, writer, concurrency);
      } finally {
        if (writer != null) {
          writer.flush();
          writer.close();
        }
      }
    }

    private void writeToOutput(BufferedWriter writer, String path, int result) throws IOException {
      if (writer == null) {
        return;
      }
      writer.write(path);
      writer.write(" ");
      writer.write(String.valueOf(result));
      writer.write("\n");
      writer.flush();
    }

    private int handlePaths(Set<Path> paths, BufferedWriter writer, int concurrency)
        throws ExecutionException, InterruptedException, IOException {
      int total = paths.size();
      long start = Time.monotonicNow();
      ExecutorService threadPool = Executors.newFixedThreadPool(concurrency);
      List<Callable<Pair<Path, Integer>>> tasks = new ArrayList<>();
      for (Path path : paths) {
        tasks.add(() -> Pair.of(path, handlePath(path)));
      }
      List<Future<Pair<Path, Integer>>> futures =
          tasks.stream().map(threadPool::submit).collect(Collectors.toList());

      boolean failed = false;
      int done = 0;
      for (Future<Pair<Path, Integer>> future : futures) {
        done++;
        if (done % 1000 == 0) {
          long elapsed = Time.monotonicNow() - start;
          double rate = (double) done / elapsed * 1000;
          String msg = "Progress: %d/%d, elapsed: %d ms, rate: %5.2f files/s%n";
          System.out.printf(msg, done, total, elapsed, rate);
        }
        writeToOutput(writer, future.get().getLeft().toString(), future.get().getRight());
        failed |= future.get().getRight() != 0;
      }
      return failed ? 1 : 0;
    }

    private int handlePath(Path path) {

      HdfsBlockLocation[] locs;
      try {
        locs = (HdfsBlockLocation[]) dfs.getFileBlockLocations(path, 0,
            dfs.getFileStatus(path).getLen());
      } catch (FileNotFoundException e) {
        System.err.println("Path not found: " + path);
        return 1;
      } catch (AccessControlException e) {
        System.err.println("No permission for path: " + path);
        return 1;
      } catch (IOException e) {
        System.err.println("Got IOE: " + StringUtils.stringifyException(e) + " for path: " + path);
        return 1;
      }

      // First pass: check for block with no live replicas
      for (HdfsBlockLocation loc : locs) {
        if (loc.getLocatedBlock().getLocations().length == 0) {
          System.err.println("Path: " + path + ". No live replicas found: " + loc);
          return 1;
        }
      }

      for (HdfsBlockLocation loc : locs) {
        if (!verifyBlock(loc.getLocatedBlock())) {
          System.err.println("Path: " + path + ". Block not readable: " + loc);
          return 1;
        }
      }
      if (!suppressed) {
        System.out.println("No issue found with path " + path);
      }
      return 0;
    }

    private boolean verifyBlock(LocatedBlock loc) {
      for (DatanodeInfo dn : loc.getLocations()) {
        if (verifyReplica(loc, dn)) {
          return true;
        }
      }
      return false;
    }

    private boolean verifyReplica(LocatedBlock loc, DatanodeInfo dn) {
      ClientDatanodeProtocol cdp = null;

      try {
        try {
          DfsClientConf clientConf = dfs.getClient().getConf();
          cdp = DFSUtilClient.createClientDatanodeProtocolProxy(dn, getConf(),
              clientConf.getSocketTimeout(), clientConf.isConnectToDnViaHostname(), loc);
          return cdp.getReplicaVisibleLength(loc.getBlock()) > 0;
        } catch (RemoteException e) {
          throw e.unwrapRemoteException();
        }
      } catch (ReplicaNotFoundException e) {
        System.err.println("Block " + loc.getBlock() + " replica does not exist on DN " + dn);
        return false;
      } catch (ConnectException e) {
        System.err.println("Block " + loc.getBlock() + " DN failed connection " + dn);
        return false;
      } catch (IOException e) {
        System.err.println(
            "Got IOE: " + StringUtils.stringifyException(e) + " for block: " + loc + " and dn: "
                + dn);
        // No need to throw exception since a failed call is a failed call
        // But log it for handling
        return false;
      } finally {
        if (cdp != null) {
          RPC.stopProxy(cdp);
        }
      }
    }
  }

  /**
   * The command for getting help about other commands.
   */
  private class HelpCommand extends DebugCommand {
    HelpCommand() {
      super("help",
"help [command-name]",
"  Get help about a command.");
    }

    int run(List<String> args) {
      DebugCommand command = popCommand(args);
      if (command == null) {
        printUsage();
        return 0;
      }
      System.out.println(command.usageText);
      System.out.println(command.helpText + System.lineSeparator());
      return 0;
    }
  }

  public DebugAdmin(Configuration conf) {
    super(conf);
  }

  private DebugCommand popCommand(List<String> args) {
    String commandStr = (args.size() == 0) ? "" : args.get(0);
    if (commandStr.startsWith("-")) {
      commandStr = commandStr.substring(1);
    }
    for (DebugCommand command : DEBUG_COMMANDS) {
      if (command.name.equals(commandStr)) {
        args.remove(0);
        return command;
      }
    }
    return null;
  }

  public int run(String[] argv) {
    LinkedList<String> args = new LinkedList<String>();
    for (int j = 0; j < argv.length; ++j) {
      args.add(argv[j]);
    }
    DebugCommand command = popCommand(args);
    if (command == null) {
      printUsage();
      return 0;
    }
    try {
      return command.run(args);
    } catch (IOException e) {
      System.err.println("IOException: " +
          StringUtils.stringifyException(e));
      return 1;
    } catch (RuntimeException e) {
      System.err.println("RuntimeException: " +
          StringUtils.stringifyException(e));
      return 1;
    }
  }

  private void printUsage() {
    System.out.println("Usage: hdfs debug <command> [arguments]\n");
    System.out.println("These commands are for advanced users only.\n");
    System.out.println("Incorrect usages may result in data loss. " +
        "Use at your own risk.\n");
    for (DebugCommand command : DEBUG_COMMANDS) {
      if (!command.name.equals("help")) {
        System.out.println(command.usageText);
      }
    }
    System.out.println();
    ToolRunner.printGenericCommandUsage(System.out);
  }

  public static void main(String[] argsArray) throws Exception {
    DebugAdmin debugAdmin = new DebugAdmin(new Configuration());
    int res = ToolRunner.run(debugAdmin, argsArray);
    System.exit(res);
  }
}
