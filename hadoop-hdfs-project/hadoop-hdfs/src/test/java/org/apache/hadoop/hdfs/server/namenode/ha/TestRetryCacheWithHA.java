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
package org.apache.hadoop.hdfs.server.namenode.ha;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream.SyncFlag;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.PathBasedCacheDirective;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstruction;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.INodeFileUnderConstruction;
import org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotTestHelper;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.retry.FailoverProxyProvider;
import org.apache.hadoop.io.retry.RetryInvocationHandler;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ipc.RetryCache.CacheEntry;
import org.apache.hadoop.util.LightWeightCache;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestRetryCacheWithHA {
  private static final Log LOG = LogFactory.getLog(TestRetryCacheWithHA.class);
  
  private static final int BlockSize = 1024;
  private static final short DataNodes = 3;
  private static final int CHECKTIMES = 10;
  
  private MiniDFSCluster cluster;
  private DistributedFileSystem dfs;
  private Configuration conf = new HdfsConfiguration();
  
  /** 
   * A dummy invocation handler extending RetryInvocationHandler. We can use
   * a boolean flag to control whether the method invocation succeeds or not. 
   */
  private static class DummyRetryInvocationHandler extends
      RetryInvocationHandler<ClientProtocol> {
    static AtomicBoolean block = new AtomicBoolean(false);

    DummyRetryInvocationHandler(
        FailoverProxyProvider<ClientProtocol> proxyProvider,
        RetryPolicy retryPolicy) {
      super(proxyProvider, retryPolicy);
    }

    @Override
    protected Object invokeMethod(Method method, Object[] args)
        throws Throwable {
      Object result = super.invokeMethod(method, args);
      if (block.get()) {
        throw new UnknownHostException("Fake Exception");
      } else {
        return result;
      }
    }
  }
  
  @Before
  public void setup() throws Exception {
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BlockSize);
    cluster = new MiniDFSCluster.Builder(conf)
        .nnTopology(MiniDFSNNTopology.simpleHATopology())
        .numDataNodes(DataNodes).build();
    cluster.waitActive();
    cluster.transitionToActive(0);
    // setup the configuration
    HATestUtil.setFailoverConfigurations(cluster, conf);
    dfs = (DistributedFileSystem) HATestUtil.configureFailoverFs(cluster, conf);
  }
  
  @After
  public void cleanup() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }
  
  /**
   * 1. Run a set of operations
   * 2. Trigger the NN failover
   * 3. Check the retry cache on the original standby NN
   */
  @Test (timeout=60000)
  public void testRetryCacheOnStandbyNN() throws Exception {
    // 1. run operations
    DFSTestUtil.runOperations(cluster, dfs, conf, BlockSize, 0);
    
    // check retry cache in NN1
    FSNamesystem fsn0 = cluster.getNamesystem(0);
    LightWeightCache<CacheEntry, CacheEntry> cacheSet = 
        (LightWeightCache<CacheEntry, CacheEntry>) fsn0.getRetryCache().getCacheSet();
    assertEquals(20, cacheSet.size());
    
    Map<CacheEntry, CacheEntry> oldEntries = 
        new HashMap<CacheEntry, CacheEntry>();
    Iterator<CacheEntry> iter = cacheSet.iterator();
    while (iter.hasNext()) {
      CacheEntry entry = iter.next();
      oldEntries.put(entry, entry);
    }
    
    // 2. Failover the current standby to active.
    cluster.getNameNode(0).getRpcServer().rollEditLog();
    cluster.getNameNode(1).getNamesystem().getEditLogTailer().doTailEdits();
    
    cluster.shutdownNameNode(0);
    cluster.transitionToActive(1);
    
    // 3. check the retry cache on the new active NN
    FSNamesystem fsn1 = cluster.getNamesystem(1);
    cacheSet = (LightWeightCache<CacheEntry, CacheEntry>) fsn1
        .getRetryCache().getCacheSet();
    assertEquals(20, cacheSet.size());
    iter = cacheSet.iterator();
    while (iter.hasNext()) {
      CacheEntry entry = iter.next();
      assertTrue(oldEntries.containsKey(entry));
    }
  }
  
  private DFSClient genClientWithDummyHandler() throws IOException {
    URI nnUri = dfs.getUri();
    Class<FailoverProxyProvider<ClientProtocol>> failoverProxyProviderClass = 
        NameNodeProxies.getFailoverProxyProviderClass(conf, nnUri, 
            ClientProtocol.class);
    FailoverProxyProvider<ClientProtocol> failoverProxyProvider = 
        NameNodeProxies.createFailoverProxyProvider(conf, 
            failoverProxyProviderClass, ClientProtocol.class, nnUri);
    InvocationHandler dummyHandler = new DummyRetryInvocationHandler(
        failoverProxyProvider, RetryPolicies
        .failoverOnNetworkException(RetryPolicies.TRY_ONCE_THEN_FAIL,
            Integer.MAX_VALUE,
            DFSConfigKeys.DFS_CLIENT_FAILOVER_SLEEPTIME_BASE_DEFAULT,
            DFSConfigKeys.DFS_CLIENT_FAILOVER_SLEEPTIME_MAX_DEFAULT));
    ClientProtocol proxy = (ClientProtocol) Proxy.newProxyInstance(
        failoverProxyProvider.getInterface().getClassLoader(),
        new Class[] { ClientProtocol.class }, dummyHandler);
    
    DFSClient client = new DFSClient(null, proxy, conf, null);
    return client;
  }
  
  abstract class AtMostOnceOp {
    private final String name;
    final DFSClient client;
    
    AtMostOnceOp(String name, DFSClient client) {
      this.name = name;
      this.client = client;
    }
    
    abstract void prepare() throws Exception;
    abstract void invoke() throws Exception;
    abstract boolean checkNamenodeBeforeReturn() throws Exception;
    abstract Object getResult();
  }
  
  /** createSnapshot operaiton */
  class CreateSnapshotOp extends AtMostOnceOp {
    private String snapshotPath;
    private String dir;
    private String snapshotName;
    
    CreateSnapshotOp(DFSClient client, String dir, String snapshotName) {
      super("createSnapshot", client);
      this.dir = dir;
      this.snapshotName = snapshotName;
    }

    @Override
    void prepare() throws Exception {
      final Path dirPath = new Path(dir);
      if (!dfs.exists(dirPath)) {
        dfs.mkdirs(dirPath);
        dfs.allowSnapshot(dirPath);
      }
    }

    @Override
    void invoke() throws Exception {
      this.snapshotPath = client.createSnapshot(dir, snapshotName);
    }

    @Override
    boolean checkNamenodeBeforeReturn() throws Exception {
      final Path sPath = SnapshotTestHelper.getSnapshotRoot(new Path(dir),
          snapshotName);
      boolean snapshotCreated = dfs.exists(sPath);
      for (int i = 0; i < CHECKTIMES && !snapshotCreated; i++) {
        Thread.sleep(1000);
        snapshotCreated = dfs.exists(sPath);
      }
      return snapshotCreated;
    }

    @Override
    Object getResult() {
      return snapshotPath;
    }
  }
  
  /** deleteSnapshot */
  class DeleteSnapshotOp extends AtMostOnceOp {
    private String dir;
    private String snapshotName;
    
    DeleteSnapshotOp(DFSClient client, String dir, String snapshotName) {
      super("deleteSnapshot", client);
      this.dir = dir;
      this.snapshotName = snapshotName;
    }

    @Override
    void prepare() throws Exception {
      final Path dirPath = new Path(dir);
      if (!dfs.exists(dirPath)) {
        dfs.mkdirs(dirPath);
      }
      
      Path sPath = SnapshotTestHelper.getSnapshotRoot(dirPath, snapshotName);
      if (!dfs.exists(sPath)) {
        dfs.allowSnapshot(dirPath);
        dfs.createSnapshot(dirPath, snapshotName);
      }
    }

    @Override
    void invoke() throws Exception {
      client.deleteSnapshot(dir, snapshotName);
    }

    @Override
    boolean checkNamenodeBeforeReturn() throws Exception {
      final Path sPath = SnapshotTestHelper.getSnapshotRoot(new Path(dir),
          snapshotName);
      boolean snapshotNotDeleted = dfs.exists(sPath);
      for (int i = 0; i < CHECKTIMES && snapshotNotDeleted; i++) {
        Thread.sleep(1000);
        snapshotNotDeleted = dfs.exists(sPath);
      }
      return !snapshotNotDeleted;
    }

    @Override
    Object getResult() {
      return null;
    }
  }
  
  /** renameSnapshot */
  class RenameSnapshotOp extends AtMostOnceOp {
    private String dir;
    private String oldName;
    private String newName;
    
    RenameSnapshotOp(DFSClient client, String dir, String oldName,
        String newName) {
      super("renameSnapshot", client);
      this.dir = dir;
      this.oldName = oldName;
      this.newName = newName;
    }

    @Override
    void prepare() throws Exception {
      final Path dirPath = new Path(dir);
      if (!dfs.exists(dirPath)) {
        dfs.mkdirs(dirPath);
      }
      
      Path sPath = SnapshotTestHelper.getSnapshotRoot(dirPath, oldName);
      if (!dfs.exists(sPath)) {
        dfs.allowSnapshot(dirPath);
        dfs.createSnapshot(dirPath, oldName);
      }
    }

    @Override
    void invoke() throws Exception {
      client.renameSnapshot(dir, oldName, newName);
    }

    @Override
    boolean checkNamenodeBeforeReturn() throws Exception {
      final Path sPath = SnapshotTestHelper.getSnapshotRoot(new Path(dir),
          newName);
      boolean snapshotRenamed = dfs.exists(sPath);
      for (int i = 0; i < CHECKTIMES && !snapshotRenamed; i++) {
        Thread.sleep(1000);
        snapshotRenamed = dfs.exists(sPath);
      }
      return snapshotRenamed;
    }

    @Override
    Object getResult() {
      return null;
    }
  }
  
  /** create file operation (without OverWrite) */
  class CreateOp extends AtMostOnceOp {
    private String fileName;
    private HdfsFileStatus status;
    
    CreateOp(DFSClient client, String fileName) {
      super("create", client);
      this.fileName = fileName;
    }

    @Override
    void prepare() throws Exception {
      final Path filePath = new Path(fileName);
      if (dfs.exists(filePath)) {
        dfs.delete(filePath, true);
      }
      final Path fileParent = filePath.getParent();
      if (!dfs.exists(fileParent)) {
        dfs.mkdirs(fileParent);
      }
    }

    @Override
    void invoke() throws Exception {
      EnumSet<CreateFlag> createFlag = EnumSet.of(CreateFlag.CREATE);
      this.status = client.getNamenode().create(fileName,
          FsPermission.getFileDefault(), client.getClientName(),
          new EnumSetWritable<CreateFlag>(createFlag), false, DataNodes,
          BlockSize);
    }

    @Override
    boolean checkNamenodeBeforeReturn() throws Exception {
      final Path filePath = new Path(fileName);
      boolean fileCreated = dfs.exists(filePath);
      for (int i = 0; i < CHECKTIMES && !fileCreated; i++) {
        Thread.sleep(1000);
        fileCreated = dfs.exists(filePath);
      }
      return fileCreated;
    }

    @Override
    Object getResult() {
      return status;
    }
  }
  
  /** append operation */
  class AppendOp extends AtMostOnceOp {
    private String fileName;
    private LocatedBlock lbk;
    
    AppendOp(DFSClient client, String fileName) {
      super("append", client);
      this.fileName = fileName;
    }

    @Override
    void prepare() throws Exception {
      final Path filePath = new Path(fileName);
      if (!dfs.exists(filePath)) {
        DFSTestUtil.createFile(dfs, filePath, BlockSize / 2, DataNodes, 0);
      }
    }

    @Override
    void invoke() throws Exception {
      lbk = client.getNamenode().append(fileName, client.getClientName());
    }
    
    // check if the inode of the file is under construction
    @Override
    boolean checkNamenodeBeforeReturn() throws Exception {
      INodeFile fileNode = cluster.getNameNode(0).getNamesystem()
          .getFSDirectory().getINode4Write(fileName).asFile();
      boolean fileIsUC = fileNode.isUnderConstruction();
      for (int i = 0; i < CHECKTIMES && !fileIsUC; i++) {
        Thread.sleep(1000);
        fileNode = cluster.getNameNode(0).getNamesystem().getFSDirectory()
            .getINode4Write(fileName).asFile();
        fileIsUC = fileNode.isUnderConstruction();
      }
      return fileIsUC;
    }

    @Override
    Object getResult() {
      return lbk;
    }
  }
  
  /** rename */
  class RenameOp extends AtMostOnceOp {
    private String oldName;
    private String newName;
    private boolean renamed;
    
    RenameOp(DFSClient client, String oldName, String newName) {
      super("rename", client);
      this.oldName = oldName;
      this.newName = newName;
    }

    @Override
    void prepare() throws Exception {
      final Path filePath = new Path(oldName);
      if (!dfs.exists(filePath)) {
        DFSTestUtil.createFile(dfs, filePath, BlockSize, DataNodes, 0);
      }
    }
    
    @SuppressWarnings("deprecation")
    @Override
    void invoke() throws Exception {
      this.renamed = client.rename(oldName, newName);
    }

    @Override
    boolean checkNamenodeBeforeReturn() throws Exception {
      Path targetPath = new Path(newName);
      boolean renamed = dfs.exists(targetPath);
      for (int i = 0; i < CHECKTIMES && !renamed; i++) {
        Thread.sleep(1000);
        renamed = dfs.exists(targetPath);
      }
      return renamed;
    }

    @Override
    Object getResult() {
      return new Boolean(renamed);
    }
  }
  
  /** rename2 */
  class Rename2Op extends AtMostOnceOp {
    private String oldName;
    private String newName;
    
    Rename2Op(DFSClient client, String oldName, String newName) {
      super("rename2", client);
      this.oldName = oldName;
      this.newName = newName;
    }

    @Override
    void prepare() throws Exception {
      final Path filePath = new Path(oldName);
      if (!dfs.exists(filePath)) {
        DFSTestUtil.createFile(dfs, filePath, BlockSize, DataNodes, 0);
      }
    }

    @Override
    void invoke() throws Exception {
      client.rename(oldName, newName, Rename.OVERWRITE);
    }

    @Override
    boolean checkNamenodeBeforeReturn() throws Exception {
      Path targetPath = new Path(newName);
      boolean renamed = dfs.exists(targetPath);
      for (int i = 0; i < CHECKTIMES && !renamed; i++) {
        Thread.sleep(1000);
        renamed = dfs.exists(targetPath);
      }
      return renamed;
    }

    @Override
    Object getResult() {
      return null;
    }
  }
  
  /** concat */
  class ConcatOp extends AtMostOnceOp {
    private String target;
    private String[] srcs;
    private Path[] srcPaths;
    
    ConcatOp(DFSClient client, Path target, int numSrc) {
      super("concat", client);
      this.target = target.toString();
      this.srcs = new String[numSrc];
      this.srcPaths = new Path[numSrc];
      Path parent = target.getParent();
      for (int i = 0; i < numSrc; i++) {
        srcPaths[i] = new Path(parent, "srcfile" + i);
        srcs[i] = srcPaths[i].toString();
      }
    }

    @Override
    void prepare() throws Exception {
      DFSTestUtil.createFile(dfs, new Path(target), BlockSize, DataNodes, 0);
      for (int i = 0; i < srcPaths.length; i++) {
        DFSTestUtil.createFile(dfs, srcPaths[i], BlockSize, DataNodes, 0);
      }
    }

    @Override
    void invoke() throws Exception {
      client.concat(target, srcs);
    }

    @Override
    boolean checkNamenodeBeforeReturn() throws Exception {
      Path targetPath = new Path(target);
      boolean done = dfs.exists(targetPath);
      for (int i = 0; i < CHECKTIMES && !done; i++) {
        Thread.sleep(1000);
        done = dfs.exists(targetPath);
      }
      return done;
    }

    @Override
    Object getResult() {
      return null;
    }
  }
  
  /** delete */
  class DeleteOp extends AtMostOnceOp {
    private String target;
    private boolean deleted;
    
    DeleteOp(DFSClient client, String target) {
      super("delete", client);
      this.target = target;
    }

    @Override
    void prepare() throws Exception {
      Path p = new Path(target);
      if (!dfs.exists(p)) {
        DFSTestUtil.createFile(dfs, p, BlockSize, DataNodes, 0);
      }
    }

    @Override
    void invoke() throws Exception {
      deleted = client.delete(target, true);
    }

    @Override
    boolean checkNamenodeBeforeReturn() throws Exception {
      Path targetPath = new Path(target);
      boolean del = !dfs.exists(targetPath);
      for (int i = 0; i < CHECKTIMES && !del; i++) {
        Thread.sleep(1000);
        del = !dfs.exists(targetPath);
      }
      return del;
    }

    @Override
    Object getResult() {
      return new Boolean(deleted);
    }
  }
  
  /** createSymlink */
  class CreateSymlinkOp extends AtMostOnceOp {
    private String target;
    private String link;
    
    public CreateSymlinkOp(DFSClient client, String target, String link) {
      super("createSymlink", client);
      this.target = target;
      this.link = link;
    }

    @Override
    void prepare() throws Exception {
      Path p = new Path(target);
      if (!dfs.exists(p)) {
        DFSTestUtil.createFile(dfs, p, BlockSize, DataNodes, 0);
      }
    }

    @Override
    void invoke() throws Exception {
      client.createSymlink(target, link, false);
    }

    @Override
    boolean checkNamenodeBeforeReturn() throws Exception {
      Path linkPath = new Path(link);
      FileStatus linkStatus = null;
      for (int i = 0; i < CHECKTIMES && linkStatus == null; i++) {
        try {
          linkStatus = dfs.getFileLinkStatus(linkPath);
        } catch (FileNotFoundException fnf) {
          // Ignoring, this can be legitimate.
          Thread.sleep(1000);
        }
      }
      return linkStatus != null;
    }

    @Override
    Object getResult() {
      return null;
    }
  }
  
  /** updatePipeline */
  class UpdatePipelineOp extends AtMostOnceOp {
    private String file;
    private ExtendedBlock oldBlock;
    private ExtendedBlock newBlock;
    private DatanodeInfo[] nodes;
    private FSDataOutputStream out;
    
    public UpdatePipelineOp(DFSClient client, String file) {
      super("updatePipeline", client);
      this.file = file;
    }

    @Override
    void prepare() throws Exception {
      final Path filePath = new Path(file);
      DFSTestUtil.createFile(dfs, filePath, BlockSize, DataNodes, 0);
      // append to the file and leave the last block under construction
      out = this.client.append(file, BlockSize, null, null);
      byte[] appendContent = new byte[100];
      new Random().nextBytes(appendContent);
      out.write(appendContent);
      ((HdfsDataOutputStream) out).hsync(EnumSet.of(SyncFlag.UPDATE_LENGTH));
      
      LocatedBlocks blks = dfs.getClient()
          .getLocatedBlocks(file, BlockSize + 1);
      assertEquals(1, blks.getLocatedBlocks().size());
      nodes = blks.get(0).getLocations();
      oldBlock = blks.get(0).getBlock();
      
      LocatedBlock newLbk = client.getNamenode().updateBlockForPipeline(
          oldBlock, client.getClientName());
      newBlock = new ExtendedBlock(oldBlock.getBlockPoolId(),
          oldBlock.getBlockId(), oldBlock.getNumBytes(), 
          newLbk.getBlock().getGenerationStamp());
    }

    @Override
    void invoke() throws Exception {
      DatanodeInfo[] newNodes = new DatanodeInfo[2];
      newNodes[0] = nodes[0];
      newNodes[1] = nodes[1];
      
      client.getNamenode().updatePipeline(client.getClientName(), oldBlock,
          newBlock, newNodes);
      out.close();
    }

    @Override
    boolean checkNamenodeBeforeReturn() throws Exception {
      INodeFileUnderConstruction fileNode = (INodeFileUnderConstruction) cluster
          .getNamesystem(0).getFSDirectory().getINode4Write(file).asFile();
      BlockInfoUnderConstruction blkUC = 
          (BlockInfoUnderConstruction) (fileNode.getBlocks())[1];
      int datanodeNum = blkUC.getExpectedLocations().length;
      for (int i = 0; i < CHECKTIMES && datanodeNum != 2; i++) {
        Thread.sleep(1000);
        datanodeNum = blkUC.getExpectedLocations().length;
      }
      return datanodeNum == 2;
    }
    
    @Override
    Object getResult() {
      return null;
    }
  }
  
  /** addPathBasedCacheDirective */
  class AddPathBasedCacheDirectiveOp extends AtMostOnceOp {
    private PathBasedCacheDirective directive;
    private Long result;

    AddPathBasedCacheDirectiveOp(DFSClient client,
        PathBasedCacheDirective directive) {
      super("addPathBasedCacheDirective", client);
      this.directive = directive;
    }

    @Override
    void prepare() throws Exception {
      dfs.addCachePool(new CachePoolInfo(directive.getPool()));
    }

    @Override
    void invoke() throws Exception {
      result = client.addPathBasedCacheDirective(directive);
    }

    @Override
    boolean checkNamenodeBeforeReturn() throws Exception {
      for (int i = 0; i < CHECKTIMES; i++) {
        RemoteIterator<PathBasedCacheDirective> iter =
            dfs.listPathBasedCacheDirectives(
                new PathBasedCacheDirective.Builder().
                    setPool(directive.getPool()).
                    setPath(directive.getPath()).
                    build());
        if (iter.hasNext()) {
          return true;
        }
        Thread.sleep(1000);
      }
      return false;
    }

    @Override
    Object getResult() {
      return result;
    }
  }

  /** modifyPathBasedCacheDirective */
  class ModifyPathBasedCacheDirectiveOp extends AtMostOnceOp {
    private final PathBasedCacheDirective directive;
    private final short newReplication;
    private long id;

    ModifyPathBasedCacheDirectiveOp(DFSClient client,
        PathBasedCacheDirective directive, short newReplication) {
      super("modifyPathBasedCacheDirective", client);
      this.directive = directive;
      this.newReplication = newReplication;
    }

    @Override
    void prepare() throws Exception {
      dfs.addCachePool(new CachePoolInfo(directive.getPool()));
      id = client.addPathBasedCacheDirective(directive);
    }

    @Override
    void invoke() throws Exception {
      client.modifyPathBasedCacheDirective(
          new PathBasedCacheDirective.Builder().
              setId(id).
              setReplication(newReplication).
              build());
    }

    @Override
    boolean checkNamenodeBeforeReturn() throws Exception {
      for (int i = 0; i < CHECKTIMES; i++) {
        RemoteIterator<PathBasedCacheDirective> iter =
            dfs.listPathBasedCacheDirectives(
                new PathBasedCacheDirective.Builder().
                    setPool(directive.getPool()).
                    setPath(directive.getPath()).
                    build());
        while (iter.hasNext()) {
          PathBasedCacheDirective result = iter.next();
          if ((result.getId() == id) &&
              (result.getReplication().shortValue() == newReplication)) {
            return true;
          }
        }
        Thread.sleep(1000);
      }
      return false;
    }

    @Override
    Object getResult() {
      return null;
    }
  }

  /** removePathBasedCacheDirective */
  class RemovePathBasedCacheDirectiveOp extends AtMostOnceOp {
    private PathBasedCacheDirective directive;
    private long id;

    RemovePathBasedCacheDirectiveOp(DFSClient client, String pool,
        String path) {
      super("removePathBasedCacheDirective", client);
      this.directive = new PathBasedCacheDirective.Builder().
          setPool(pool).
          setPath(new Path(path)).
          build();
    }

    @Override
    void prepare() throws Exception {
      dfs.addCachePool(new CachePoolInfo(directive.getPool()));
      id = dfs.addPathBasedCacheDirective(directive);
    }

    @Override
    void invoke() throws Exception {
      client.removePathBasedCacheDirective(id);
    }

    @Override
    boolean checkNamenodeBeforeReturn() throws Exception {
      for (int i = 0; i < CHECKTIMES; i++) {
        RemoteIterator<PathBasedCacheDirective> iter =
            dfs.listPathBasedCacheDirectives(
                new PathBasedCacheDirective.Builder().
                  setPool(directive.getPool()).
                  setPath(directive.getPath()).
                  build());
        if (!iter.hasNext()) {
          return true;
        }
        Thread.sleep(1000);
      }
      return false;
    }

    @Override
    Object getResult() {
      return null;
    }
  }

  /** addCachePool */
  class AddCachePoolOp extends AtMostOnceOp {
    private String pool;

    AddCachePoolOp(DFSClient client, String pool) {
      super("addCachePool", client);
      this.pool = pool;
    }

    @Override
    void prepare() throws Exception {
    }

    @Override
    void invoke() throws Exception {
      client.addCachePool(new CachePoolInfo(pool));
    }

    @Override
    boolean checkNamenodeBeforeReturn() throws Exception {
      for (int i = 0; i < CHECKTIMES; i++) {
        RemoteIterator<CachePoolInfo> iter = dfs.listCachePools();
        if (iter.hasNext()) {
          return true;
        }
        Thread.sleep(1000);
      }
      return false;
    }

    @Override
    Object getResult() {
      return null;
    }
  }

  /** modifyCachePool */
  class ModifyCachePoolOp extends AtMostOnceOp {
    String pool;

    ModifyCachePoolOp(DFSClient client, String pool) {
      super("modifyCachePool", client);
      this.pool = pool;
    }

    @Override
    void prepare() throws Exception {
      client.addCachePool(new CachePoolInfo(pool).setWeight(10));
    }

    @Override
    void invoke() throws Exception {
      client.modifyCachePool(new CachePoolInfo(pool).setWeight(99));
    }

    @Override
    boolean checkNamenodeBeforeReturn() throws Exception {
      for (int i = 0; i < CHECKTIMES; i++) {
        RemoteIterator<CachePoolInfo> iter = dfs.listCachePools();
        if (iter.hasNext() && iter.next().getWeight() == 99) {
          return true;
        }
        Thread.sleep(1000);
      }
      return false;
    }

    @Override
    Object getResult() {
      return null;
    }
  }

  /** removeCachePool */
  class RemoveCachePoolOp extends AtMostOnceOp {
    private String pool;

    RemoveCachePoolOp(DFSClient client, String pool) {
      super("removeCachePool", client);
      this.pool = pool;
    }

    @Override
    void prepare() throws Exception {
      client.addCachePool(new CachePoolInfo(pool));
    }

    @Override
    void invoke() throws Exception {
      client.removeCachePool(pool);
    }

    @Override
    boolean checkNamenodeBeforeReturn() throws Exception {
      for (int i = 0; i < CHECKTIMES; i++) {
        RemoteIterator<CachePoolInfo> iter = dfs.listCachePools();
        if (!iter.hasNext()) {
          return true;
        }
        Thread.sleep(1000);
      }
      return false;
    }

    @Override
    Object getResult() {
      return null;
    }
  }

  @Test (timeout=60000)
  public void testCreateSnapshot() throws Exception {
    final DFSClient client = genClientWithDummyHandler();
    AtMostOnceOp op = new CreateSnapshotOp(client, "/test", "s1");
    testClientRetryWithFailover(op);
  }
  
  @Test (timeout=60000)
  public void testDeleteSnapshot() throws Exception {
    final DFSClient client = genClientWithDummyHandler();
    AtMostOnceOp op = new DeleteSnapshotOp(client, "/test", "s1");
    testClientRetryWithFailover(op);
  }
  
  @Test (timeout=60000)
  public void testRenameSnapshot() throws Exception {
    final DFSClient client = genClientWithDummyHandler();
    AtMostOnceOp op = new RenameSnapshotOp(client, "/test", "s1", "s2");
    testClientRetryWithFailover(op);
  }
  
  @Test (timeout=60000)
  public void testCreate() throws Exception {
    final DFSClient client = genClientWithDummyHandler();
    AtMostOnceOp op = new CreateOp(client, "/testfile");
    testClientRetryWithFailover(op);
  }
  
  @Test (timeout=60000)
  public void testAppend() throws Exception {
    final DFSClient client = genClientWithDummyHandler();
    AtMostOnceOp op = new AppendOp(client, "/testfile");
    testClientRetryWithFailover(op);
  }
  
  @Test (timeout=60000)
  public void testRename() throws Exception {
    final DFSClient client = genClientWithDummyHandler();
    AtMostOnceOp op = new RenameOp(client, "/file1", "/file2");
    testClientRetryWithFailover(op);
  }
  
  @Test (timeout=60000)
  public void testRename2() throws Exception {
    final DFSClient client = genClientWithDummyHandler();
    AtMostOnceOp op = new Rename2Op(client, "/file1", "/file2");
    testClientRetryWithFailover(op);
  }
  
  @Test (timeout=60000)
  public void testConcat() throws Exception {
    final DFSClient client = genClientWithDummyHandler();
    AtMostOnceOp op = new ConcatOp(client, new Path("/test/file"), 5);
    testClientRetryWithFailover(op);
  }
  
  @Test (timeout=60000)
  public void testDelete() throws Exception {
    final DFSClient client = genClientWithDummyHandler();
    AtMostOnceOp op = new DeleteOp(client, "/testfile");
    testClientRetryWithFailover(op);
  }
  
  @Test (timeout=60000)
  public void testCreateSymlink() throws Exception {
    final DFSClient client = genClientWithDummyHandler();
    AtMostOnceOp op = new CreateSymlinkOp(client, "/testfile", "/testlink");
    testClientRetryWithFailover(op);
  }
  
  @Test (timeout=60000)
  public void testUpdatePipeline() throws Exception {
    final DFSClient client = genClientWithDummyHandler();
    AtMostOnceOp op = new UpdatePipelineOp(client, "/testfile");
    testClientRetryWithFailover(op);
  }
  
  @Test (timeout=60000)
  public void testAddPathBasedCacheDirective() throws Exception {
    DFSClient client = genClientWithDummyHandler();
    AtMostOnceOp op = new AddPathBasedCacheDirectiveOp(client, 
        new PathBasedCacheDirective.Builder().
            setPool("pool").
            setPath(new Path("/path")).
            build());
    testClientRetryWithFailover(op);
  }

  @Test (timeout=60000)
  public void testModifyPathBasedCacheDirective() throws Exception {
    DFSClient client = genClientWithDummyHandler();
    AtMostOnceOp op = new ModifyPathBasedCacheDirectiveOp(client, 
        new PathBasedCacheDirective.Builder().
            setPool("pool").
            setPath(new Path("/path")).
            setReplication((short)1).build(),
        (short)555);
    testClientRetryWithFailover(op);
  }

  @Test (timeout=60000)
  public void testRemovePathBasedCacheDescriptor() throws Exception {
    DFSClient client = genClientWithDummyHandler();
    AtMostOnceOp op = new RemovePathBasedCacheDirectiveOp(client, "pool",
        "/path");
    testClientRetryWithFailover(op);
  }

  @Test (timeout=60000)
  public void testAddCachePool() throws Exception {
    DFSClient client = genClientWithDummyHandler();
    AtMostOnceOp op = new AddCachePoolOp(client, "pool");
    testClientRetryWithFailover(op);
  }

  @Test (timeout=60000)
  public void testModifyCachePool() throws Exception {
    DFSClient client = genClientWithDummyHandler();
    AtMostOnceOp op = new ModifyCachePoolOp(client, "pool");
    testClientRetryWithFailover(op);
  }

  @Test (timeout=60000)
  public void testRemoveCachePool() throws Exception {
    DFSClient client = genClientWithDummyHandler();
    AtMostOnceOp op = new RemoveCachePoolOp(client, "pool");
    testClientRetryWithFailover(op);
  }

  /**
   * When NN failover happens, if the client did not receive the response and
   * send a retry request to the other NN, the same response should be recieved
   * based on the retry cache.
   */
  public void testClientRetryWithFailover(final AtMostOnceOp op)
      throws Exception {
    final Map<String, Object> results = new HashMap<String, Object>();
    
    op.prepare();
    // set DummyRetryInvocationHandler#block to true
    DummyRetryInvocationHandler.block.set(true);
    
    new Thread() {
      @Override
      public void run() {
        try {
          op.invoke();
          Object result = op.getResult();
          LOG.info("Operation " + op.name + " finished");
          synchronized (TestRetryCacheWithHA.this) {
            results.put(op.name, result == null ? "SUCCESS" : result);
            TestRetryCacheWithHA.this.notifyAll();
          }
        } catch (Exception e) {
          LOG.info("Got Exception while calling " + op.name, e);
        } finally {
          IOUtils.cleanup(null, op.client);
        }
      }
    }.start();
    
    // make sure the client's call has actually been handled by the active NN
    assertTrue("After waiting the operation " + op.name
        + " still has not taken effect on NN yet",
        op.checkNamenodeBeforeReturn());
    
    // force the failover
    cluster.transitionToStandby(0);
    cluster.transitionToActive(1);
    // disable the block in DummyHandler
    LOG.info("Setting block to false");
    DummyRetryInvocationHandler.block.set(false);
    
    synchronized (this) {
      while (!results.containsKey(op.name)) {
        this.wait();
      }
      LOG.info("Got the result of " + op.name + ": "
          + results.get(op.name));
    }
  }
}
