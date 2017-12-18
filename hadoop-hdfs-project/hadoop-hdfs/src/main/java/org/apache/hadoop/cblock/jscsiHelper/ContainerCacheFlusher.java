/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.cblock.jscsiHelper;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.cblock.CBlockConfigKeys;
import org.apache.hadoop.cblock.jscsiHelper.cache.LogicalBlock;
import org.apache.hadoop.cblock.jscsiHelper.cache.impl.AsyncBlockWriter;
import org.apache.hadoop.cblock.jscsiHelper.cache.impl.DiskBlock;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.scm.XceiverClientManager;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.utils.LevelDBStore;
import org.iq80.leveldb.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.hadoop.cblock.CBlockConfigKeys
    .DFS_CBLOCK_CACHE_BLOCK_BUFFER_SIZE;
import static org.apache.hadoop.cblock.CBlockConfigKeys
    .DFS_CBLOCK_CACHE_BLOCK_BUFFER_SIZE_DEFAULT;
import static org.apache.hadoop.cblock.CBlockConfigKeys
    .DFS_CBLOCK_CACHE_CORE_MIN_POOL_SIZE;
import static org.apache.hadoop.cblock.CBlockConfigKeys
    .DFS_CBLOCK_CACHE_CORE_MIN_POOL_SIZE_DEFAULT;
import static org.apache.hadoop.cblock.CBlockConfigKeys
    .DFS_CBLOCK_CACHE_KEEP_ALIVE;
import static org.apache.hadoop.cblock.CBlockConfigKeys
    .DFS_CBLOCK_CACHE_KEEP_ALIVE_DEFAULT;
import static org.apache.hadoop.cblock.CBlockConfigKeys
    .DFS_CBLOCK_CACHE_MAX_POOL_SIZE;
import static org.apache.hadoop.cblock.CBlockConfigKeys
    .DFS_CBLOCK_CACHE_MAX_POOL_SIZE_DEFAULT;
import static org.apache.hadoop.cblock.CBlockConfigKeys
    .DFS_CBLOCK_CACHE_QUEUE_SIZE_KB;
import static org.apache.hadoop.cblock.CBlockConfigKeys
    .DFS_CBLOCK_CACHE_QUEUE_SIZE_KB_DEFAULT;
import static org.apache.hadoop.cblock.CBlockConfigKeys
    .DFS_CBLOCK_CACHE_THREAD_PRIORITY;
import static org.apache.hadoop.cblock.CBlockConfigKeys
    .DFS_CBLOCK_CACHE_THREAD_PRIORITY_DEFAULT;
import static org.apache.hadoop.cblock.CBlockConfigKeys
    .DFS_CBLOCK_CACHE_LEVELDB_CACHE_SIZE_MB_KEY;
import static org.apache.hadoop.cblock.CBlockConfigKeys
    .DFS_CBLOCK_CACHE_LEVELDB_CACHE_SIZE_MB_DEFAULT;

/**
 * Class that writes to remote containers.
 */
public class ContainerCacheFlusher implements Runnable {
  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerCacheFlusher.class);
  private final LinkedBlockingQueue<Message> messageQueue;
  private final ThreadPoolExecutor threadPoolExecutor;
  private final ArrayBlockingQueue<Runnable> workQueue;
  private final ConcurrentMap<String, RefCountedDB> dbMap;
  private final ByteBuffer blockIDBuffer;
  private final ConcurrentMap<String, Pipeline[]> pipelineMap;
  private final AtomicLong remoteIO;
  private final XceiverClientManager xceiverClientManager;
  private final CBlockTargetMetrics metrics;
  private AtomicBoolean shutdown;
  private final long levelDBCacheSize;
  private final int maxRetryCount;
  private final String tracePrefix;

  private final ConcurrentMap<String, FinishCounter> finishCountMap;

  /**
   * Constructs the writers to remote queue.
   */
  public ContainerCacheFlusher(Configuration config,
      XceiverClientManager xceiverClientManager,
      CBlockTargetMetrics metrics) {
    int queueSize = config.getInt(DFS_CBLOCK_CACHE_QUEUE_SIZE_KB,
        DFS_CBLOCK_CACHE_QUEUE_SIZE_KB_DEFAULT) * 1024;
    int corePoolSize = config.getInt(DFS_CBLOCK_CACHE_CORE_MIN_POOL_SIZE,
        DFS_CBLOCK_CACHE_CORE_MIN_POOL_SIZE_DEFAULT);
    int maxPoolSize = config.getInt(DFS_CBLOCK_CACHE_MAX_POOL_SIZE,
        DFS_CBLOCK_CACHE_MAX_POOL_SIZE_DEFAULT);
    long keepAlive = config.getTimeDuration(DFS_CBLOCK_CACHE_KEEP_ALIVE,
        DFS_CBLOCK_CACHE_KEEP_ALIVE_DEFAULT, TimeUnit.SECONDS);
    int threadPri = config.getInt(DFS_CBLOCK_CACHE_THREAD_PRIORITY,
        DFS_CBLOCK_CACHE_THREAD_PRIORITY_DEFAULT);
    int blockBufferSize = config.getInt(DFS_CBLOCK_CACHE_BLOCK_BUFFER_SIZE,
        DFS_CBLOCK_CACHE_BLOCK_BUFFER_SIZE_DEFAULT) * (Long.SIZE / Byte.SIZE);
    levelDBCacheSize = config.getInt(DFS_CBLOCK_CACHE_LEVELDB_CACHE_SIZE_MB_KEY,
        DFS_CBLOCK_CACHE_LEVELDB_CACHE_SIZE_MB_DEFAULT) * OzoneConsts.MB;

    LOG.info("Cache: Core Pool Size: {}", corePoolSize);
    LOG.info("Cache: Keep Alive: {}", keepAlive);
    LOG.info("Cache: Max Pool Size: {}", maxPoolSize);
    LOG.info("Cache: Thread Pri: {}", threadPri);
    LOG.info("Cache: BlockBuffer Size: {}", blockBufferSize);

    shutdown = new AtomicBoolean(false);
    messageQueue = new LinkedBlockingQueue<>();
    workQueue = new ArrayBlockingQueue<>(queueSize, true);

    ThreadFactory workerThreadFactory = new ThreadFactoryBuilder()
        .setNameFormat("Cache Block Writer Thread #%d")
        .setDaemon(true)
        .setPriority(threadPri)
        .build();
    threadPoolExecutor = new ThreadPoolExecutor(corePoolSize, maxPoolSize,
        keepAlive, TimeUnit.SECONDS, workQueue, workerThreadFactory,
        new ThreadPoolExecutor.AbortPolicy());
    threadPoolExecutor.prestartAllCoreThreads();

    dbMap = new ConcurrentHashMap<>();
    pipelineMap = new ConcurrentHashMap<>();
    blockIDBuffer = ByteBuffer.allocateDirect(blockBufferSize);
    this.xceiverClientManager = xceiverClientManager;
    this.metrics = metrics;
    this.remoteIO = new AtomicLong();

    this.finishCountMap = new ConcurrentHashMap<>();
    this.maxRetryCount =
        config.getInt(CBlockConfigKeys.DFS_CBLOCK_CACHE_MAX_RETRY_KEY,
            CBlockConfigKeys.DFS_CBLOCK_CACHE_MAX_RETRY_DEFAULT);
    this.tracePrefix = getTracePrefix();
  }

  private void checkExistingLog(String prefixFileName, File dbPath) {
    if (!dbPath.exists()) {
      LOG.debug("No existing dirty log found at {}", dbPath);
      return;
    }
    LOG.debug("Need to check and requeue existing dirty log {}", dbPath);
    HashMap<String, ArrayList<String>> allFiles = new HashMap<>();
    traverse(prefixFileName, dbPath, allFiles);
    for (Map.Entry<String, ArrayList<String>> entry : allFiles.entrySet()) {
      String parentPath = entry.getKey();
      for (String fileName : entry.getValue()) {
        LOG.info("found {} {} with prefix {}",
            parentPath, fileName, prefixFileName);
        processDirtyBlocks(parentPath, fileName);
      }
    }
  }

  private void traverse(String prefixFileName, File path,
                        HashMap<String, ArrayList<String>> files) {
    if (path.isFile()) {
      if (path.getName().startsWith(prefixFileName)) {
        LOG.debug("found this {} with {}", path.getParent(), path.getName());
        if (!files.containsKey(path.getParent())) {
          files.put(path.getParent(), new ArrayList<>());
        }
        files.get(path.getParent()).add(path.getName());
      }
    } else {
      File[] listFiles = path.listFiles();
      if (listFiles != null) {
        for (File subPath : listFiles) {
          traverse(prefixFileName, subPath, files);
        }
      }
    }
  }

  /**
   * Gets the CBlockTargetMetrics.
   *
   * @return CBlockTargetMetrics
   */
  public CBlockTargetMetrics getTargetMetrics() {
    return metrics;
  }

  /**
   * Gets the  getXceiverClientManager.
   *
   * @return XceiverClientManager
   */
  public XceiverClientManager getXceiverClientManager() {
    return xceiverClientManager;
  }

  /**
   * Shutdown this instance.
   */
  public void shutdown() {
    this.shutdown.set(true);
    threadPoolExecutor.shutdown();
  }

  public long incrementRemoteIO() {
    return remoteIO.incrementAndGet();
  }

  /**
   * Processes a block cache file and queues those blocks for the remote I/O.
   *
   * @param dbPath - Location where the DB can be found.
   * @param fileName - Block Cache File Name
   */
  public void processDirtyBlocks(String dbPath, String fileName) {
    LOG.info("Adding {}/{} to queue. Queue Length: {}", dbPath, fileName,
        messageQueue.size());
    this.messageQueue.add(new Message(dbPath, fileName));
  }

  public Logger getLOG() {
    return LOG;
  }

  /**
   * Opens a DB if needed or returns a handle to an already open DB.
   *
   * @param dbPath -- dbPath
   * @return the levelDB on the given path.
   * @throws IOException
   */
  public synchronized LevelDBStore openDB(String dbPath)
      throws IOException {
    if (dbMap.containsKey(dbPath)) {
      RefCountedDB refDB = dbMap.get(dbPath);
      refDB.open();
      return refDB.db;
    } else {
      Options options = new Options();
      options.cacheSize(levelDBCacheSize);
      options.createIfMissing(true);
      LevelDBStore cacheDB = new LevelDBStore(
          new File(getDBFileName(dbPath)), options);
      RefCountedDB refDB = new RefCountedDB(dbPath, cacheDB);
      dbMap.put(dbPath, refDB);
      return cacheDB;
    }
  }

  /**
   * Updates the container map. This data never changes so we will update this
   * during restarts and it should not hurt us.
   *
   * Once a CBlockLocalCache cache is registered, requeue dirty/retry log files
   * for the volume
   *
   * @param dbPath - DbPath
   * @param containerList - Container List.
   */
  public void register(String dbPath, Pipeline[] containerList) {
    File dbFile = Paths.get(dbPath).toFile();
    pipelineMap.put(dbPath, containerList);
    checkExistingLog(AsyncBlockWriter.DIRTY_LOG_PREFIX, dbFile);
    checkExistingLog(AsyncBlockWriter.RETRY_LOG_PREFIX, dbFile);
  }

  private String getDBFileName(String dbPath) {
    return dbPath + ".db";
  }

  public LevelDBStore getCacheDB(String dbPath) throws IOException {
    return openDB(dbPath);
  }

  public void releaseCacheDB(String dbPath) {
    try {
      closeDB(dbPath);
    } catch (Exception e) {
      metrics.incNumFailedReleaseLevelDB();
      LOG.error("LevelDB close failed, dbPath:" + dbPath, e);
    }
  }
  /**
   * Close the DB if we don't have any outstanding references.
   *
   * @param dbPath - dbPath
   * @throws IOException
   */
  public synchronized void closeDB(String dbPath) throws IOException {
    if (dbMap.containsKey(dbPath)) {
      RefCountedDB refDB = dbMap.get(dbPath);
      int count = refDB.close();
      if (count == 0) {
        dbMap.remove(dbPath);
      }
    }
  }

  Pipeline getPipeline(String dbPath, long blockId) {
    Pipeline[] containerList = pipelineMap.get(dbPath);
    Preconditions.checkNotNull(containerList);
    int containerIdx = (int) blockId % containerList.length;
    long cBlockIndex =
        Longs.fromByteArray(containerList[containerIdx].getData());
    if (cBlockIndex > 0) {
      // This catches the case when we get a wrong container in the ordering
      // of the containers.
      Preconditions.checkState(containerIdx % cBlockIndex == 0,
          "The container ID computed should match with the container index " +
              "returned from cBlock Server.");
    }
    return containerList[containerIdx];
  }

  public void incFinishCount(String fileName) {
    if (!finishCountMap.containsKey(fileName)) {
      LOG.error("No record for such file:" + fileName);
      return;
    }
    finishCountMap.get(fileName).incCount();
    if (finishCountMap.get(fileName).isFileDeleted()) {
      finishCountMap.remove(fileName);
    }
  }

  /**
   * When an object implementing interface <code>Runnable</code> is used
   * to create a thread, starting the thread causes the object's
   * <code>run</code> method to be called in that separately executing
   * thread.
   * <p>
   * The general contract of the method <code>run</code> is that it may
   * take any action whatsoever.
   *
   * @see Thread#run()
   */
  @Override
  public void run() {
    while (!this.shutdown.get()) {
      try {
        Message message = messageQueue.take();
        LOG.debug("Got message to process -- DB Path : {} , FileName; {}",
            message.getDbPath(), message.getFileName());
        String fullPath = Paths.get(message.getDbPath(),
            message.getFileName()).toString();
        String[] fileNameParts = message.getFileName().split("\\.");
        Preconditions.checkState(fileNameParts.length > 1);
        String fileType = fileNameParts[0];
        boolean isDirtyLogFile =
            fileType.equalsIgnoreCase(AsyncBlockWriter.DIRTY_LOG_PREFIX);
        ReadableByteChannel fileChannel = new FileInputStream(fullPath)
            .getChannel();
        // TODO: We can batch and unique the IOs here. First getting the code
        // to work, we will add those later.
        int bytesRead = fileChannel.read(blockIDBuffer);
        fileChannel.close();
        LOG.debug("Read blockID log of size: {} position {} remaining {}",
            bytesRead, blockIDBuffer.position(), blockIDBuffer.remaining());
        // current position of in the buffer in bytes, divided by number of
        // bytes per long (which is calculated by number of bits per long
        // divided by number of bits per byte) gives the number of blocks
        int blockCount = blockIDBuffer.position()/(Long.SIZE / Byte.SIZE);
        if (isDirtyLogFile) {
          getTargetMetrics().incNumBytesDirtyLogRead(bytesRead);
        } else {
          getTargetMetrics().incNumBytesRetryLogRead(bytesRead);
        }
        if (finishCountMap.containsKey(message.getFileName())) {
          // In theory this should never happen. But if it happened,
          // we need to know it...
          getTargetMetrics().incNumIllegalDirtyLogFiles();
          LOG.error("Adding DirtyLog file again {} current count {} new {}",
              message.getFileName(),
              finishCountMap.get(message.getFileName()).expectedCount,
              blockCount);
        }
        finishCountMap.put(message.getFileName(),
            new FinishCounter(blockCount, message.getDbPath(),
                message.getFileName(), this));
        // should be flip instead of rewind, because we also need to make sure
        // the end position is correct.
        blockIDBuffer.flip();
        LOG.debug("Remaining blocks count {} and {}", blockIDBuffer.remaining(),
            blockCount);
        while (blockIDBuffer.remaining() >= (Long.SIZE / Byte.SIZE)) {
          long blockID = blockIDBuffer.getLong();
          int retryCount = 0;
          if (isDirtyLogFile) {
            getTargetMetrics().incNumDirtyLogBlockRead();
          } else {
            getTargetMetrics().incNumRetryLogBlockRead();
            Preconditions.checkState(fileNameParts.length == 4);
            retryCount = Integer.parseInt(fileNameParts[3]);
          }
          LogicalBlock block = new DiskBlock(blockID, null, false);
          BlockWriterTask blockWriterTask = new BlockWriterTask(block, this,
              message.getDbPath(), retryCount, message.getFileName(),
              maxRetryCount);
          threadPoolExecutor.submit(blockWriterTask);
        }
        blockIDBuffer.clear();
      } catch (InterruptedException e) {
        LOG.info("ContainerCacheFlusher is interrupted.", e);
      } catch (FileNotFoundException e) {
        LOG.error("Unable to find the dirty blocks file. This will cause " +
            "data errors. Please stop using this volume.", e);
      } catch (IOException e) {
        LOG.error("Unable to read the dirty blocks file. This will cause " +
            "data errors. Please stop using this volume.", e);
      } catch (Exception e) {
        LOG.error("Generic exception.", e);
      }
    }
    LOG.info("Exiting flusher");
  }

  /**
   * Tries to get the local host IP Address as trace prefix
   * for creating trace IDs, otherwise uses a random UUID for it.
   */
  private static String getTracePrefix() {
    String tmp;
    try {
      tmp = InetAddress.getLocalHost().getHostAddress();
    } catch (UnknownHostException ex) {
      tmp = UUID.randomUUID().toString();
      LOG.error("Unable to read the host address. Using a GUID for " +
          "hostname:{} ", tmp, ex);
    }
    return tmp;
  }

  /**
   * We create a trace ID to make it easy to debug issues.
   * A trace ID is in IPAddress:UserName:VolumeName:blockID:second format.
   *
   * This will get written down on the data node if we get any failures, so
   * with this trace ID we can correlate cBlock failures across machines.
   *
   * @param blockID - Block ID
   * @return trace ID
   */
  public String getTraceID(File dbPath, long blockID) {
    String volumeName = dbPath.getName();
    String userName = dbPath.getParentFile().getName();
    // mapping to seconds to make the string smaller.
    return tracePrefix + ":" + userName + ":" + volumeName
        + ":" + blockID + ":" + Time.monotonicNow() / 1000;
  }

  /**
   * Keeps a Reference counted DB that we close only when the total Reference
   * has gone to zero.
   */
  private static class RefCountedDB {
    private LevelDBStore db;
    private AtomicInteger refcount;
    private String dbPath;

    /**
     * RefCountedDB DB ctor.
     *
     * @param dbPath - DB path.
     * @param db - LevelDBStore db
     */
    RefCountedDB(String dbPath, LevelDBStore db) {
      this.db = db;
      this.refcount = new AtomicInteger(1);
      this.dbPath = dbPath;
    }

    /**
     * close the DB if possible.
     */
    public int close() throws IOException {
      int count = this.refcount.decrementAndGet();
      if (count == 0) {
        LOG.info("Closing the LevelDB. {} ", this.dbPath);
        db.close();
      }
      return count;
    }

    public void open() {
      this.refcount.incrementAndGet();
    }
  }

  /**
   * The message held in processing queue.
   */
  private static class Message {
    private String dbPath;
    private String fileName;

    /**
     * A message that holds the info about which path dirty blocks log and
     * which path contains db.
     *
     * @param dbPath
     * @param fileName
     */
    Message(String dbPath, String fileName) {
      this.dbPath = dbPath;
      this.fileName = fileName;
    }

    public String getDbPath() {
      return dbPath;
    }

    public void setDbPath(String dbPath) {
      this.dbPath = dbPath;
    }

    public String getFileName() {
      return fileName;
    }

    public void setFileName(String fileName) {
      this.fileName = fileName;
    }
  }

  private static class FinishCounter {
    private final long expectedCount;
    private final String dbPath;
    private final String dirtyLogPath;
    private final AtomicLong currentCount;
    private AtomicBoolean fileDeleted;
    private final ContainerCacheFlusher flusher;

    FinishCounter(long expectedCount, String dbPath,
        String dirtyLogPath, ContainerCacheFlusher flusher) throws IOException {
      this.expectedCount = expectedCount;
      this.dbPath = dbPath;
      this.dirtyLogPath = dirtyLogPath;
      this.currentCount = new AtomicLong(0);
      this.fileDeleted = new AtomicBoolean(false);
      this.flusher = flusher;
    }

    public boolean isFileDeleted() {
      return fileDeleted.get();
    }

    public void incCount() {
      long count = this.currentCount.incrementAndGet();
      if (count >= expectedCount) {
        String filePath = String.format("%s/%s", dbPath, dirtyLogPath);
        LOG.debug(
            "Deleting {} with count {} {}", filePath, count, expectedCount);
        try {
          Path path = Paths.get(filePath);
          Files.delete(path);
          // the following part tries to remove the directory if it is empty
          // but not sufficient, because the .db directory still exists....
          // TODO how to handle the .db directory?
          /*Path parent = path.getParent();
          if (parent.toFile().listFiles().length == 0) {
            Files.delete(parent);
          }*/
          fileDeleted.set(true);
        } catch (Exception e) {
          flusher.getTargetMetrics().incNumFailedDirtyLogFileDeletes();
          LOG.error("Error deleting dirty log file:" + filePath, e);
        }
      }
    }
  }
}
