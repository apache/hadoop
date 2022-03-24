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
package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsVolumeImpl;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.util.NativeCodeLoader;
import org.apache.hadoop.util.Shell;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DISK_STAT_INTERVAL_SECONDS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DISK_STAT_INTERVAL_SECONDS_KEY;

public class DiskIOUtilManager implements Runnable {
  public static final Logger LOG =
      LoggerFactory.getLogger(DiskIOUtilManager.class);
  private volatile long intervalMs;
  private volatile boolean shouldStop = false;
  private Thread diskStatThread;
  private Map<StorageLocation, DiskLocation> locationToDisks = new HashMap<>();
  private Map<DiskLocation, IOStat> ioStats = new HashMap<>();

  private static class DiskLocation {
    private final String diskName;
    private final StorageLocation location;
    DiskLocation(StorageLocation location) throws IOException {
      this.location = location;
      FileStore fs = Files.getFileStore(Paths.get(location.getUri().getPath()));
      Path path = Paths.get(fs.name());
      if (path == null) {
        throw new IOException("Storage location is invalid, path is null");
      }
      Path diskName = path.getFileName();
      if (diskName == null) {
        throw new IOException("Storage location is invalid, diskName is null");
      }
      String diskNamePlace = null;
      if (NativeCodeLoader.isNativeCodeLoaded() && Shell.LINUX) {
        try {
          diskNamePlace = NativeIO.getDiskName(location.getUri().getPath());
          LOG.info("location is {}, disk name is {}", location.getUri().getPath(), diskNamePlace);
        } catch (IOException e) {
          LOG.error("Get disk name by NativeIO failed.", e);
          diskNamePlace = diskName.toString();
        } finally {
          this.diskName = diskNamePlace;
        }
      } else {
        this.diskName = diskName.toString();
      }
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof DiskLocation)) {
        return false;
      }
      DiskLocation o = (DiskLocation) other;
      return diskName.equals(o.diskName) && location.equals(o.location);
    }

    @Override
    public int hashCode() {
      return location.hashCode();
    }

    @Override
    public String toString() {
      return location.toString() + " disk: " + diskName;
    }
  }

  private static class IOStat {
    private long lastTotalTicks;
    private int util;
    IOStat(long lastTotalTicks) {
      this.lastTotalTicks = lastTotalTicks;
    }

    public int getUtil() {
      return util;
    }

    public void setUtil(int util) {
      if (util <= 100 && util >= 0) {
        this.util = util;
      } else if (util < 0) {
        this.util = 0;
      } else {
        this.util = 100;
      }
    }

    public long getLastTotalTicks() {
      return lastTotalTicks;
    }

    public void setLastTotalTicks(long lastTotalTicks) {
      this.lastTotalTicks = lastTotalTicks;
    }
  }

  DiskIOUtilManager(Configuration conf) {
    this.intervalMs = TimeUnit.SECONDS.toMillis(conf.getLong(
        DFS_DATANODE_DISK_STAT_INTERVAL_SECONDS_KEY,
        DFS_DATANODE_DISK_STAT_INTERVAL_SECONDS_DEFAULT));
    if (this.intervalMs < DFS_DATANODE_DISK_STAT_INTERVAL_SECONDS_DEFAULT) {
      this.intervalMs = 1;
    }
  }

  @Override
  public void run() {
    FsVolumeImpl.LOG.info(this + " starting disk util stat.");
    while (true) {
      try {
        if (shouldStop) {
          FsVolumeImpl.LOG.info(this + " stopping disk util stat.");
          break;
        }
        if (!Shell.LINUX) {
          FsVolumeImpl.LOG.debug("Not support disk util stat on this os release.");
          continue;
        }
        Map<String, IOStat> allIOStats = getDiskIoUtils();
        synchronized (this) {
          for (Map.Entry<DiskLocation, IOStat> entry : ioStats.entrySet()) {
            String disk = entry.getKey().diskName;
            IOStat oldStat = entry.getValue();
            int util = 0;
            if (allIOStats.containsKey(disk)) {
              long oldTotalTicks = oldStat.getLastTotalTicks();
              long newTotalTicks = allIOStats.get(disk).getLastTotalTicks();
              if (oldTotalTicks != 0) {
                util = (int) ((double) (newTotalTicks - oldTotalTicks) * 100 / intervalMs);
              }
              oldStat.setLastTotalTicks(newTotalTicks);
              oldStat.setUtil(util);
              LOG.debug(disk + " disk io util:" + util);
            } else {
              //Maybe this disk has been umounted.
              oldStat.setUtil(100);
            }
          }
        }
        try {
          Thread.sleep(intervalMs);
        } catch (InterruptedException e) {
          LOG.debug("Thread interrupted while sleep in DiskIOUtilManager", e);
        }
      } catch (Throwable e) {
        LOG.error("DiskIOUtilManager encountered an exception.", e);
      }
    }
  }

  void start() {
    if (diskStatThread != null) {
      return;
    }
    shouldStop = false;
    diskStatThread = new Thread(this, threadName());
    diskStatThread.setDaemon(true);
    diskStatThread.start();
  }

  void stop() {
    shouldStop = true;
    if (diskStatThread != null) {
      diskStatThread.interrupt();
      try {
        diskStatThread.join();
      } catch (InterruptedException e) {
        LOG.debug("Thread interrupted while stop DiskIOUtilManager", e);
      }
    }
  }

  private String threadName() {
    return "DataNode disk io util manager";
  }

  private static final String PROC_DISKSSTATS = "/proc/diskstats";
  private static final Pattern DISK_STAT_FORMAT =
      Pattern.compile("[ \t]*[0-9]*[ \t]*[0-9]*[ \t]*(\\S*)" +
          "[ \t]*[0-9]*[ \t]*[0-9]*[ \t]*[0-9]*" +
          "[ \t]*[0-9]*[ \t]*[0-9]*[ \t]*[0-9]*" +
          "[ \t]*[0-9]*[ \t]*[0-9]*[ \t]*[0-9]*" +
          "[ \t]*([0-9]*)[ \t].*");

  private Map<String, IOStat> getDiskIoUtils() {
    Map<String, IOStat> rets = new HashMap<>();
    try(InputStreamReader fReader = new InputStreamReader(
        new FileInputStream(PROC_DISKSSTATS), Charset.forName("UTF-8"));
        BufferedReader in = new BufferedReader(fReader)) {
      Matcher mat = null;
      String str = in.readLine();
      while (str != null) {
        mat = DISK_STAT_FORMAT.matcher(str);
        if (mat.find()) {
          String disk = mat.group(1);
          long totalTicks = Long.parseLong(mat.group(2));
          LOG.debug(str + " totalTicks:" + totalTicks);
          IOStat stat = new IOStat(totalTicks);
          rets.put(disk, stat);
        }
        str = in.readLine();
      }
    } catch (FileNotFoundException f) {
      // Shouldn't happen.
      return rets;
    } catch (IOException e) {
      LOG.warn("Get disk ioUtils failed.", e);
    }
    return rets;
  }

  public synchronized void setStorageLocations(List<StorageLocation> locations) throws IOException {
    if (locations == null) {
      return;
    }
    locationToDisks.clear();
    ioStats.clear();
    for (StorageLocation location : locations) {
      DiskLocation diskLocation = new DiskLocation(location);
      locationToDisks.put(location, diskLocation);
      IOStat stat = new IOStat(0);
      ioStats.put(diskLocation, stat);
    }
  }

  public synchronized int getStorageLocationDiskIOUtil(StorageLocation location) {
    DiskLocation diskLocation = locationToDisks.get(location);
    if (diskLocation == null) {
      return 0;
    }
    if (ioStats.containsKey(diskLocation)) {
      return ioStats.get(diskLocation).getUtil();
    } else {
      return 0;
    }
  }
}