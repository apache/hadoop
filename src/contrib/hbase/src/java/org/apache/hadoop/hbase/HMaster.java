/**
 * Copyright 2006-7 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.ipc.*;

import java.io.*;
import java.util.*;

/*******************************************************************************
 * HMaster is the "master server" for a HBase.
 * There is only one HMaster for a single HBase deployment.
 ******************************************************************************/
public class HMaster extends HGlobals 
    implements HConstants, HMasterInterface, HMasterRegionInterface {

  private boolean closed;
  private Path dir;
  private Configuration conf;
  private FileSystem fs;
  private Random rand;
  private long maxRegionOpenTime;
  
  // The 'msgQueue' is used to assign work to the client processor thread
  
  private Vector<PendingOperation> msgQueue;
  
  private Leases serverLeases;
  private Server server;
  
  private HClient client;
 
  private long metaRescanInterval;
  
  private HServerAddress rootRegionLocation;
  
  //////////////////////////////////////////////////////////////////////////////
  // The ROOT/META scans, performed each time a meta region comes on-line
  // Since both have identical structure, we only need one class to get the work
  // done, however we have to create separate objects for each.
  //////////////////////////////////////////////////////////////////////////////

  private boolean rootScanned;
  private int numMetaRegions;
  
  /**
   * How do we know if all regions are assigned?
   * 
   * 1. After the initial scan of the root and meta regions, all regions known
   *    at that time will have been or are in the process of being assigned.
   * 
   * 2. When a region is split the region server notifies the master of the split
   *    and the new regions are assigned. But suppose the master loses the split
   *    message? We need to periodically rescan the root and meta regions.
   *    
   *    - If we rescan, any regions that are new but not assigned will have no
   *      server info. Any regions that are not being served by the same server
   *      will get re-assigned.
   *      
   *      - Thus a periodic rescan of the root region will find any new meta
   *        regions where we missed the meta split message or we failed to detect
   *        a server death and consequently need to assign the region to a new
   *        server.
   *        
   *      - if we keep track of all the known meta regions, then we can rescan
   *        them periodically. If we do this then we can detect an regions for
   *        which we missed a region split message.
   *        
   * Thus just keeping track of all the meta regions permits periodic rescanning
   * which will detect unassigned regions (new or otherwise) without the need to
   * keep track of every region.
   * 
   * So the root region scanner needs to wake up
   * 1. when the master receives notification that the root region has been opened.
   * 2. periodically after the first scan
   * 
   * The meta scanner needs to wake up:
   * 1. when a meta region comes on line
   * 2. periodically to rescan the known meta regions
   * 
   * A meta region is not 'known' until it has been scanned once.
   *        
   */
  private class RootScanner implements Runnable {
    public RootScanner() {
    }
    
    public void run() {
      Text cols[] = {
          ROOT_COLUMN_FAMILY
      };
      Text firstRow = new Text();
  
      while((! closed)) {
        int metaRegions = 0;
        while(rootRegionLocation == null) {
          try {
            rootRegionLocation.wait();
              
          } catch(InterruptedException e) {
          }
        }
        
        HRegionInterface server = null;
        HScannerInterface scanner = null;
        
        try {
          server = client.getHRegionConnection(rootRegionLocation);
          scanner = server.openScanner(rootRegionInfo.regionName, cols, firstRow);
          
        } catch(IOException iex) {
          try {
            close();
            
          } catch(IOException iex2) {
          }
          break;
        }
        try {
          HStoreKey key = new HStoreKey();
          TreeMap<Text, byte[]> results = new TreeMap<Text, byte[]>();
          DataInputBuffer inbuf = new DataInputBuffer();
  
          while(scanner.next(key, results)) {
            byte hRegionInfoBytes[] = results.get(ROOT_COL_REGIONINFO);
            inbuf.reset(hRegionInfoBytes, hRegionInfoBytes.length);
            HRegionInfo info = new HRegionInfo();
            info.readFields(inbuf);
                        
            byte serverBytes[] = results.get(ROOT_COL_SERVER);
            String serverName = new String(serverBytes, UTF8_ENCODING);
  
            byte startCodeBytes[] = results.get(ROOT_COL_STARTCODE);
            long startCode = Long.decode(new String(startCodeBytes, UTF8_ENCODING));
  
            // Note META region to load.
  
            HServerInfo storedInfo = null;
            synchronized(serversToServerInfo) {
              storedInfo = serversToServerInfo.get(serverName);
              if(storedInfo == null
                  || storedInfo.getStartCode() != startCode) {
              
                // The current assignment is no good; load the region.
  
                synchronized(unassignedRegions) {
                  unassignedRegions.put(info.regionName, info);
                  assignAttempts.put(info.regionName, 0L);
                }
              }
            }
            results.clear();
            metaRegions += 1;
          }
          
        } catch(Exception iex) {
        } finally {
          try {
            scanner.close();
            
          } catch(IOException iex2) {
          }
        }
        rootScanned = true;
        numMetaRegions = metaRegions;
        try {
          Thread.sleep(metaRescanInterval);
          
        } catch(InterruptedException e) {
        }
      }
    }
  }
  
  private RootScanner rootScanner;
  private Thread rootScannerThread;
  
  /** Contains information the meta scanner needs to process a "new" meta region */
  private class MetaRegion {
    public HServerAddress server;
    public Text regionName;
    public Text startKey;
  }
  
  /** Work for the meta scanner is queued up here */
  private Vector<MetaRegion> metaRegionsToScan;

  private TreeMap<Text, MetaRegion> knownMetaRegions;
  private Boolean allMetaRegionsScanned;
  
  /**
   * MetaScanner scans a region either in the META table.
   * 
   * When a meta server comes on line, a MetaRegion object is queued up by
   * regionServerReport() and this thread wakes up.
   *
   * It's important to do this work in a separate thread, or else the blocking 
   * action would prevent other work from getting done.
   */
  private class MetaScanner implements Runnable {
    private final Text cols[] = {
        META_COLUMN_FAMILY
    };
    private final Text firstRow = new Text();
    
    public MetaScanner() {
    }

    private void scanRegion(MetaRegion region) {
      HRegionInterface server = null;
      HScannerInterface scanner = null;
      
      try {
        server = client.getHRegionConnection(region.server);
        scanner = server.openScanner(region.regionName, cols, firstRow);
        
      } catch(IOException iex) {
        try {
          close();
          
        } catch(IOException iex2) {
        }
        return;
      }
      try {
        HStoreKey key = new HStoreKey();
        TreeMap<Text, byte[]> results = new TreeMap<Text, byte[]>();
        DataInputBuffer inbuf = new DataInputBuffer();

        while(scanner.next(key, results)) {
          byte hRegionInfoBytes[] = results.get(META_COL_REGIONINFO);
          inbuf.reset(hRegionInfoBytes, hRegionInfoBytes.length);
          HRegionInfo info = new HRegionInfo();
          info.readFields(inbuf);
                      
          byte serverBytes[] = results.get(META_COL_SERVER);
          String serverName = new String(serverBytes, UTF8_ENCODING);

          byte startCodeBytes[] = results.get(META_COL_STARTCODE);
          long startCode = Long.decode(new String(startCodeBytes, UTF8_ENCODING));

          // Note HRegion to load.

          HServerInfo storedInfo = null;
          synchronized(serversToServerInfo) {
            storedInfo = serversToServerInfo.get(serverName);
            if(storedInfo == null
                || storedInfo.getStartCode() != startCode) {
            
              // The current assignment is no good; load the region.

              synchronized(unassignedRegions) {
                unassignedRegions.put(info.regionName, info);
                assignAttempts.put(info.regionName, 0L);
              }
            }
          }
          results.clear();
        }
      } catch(Exception iex) {
      } finally {
        try {
          scanner.close();
          
        } catch(IOException iex2) {
        }
      }
    }

    public void run() {
      while((! closed)) {
        MetaRegion region = null;
        
        while(region == null) {
          synchronized(metaRegionsToScan) {
            if(metaRegionsToScan.size() != 0) {
              region = metaRegionsToScan.remove(0);
            }
          }
          if(region == null) {
            try {
              metaRegionsToScan.wait();
              
            } catch(InterruptedException e) {
            }
          }
        }
     
        scanRegion(region);
        
        synchronized(knownMetaRegions) {
          knownMetaRegions.put(region.startKey, region);
          if(rootScanned && knownMetaRegions.size() == numMetaRegions) {
            allMetaRegionsScanned = true;
            allMetaRegionsScanned.notifyAll();
          }
        }

        do {
          try {
            Thread.sleep(metaRescanInterval);
          
          } catch(InterruptedException ex) {
          }
          if(! allMetaRegionsScanned) {
            break;                              // A region must have split
          }
          
          // Rescan the known meta regions every so often
          
          Vector<MetaRegion> v = new Vector<MetaRegion>();
          v.addAll(knownMetaRegions.values());
          
          for(Iterator<MetaRegion> i = v.iterator(); i.hasNext(); ) {
            scanRegion(i.next());
          }
        } while(true);
      }
    }
  }

  private MetaScanner metaScanner;
  private Thread metaScannerThread;
  
  // Client processing
  
  private ClientProcessor clientProcessor;
  private Thread clientProcessorThread;

  // The 'unassignedRegions' table maps from a region name to a HRegionInfo record,
  // which includes the region's table, its id, and its start/end keys.
  //
  // We fill 'unassignedRecords' by scanning ROOT and META tables, learning the 
  // set of all known valid regions.

  private TreeMap<Text, HRegionInfo> unassignedRegions;

  // The 'assignAttempts' table maps from regions to a timestamp that indicates 
  // the last time we *tried* to assign the region to a RegionServer. If the 
  // timestamp is out of date, then we can try to reassign it.
  
  private TreeMap<Text, Long> assignAttempts;

  // 'killList' indicates regions that we hope to close and then never reopen 
  // (because we're merging them, say).

  private TreeMap<String, TreeMap<Text, HRegionInfo>> killList;

  // 'serversToServerInfo' maps from the String to its HServerInfo

  private TreeMap<String, HServerInfo> serversToServerInfo;

  /** Build the HMaster out of a raw configuration item. */
  public HMaster(Configuration conf) throws IOException {
    this(new Path(conf.get(HREGION_DIR, DEFAULT_HREGION_DIR)),
        new HServerAddress(conf.get(MASTER_DEFAULT_NAME)),
        conf);
  }

  /** 
   * Build the HMaster
   * @param dir         - base directory
   * @param address     - server address and port number
   * @param conf        - configuration
   * 
   * @throws IOException
   */
  public HMaster(Path dir, HServerAddress address, Configuration conf) throws IOException {
    this.closed = true;
    this.dir = dir;
    this.conf = conf;
    this.fs = FileSystem.get(conf);
    this.rand = new Random();

    // Make sure the root directory exists!
    
    if(! fs.exists(dir)) {
      fs.mkdirs(dir);
    }

    Path rootRegionDir = HStoreFile.getHRegionDir(dir, rootRegionInfo.regionName);
    if(! fs.exists(rootRegionDir)) {
      
      // Bootstrap! Need to create the root region and the first meta region.
      //TODO is the root region self referential?

      HRegion root = createNewHRegion(rootTableDesc, 0L);
      HRegion meta = createNewHRegion(metaTableDesc, 1L);
      
      addTableToMeta(root, meta);
    }

    this.maxRegionOpenTime = conf.getLong("hbase.hbasemaster.maxregionopen", 30 * 1000);
    this.msgQueue = new Vector<PendingOperation>();
    this.serverLeases = new Leases(conf.getLong("hbase.master.lease.period", 15 * 1000), 
        conf.getLong("hbase.master.lease.thread.wakefrequency", 15 * 1000));
    this.server = RPC.getServer(this, address.getBindAddress(),
        address.getPort(), conf.getInt("hbase.hregionserver.handler.count", 10), false, conf);
    this.client = new HClient(conf);
    
    this.metaRescanInterval
      = conf.getLong("hbase.master.meta.thread.rescanfrequency", 60 * 1000);

    // The root region
    
    this.rootRegionLocation = null;
    this.rootScanned = false;
    this.rootScanner = new RootScanner();
    this.rootScannerThread = new Thread(rootScanner, "HMaster.rootScanner");
    
    // Scans the meta table

    this.numMetaRegions = 0;
    this.metaRegionsToScan = new Vector<MetaRegion>();
    this.knownMetaRegions = new TreeMap<Text, MetaRegion>();
    this.allMetaRegionsScanned = new Boolean(false);
    this.metaScanner = new MetaScanner();
    this.metaScannerThread = new Thread(metaScanner, "HMaster.metaScanner");

    // Process updates to meta asychronously
    
    this.clientProcessor = new ClientProcessor();
    this.clientProcessorThread = new Thread(clientProcessor, "HMaster.clientProcessor");
    
    this.unassignedRegions = new TreeMap<Text, HRegionInfo>();
    this.unassignedRegions.put(rootRegionInfo.regionName, rootRegionInfo);
    
    this.assignAttempts = new TreeMap<Text, Long>();
    this.assignAttempts.put(rootRegionInfo.regionName, 0L);

    this.killList = new TreeMap<String, TreeMap<Text, HRegionInfo>>();
    this.serversToServerInfo = new TreeMap<String, HServerInfo>();
    
    // We're almost open for business
    
    this.closed = false;
    
    try { 
      // Start things up
    
      this.rootScannerThread.start();
      this.metaScannerThread.start();
      this.clientProcessorThread.start();

      // Start the server last so everything else is running before we start
      // receiving requests
    
      this.server.start();
      
    } catch(IOException e) {
      // Something happend during startup. Shut things down.
      
      this.closed = true;
      throw e;
    }
  }

  /** Turn off the HMaster.  Turn off all the threads, close files, etc. */
  public void close() throws IOException {
    closed = true;

    try {
      client.close();
      
    } catch(IOException iex) {
    }
    
    try {
      server.stop();
      server.join();
      
    } catch(InterruptedException iex) {
    }
    try {
      clientProcessorThread.join();
      
    } catch(Exception iex) {
    }
    try {
      metaScannerThread.join();
      
    } catch(Exception iex) {
    }
    try {
      rootScannerThread.join();
      
    } catch(Exception iex) {
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  // HMasterRegionInterface
  //////////////////////////////////////////////////////////////////////////////
  
  /** HRegionServers call this method upon startup. */
  public void regionServerStartup(HServerInfo serverInfo) throws IOException {
    String server = serverInfo.getServerAddress().toString();
    HServerInfo storedInfo = null;

    // If we get the startup message but there's an old server by that
    // name, then we can timeout the old one right away and register
    // the new one.
    
    synchronized(serversToServerInfo) {
      storedInfo = serversToServerInfo.get(server);
        
      if(storedInfo != null) {
        serversToServerInfo.remove(server);

        synchronized(msgQueue) {
          msgQueue.add(new PendingServerShutdown(storedInfo));
          msgQueue.notifyAll();
        }

      }

      // Either way, record the new server

      serversToServerInfo.put(server, serverInfo);


      Text serverLabel = new Text(server);        
      serverLeases.createLease(serverLabel, serverLabel, new ServerExpirer(server));
    }
  }

  /** HRegionServers call this method repeatedly. */
  public HMsg[] regionServerReport(HServerInfo serverInfo, HMsg msgs[]) throws IOException {
    String server = serverInfo.getServerAddress().toString();

    synchronized(serversToServerInfo) {
      HServerInfo storedInfo = serversToServerInfo.get(server);
      
      if(storedInfo == null) {
        
        // The HBaseMaster may have been restarted.
        // Tell the RegionServer to start over and call regionServerStartup()
        
        HMsg returnMsgs[] = new HMsg[1];
        returnMsgs[0] = new HMsg(HMsg.MSG_CALL_SERVER_STARTUP);
        return returnMsgs;
        
      } else if(storedInfo.getStartCode() != serverInfo.getStartCode()) {
        
        // This state is reachable if:
        //
        // 1) RegionServer A started
        // 2) RegionServer B started on the same machine, then 
        //    clobbered A in regionServerStartup.
        // 3) RegionServer A returns, expecting to work as usual.
        //
        // The answer is to ask A to shut down for good.
        
        HMsg returnMsgs[] = new HMsg[1];
        returnMsgs[0] = new HMsg(HMsg.MSG_REGIONSERVER_ALREADY_RUNNING);
        return returnMsgs;
        
      } else {
        
        // All's well.  Renew the server's lease.
        // This will always succeed; otherwise, the fetch of serversToServerInfo
        // would have failed above.
        
        Text serverLabel = new Text(server);
        serverLeases.renewLease(serverLabel, serverLabel);

        // Refresh the info object
        serversToServerInfo.put(server, serverInfo);

        // Next, process messages for this server
        return processMsgs(serverInfo, msgs);
      }
    }
  }

  /** Process all the incoming messages from a server that's contacted us. */
  HMsg[] processMsgs(HServerInfo info, HMsg incomingMsgs[]) throws IOException {
    Vector<HMsg> returnMsgs = new Vector<HMsg>();
    
    // Process the kill list
    
    TreeMap<Text, HRegionInfo> regionsToKill = killList.get(info.toString());
    if(regionsToKill != null) {
      for(Iterator<HRegionInfo> i = regionsToKill.values().iterator();
          i.hasNext(); ) {
        
        returnMsgs.add(new HMsg(HMsg.MSG_REGION_CLOSE_AND_DELETE, i.next()));
      }
    }

    // Get reports on what the RegionServer did.
    
    synchronized(unassignedRegions) {
      for(int i = 0; i < incomingMsgs.length; i++) {
        HRegionInfo region = incomingMsgs[i].getRegionInfo();
        
        switch(incomingMsgs[i].getMsg()) {

        case HMsg.MSG_REPORT_OPEN:
          HRegionInfo regionInfo = unassignedRegions.get(region.regionName);

          if(regionInfo == null) {

            // This Region should not have been opened.
            // Ask the server to shut it down, but don't report it as closed.  
            // Otherwise the HMaster will think the Region was closed on purpose, 
            // and then try to reopen it elsewhere; that's not what we want.

            returnMsgs.add(new HMsg(HMsg.MSG_REGION_CLOSE_WITHOUT_REPORT, region)); 

          } else {

            // Remove from unassigned list so we don't assign it to someone else

            unassignedRegions.remove(region.regionName);
            assignAttempts.remove(region.regionName);

            if(region.regionName.compareTo(rootRegionInfo.regionName) == 0) {

              // Store the Root Region location (in memory)

              rootRegionLocation = new HServerAddress(info.getServerAddress());
              
              // Wake up the root scanner
              
              rootRegionLocation.notifyAll();
              break;
              
            } else if(region.regionName.find(META_TABLE_NAME.toString()) == 0) {

              // It's a meta region. Put it on the queue to be scanned.
              
              MetaRegion r = new MetaRegion();
              r.server = info.getServerAddress();
              r.regionName = region.regionName;
              r.startKey = region.startKey;
              
              synchronized(metaRegionsToScan) {
                metaRegionsToScan.add(r);
                metaRegionsToScan.notifyAll();
              }
            }
            
            // Queue up an update to note the region location.

            synchronized(msgQueue) {
              msgQueue.add(new PendingOpenReport(info, region.regionName));
              msgQueue.notifyAll();
            }
          }
          break;

        case HMsg.MSG_REPORT_CLOSE:
          if(region.regionName.compareTo(rootRegionInfo.regionName) == 0) { // Root region
            rootRegionLocation = null;
            unassignedRegions.put(region.regionName, region);
            assignAttempts.put(region.regionName, 0L);

          } else {
            boolean reassignRegion = true;
            
            if(regionsToKill.containsKey(region.regionName)) {
              regionsToKill.remove(region.regionName);
              
              if(regionsToKill.size() > 0) {
                killList.put(info.toString(), regionsToKill);
                
              } else {
                killList.remove(info.toString());
              }
              reassignRegion = false;
            }
            
            synchronized(msgQueue) {
              msgQueue.add(new PendingCloseReport(region, reassignRegion));
              msgQueue.notifyAll();
            }

            // NOTE: we cannot put the region into unassignedRegions as that
            //       could create a race with the pending close if it gets 
            //       reassigned before the close is processed.

          }
          break;

        case HMsg.MSG_NEW_REGION:
          if(region.regionName.find(META_TABLE_NAME.toString()) == 0) {
            // A meta region has split.
            
            allMetaRegionsScanned = false;
          }
          synchronized(unassignedRegions) {
            unassignedRegions.put(region.regionName, region);
            assignAttempts.put(region.regionName, 0L);
          }
          break;
          
        default:
          throw new IOException("Impossible state during msg processing.  Instruction: "
              + incomingMsgs[i].getMsg());
        }
      }

      // Figure out what the RegionServer ought to do, and write back.

      if(unassignedRegions.size() > 0) {

        // Open new regions as necessary

        int targetForServer = (int) Math.ceil(unassignedRegions.size()
            / (1.0 * serversToServerInfo.size()));

        int counter = 0;
        long now = System.currentTimeMillis();

        for(Iterator<Text> it = unassignedRegions.keySet().iterator();
        it.hasNext(); ) {

          Text curRegionName = it.next();
          HRegionInfo regionInfo = unassignedRegions.get(curRegionName);
          long assignedTime = assignAttempts.get(curRegionName);

          if(now - assignedTime > maxRegionOpenTime) {
            returnMsgs.add(new HMsg(HMsg.MSG_REGION_OPEN, regionInfo));

            assignAttempts.put(curRegionName, now);
            counter++;
          }

          if(counter >= targetForServer) {
            break;
          }
        }
      }
    }
    return (HMsg[]) returnMsgs.toArray(new HMsg[returnMsgs.size()]);
  }

  //////////////////////////////////////////////////////////////////////////////
  // Some internal classes to manage msg-passing and client operations
  //////////////////////////////////////////////////////////////////////////////
  
  private class ClientProcessor implements Runnable {
    public ClientProcessor() {
    }
    
    public void run() {
      while(! closed) {
        PendingOperation op = null;
        
        synchronized(msgQueue) {
          while(msgQueue.size() == 0) {
            try {
              msgQueue.wait();
              
            } catch(InterruptedException iex) {
            }
          }
          op = msgQueue.elementAt(msgQueue.size()-1);
          msgQueue.removeElementAt(msgQueue.size()-1);
        }
        try {
          op.process();
          
        } catch(Exception ex) {
          synchronized(msgQueue) {
            msgQueue.insertElementAt(op, 0);
          }
        }
      }
    }
  }

  abstract class PendingOperation {
    protected final Text[] columns = {
        META_COLUMN_FAMILY
    };
    protected final Text startRow = new Text();
    protected long clientId;

    public PendingOperation() {
      this.clientId = rand.nextLong();
    }
    
    public abstract void process() throws IOException;
  }
  
  class PendingServerShutdown extends PendingOperation {
    String deadServer;
    long oldStartCode;
    
    public PendingServerShutdown(HServerInfo serverInfo) {
      super();
      this.deadServer = serverInfo.getServerAddress().toString();
      this.oldStartCode = serverInfo.getStartCode();
    }
    
    private void scanMetaRegion(HRegionInterface server, HScannerInterface scanner,
        Text regionName) throws IOException {

      Vector<HStoreKey> toDoList = new Vector<HStoreKey>();
      TreeMap<Text, HRegionInfo> regions = new TreeMap<Text, HRegionInfo>();

      DataInputBuffer inbuf = new DataInputBuffer();
      try {
        HStoreKey key = new HStoreKey();
        TreeMap<Text, byte[]> results = new TreeMap<Text, byte[]>();

        while(scanner.next(key, results)) {
          byte serverBytes[] = results.get(META_COL_SERVER);
          String serverName = new String(serverBytes, UTF8_ENCODING);

          if(deadServer.compareTo(serverName) != 0) {
            // This isn't the server you're looking for - move along
            continue;
          }

          byte startCodeBytes[] = results.get(META_COL_STARTCODE);
          long startCode = Long.decode(new String(startCodeBytes, UTF8_ENCODING));

          if(oldStartCode != startCode) {
            // Close but no cigar
            continue;
          }

          // Bingo! Found it.

          byte hRegionInfoBytes[] = results.get(META_COL_REGIONINFO);
          inbuf.reset(hRegionInfoBytes, hRegionInfoBytes.length);
          HRegionInfo info = new HRegionInfo();
          info.readFields(inbuf);

          // Add to our to do lists

          toDoList.add(key);
          regions.put(info.regionName, info);
        }

      } finally {
        scanner.close();
      }

      // Remove server from root/meta entries

      for(int i = 0; i < toDoList.size(); i++) {
        long lockid = server.startUpdate(regionName, clientId, toDoList.get(i).getRow());
        server.delete(regionName, clientId, lockid, META_COL_SERVER);
        server.delete(regionName, clientId, lockid, META_COL_STARTCODE);
        server.commit(regionName, clientId, lockid);
      }

      // Put all the regions we found on the unassigned region list

      for(Iterator<Map.Entry<Text, HRegionInfo>> i = regions.entrySet().iterator();
          i.hasNext(); ) {

        Map.Entry<Text, HRegionInfo> e = i.next();
        Text region = e.getKey();
        HRegionInfo regionInfo = e.getValue();

        synchronized(unassignedRegions) {
          unassignedRegions.put(region, regionInfo);
          assignAttempts.put(region, 0L);
        }
      }
    }
    
    public void process() throws IOException {

      // We can not scan every meta region if they have not already been assigned
      // and scanned.
      
      while(!allMetaRegionsScanned) {
        try {
          allMetaRegionsScanned.wait();
          
        } catch(InterruptedException e) {
        }
      }

      // First scan the ROOT region
      
      HRegionInterface server = client.getHRegionConnection(rootRegionLocation);
      HScannerInterface scanner = server.openScanner(rootRegionInfo.regionName,
          columns, startRow);
      
      scanMetaRegion(server, scanner, rootRegionInfo.regionName);
      for(Iterator<MetaRegion> i = knownMetaRegions.values().iterator();
          i.hasNext(); ) {
        
        MetaRegion r = i.next();

        server = client.getHRegionConnection(r.server);
        scanner = server.openScanner(r.regionName, columns, startRow);
        scanMetaRegion(server, scanner, r.regionName);
      }
    }
  }
  
  /** PendingCloseReport is a close message that is saved in a different thread. */
  class PendingCloseReport extends PendingOperation {
    HRegionInfo regionInfo;
    boolean reassignRegion;
    boolean rootRegion;
    
    public PendingCloseReport(HRegionInfo regionInfo, boolean reassignRegion) {
      super();

      this.regionInfo = regionInfo;
      this.reassignRegion = reassignRegion;

      // If the region closing down is a meta region then we need to update
      // the ROOT table
      
      if(this.regionInfo.regionName.find(metaTableDesc.getName().toString()) == 0) {
        this.rootRegion = true;
        
      } else {
        this.rootRegion = false;
      }
    }
    
    public void process() throws IOException {

      // We can not access any meta region if they have not already been assigned
      // and scanned.
      
      while(!allMetaRegionsScanned) {
        try {
          allMetaRegionsScanned.wait();
          
        } catch(InterruptedException e) {
        }
      }

      // Mark the Region as unavailable in the appropriate meta table

      Text metaRegionName;
      HRegionInterface server;
      if(rootRegion) {
        metaRegionName = rootRegionInfo.regionName;
        server = client.getHRegionConnection(rootRegionLocation);
        
      } else {
        Text metaStartRow = knownMetaRegions.headMap(regionInfo.regionName).lastKey();
        MetaRegion r = knownMetaRegions.get(metaStartRow);
        metaRegionName = r.regionName;
        server = client.getHRegionConnection(r.server);
      }
      long lockid = server.startUpdate(metaRegionName, clientId, regionInfo.regionName);
      server.delete(metaRegionName, clientId, lockid, META_COL_SERVER);
      server.delete(metaRegionName, clientId, lockid, META_COL_STARTCODE);
      server.commit(metaRegionName, clientId, lockid);
      
      if(reassignRegion) {
        synchronized(unassignedRegions) {
          unassignedRegions.put(regionInfo.regionName, regionInfo);
          assignAttempts.put(regionInfo.regionName, 0L);
        }
      }
    }
  }

  /** PendingOpenReport is an open message that is saved in a different thread. */
  class PendingOpenReport extends PendingOperation {
    boolean rootRegion;
    Text regionName;
    BytesWritable serverAddress;
    BytesWritable startCode;
    
    public PendingOpenReport(HServerInfo info, Text regionName) {
      if(regionName.find(metaTableDesc.getName().toString()) == 0) {
        
        // The region which just came on-line is a META region.
        // We need to look in the ROOT region for its information.
        
        this.rootRegion = true;
        
      } else {
        
        // Just an ordinary region. Look for it in the META table.
        
        this.rootRegion = false;
      }
      this.regionName = regionName;
      
      try {
        this.serverAddress = new BytesWritable(
            info.getServerAddress().toString().getBytes(UTF8_ENCODING));
        
        this.startCode = new BytesWritable(
            String.valueOf(info.getStartCode()).getBytes(UTF8_ENCODING));
        
      } catch(UnsupportedEncodingException e) {
      }

    }
    
    public void process() throws IOException {

      // We can not access any meta region if they have not already been assigned
      // and scanned.
      
      while(!allMetaRegionsScanned) {
        try {
          allMetaRegionsScanned.wait();
          
        } catch(InterruptedException e) {
        }
      }

      // Register the newly-available Region's location.

      Text metaRegionName;
      HRegionInterface server;
      if(rootRegion) {
        metaRegionName = rootRegionInfo.regionName;
        server = client.getHRegionConnection(rootRegionLocation);
        
      } else {
        Text metaStartRow = knownMetaRegions.headMap(regionName).lastKey();
        MetaRegion r = knownMetaRegions.get(metaStartRow);
        metaRegionName = r.regionName;
        server = client.getHRegionConnection(r.server);
      }
      long lockid = server.startUpdate(metaRegionName, clientId, regionName);
      server.put(metaRegionName, clientId, lockid, META_COL_SERVER, serverAddress);
      server.put(metaRegionName, clientId, lockid, META_COL_STARTCODE, startCode);
      server.commit(metaRegionName, clientId, lockid);
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  // HMasterInterface
  //////////////////////////////////////////////////////////////////////////////
  
  public void createTable(HTableDescriptor desc) throws IOException {
    HRegionInfo newRegion = new HRegionInfo(rand.nextLong(), desc, null, null);
    
    // We can not access any meta region if they have not already been assigned
    // and scanned.
    
    while(!allMetaRegionsScanned) {
      try {
        allMetaRegionsScanned.wait();
        
      } catch(InterruptedException e) {
      }
    }

    // 1. Check to see if table already exists

    Text metaStartRow = knownMetaRegions.headMap(newRegion.regionName).lastKey();
    MetaRegion m = knownMetaRegions.get(metaStartRow);
    Text metaRegionName = m.regionName;
    HRegionInterface server = client.getHRegionConnection(m.server);


    BytesWritable bytes = server.get(metaRegionName, desc.getName(), META_COL_REGIONINFO);
    if(bytes != null && bytes.getSize() != 0) {
      byte[] infoBytes = bytes.get();
      DataInputBuffer inbuf = new DataInputBuffer();
      inbuf.reset(infoBytes, infoBytes.length);
      HRegionInfo info = new HRegionInfo();
      info.readFields(inbuf);
      if(info.tableDesc.getName().compareTo(desc.getName()) == 0) {
        throw new IOException("table already exists");
      }
    }
    
    // 2. Create the HRegion
    
    HRegion r = createNewHRegion(desc, newRegion.regionId);
    
    // 3. Insert into meta
    
    HRegionInfo info = r.getRegionInfo();
    Text regionName = r.getRegionName();
    ByteArrayOutputStream byteValue = new ByteArrayOutputStream();
    DataOutputStream s = new DataOutputStream(byteValue);
    info.write(s);

    long clientId = rand.nextLong();
    long lockid = server.startUpdate(metaRegionName, clientId, regionName);
    server.put(metaRegionName, clientId, lockid, META_COL_REGIONINFO, 
        new BytesWritable(byteValue.toByteArray()));
    server.commit(metaRegionName, clientId, lockid);
    
    // 4. Get it assigned to a server
    
    synchronized(unassignedRegions) {
      unassignedRegions.put(regionName, info);
      assignAttempts.put(regionName, 0L);
    }
  }

  /**
   * Internal method to create a new HRegion. Used by createTable and by the
   * bootstrap code in the HMaster constructor
   * 
   * @param desc        - table descriptor
   * @param regionId    - region id
   * @return            - new HRegion
   * 
   * @throws IOException
   */
  private HRegion createNewHRegion(HTableDescriptor desc, long regionId) 
      throws IOException {
    
    HRegionInfo info = new HRegionInfo(regionId, desc, null, null);
    Path regionDir = HStoreFile.getHRegionDir(dir, info.regionName);
    fs.mkdirs(regionDir);

    return new HRegion(dir, new HLog(fs, new Path(regionDir, "log"), conf), fs,
        conf, info, null, null);
  }
  
  /**
   * Inserts a new table's meta information into the meta table. Used by
   * the HMaster bootstrap code.
   * 
   * @param meta                - HRegion to be updated
   * @param table               - HRegion of new table
   * 
   * @throws IOException
   */
  private void addTableToMeta(HRegion meta, HRegion table) throws IOException {
    
    // The row key is the region name
    
    long writeid = meta.startUpdate(table.getRegionName());
    
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    DataOutputStream s = new DataOutputStream(bytes);

    table.getRegionInfo().write(s);
    
    s.writeLong(table.getRegionId());
    meta.put(writeid, META_COL_REGIONINFO, bytes.toByteArray());
    
    bytes.reset();
    new HServerAddress().write(s);
    meta.put(writeid, META_COL_SERVER, bytes.toByteArray());
    
    bytes.reset();
    s.writeLong(0L);
    meta.put(writeid, META_COL_STARTCODE, bytes.toByteArray());
    
    meta.commit(writeid);
  }
  
  public void deleteTable(Text tableName) throws IOException {
    Text[] columns = {
        META_COLUMN_FAMILY
    };
    
    // We can not access any meta region if they have not already been assigned
    // and scanned.
    
    while(!allMetaRegionsScanned) {
      try {
        allMetaRegionsScanned.wait();
        
      } catch(InterruptedException e) {
      }
    }

    for(Iterator<MetaRegion> i = knownMetaRegions.tailMap(tableName).values().iterator();
        i.hasNext(); ) {

      // Find all the regions that make up this table
      
      long clientId = rand.nextLong();
      MetaRegion m = i.next();
      HRegionInterface server = client.getHRegionConnection(m.server);
      try {
        HScannerInterface scanner
          = server.openScanner(m.regionName, columns, tableName);
        
        HStoreKey key = new HStoreKey();
        TreeMap<Text, byte[]> results = new TreeMap<Text, byte[]>();
        DataInputBuffer inbuf = new DataInputBuffer();
        
        Vector<Text> rowsToDelete = new Vector<Text>();
        
        while(scanner.next(key, results)) {
          byte hRegionInfoBytes[] = results.get(META_COL_REGIONINFO);
          inbuf.reset(hRegionInfoBytes, hRegionInfoBytes.length);
          HRegionInfo info = new HRegionInfo();
          info.readFields(inbuf);

          if(info.tableDesc.getName().compareTo(tableName) > 0) {
            break;                      // Beyond any more entries for this table
          }

          // Is it being served?
          
          byte serverBytes[] = results.get(META_COL_SERVER);
          String serverName = new String(serverBytes, UTF8_ENCODING);

          byte startCodeBytes[] = results.get(META_COL_STARTCODE);
          long startCode = Long.decode(new String(startCodeBytes, UTF8_ENCODING));

          synchronized(serversToServerInfo) {
            HServerInfo s = serversToServerInfo.get(serverName);
            if(s != null && s.getStartCode() == startCode) {
              
              // It is being served. Tell the server to stop it and not report back
              
              TreeMap<Text, HRegionInfo> regionsToKill = killList.get(serverName);
              if(regionsToKill == null) {
                regionsToKill = new TreeMap<Text, HRegionInfo>();
              }
              regionsToKill.put(info.regionName, info);
              killList.put(serverName, regionsToKill);
            }
          }
        }
        for(Iterator<Text> row = rowsToDelete.iterator(); row.hasNext(); ) {
          long lockid = server.startUpdate(m.regionName, clientId, row.next());
          server.delete(m.regionName, clientId, lockid, columns[0]);
          server.commit(m.regionName, clientId, lockid);
        }
      } catch(IOException e) {
        e.printStackTrace();
      }
    }
  }
  
  public HServerAddress findRootRegion() {
    return rootRegionLocation;
  }

  //////////////////////////////////////////////////////////////////////////////
  // Managing leases
  //////////////////////////////////////////////////////////////////////////////
  
  class ServerExpirer extends LeaseListener {
    String server = null;
    
    public ServerExpirer(String server) {
      this.server = new String(server);
    }
    
    public void leaseExpired() {
      HServerInfo storedInfo = null;
      
      synchronized(serversToServerInfo) {
        storedInfo = serversToServerInfo.remove(server);
      }
      synchronized(msgQueue) {
        msgQueue.add(new PendingServerShutdown(storedInfo));
        msgQueue.notifyAll();
      }
    }
  }
}

