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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.util.HostsFileReader;

/**
 * This class manages the include and exclude files for HDFS.
 * 
 * These files control which DataNodes the NameNode expects to see in the
 * cluster.  Loosely speaking, the include file, if it exists and is not
 * empty, is a list of everything we expect to see.  The exclude file is 
 * a list of everything we want to ignore if we do see it.
 *
 * Entries may or may not specify a port.  If they don't, we consider
 * them to apply to every DataNode on that host.  For example, putting 
 * 192.168.0.100 in the excludes file blacklists both 192.168.0.100:5000 and
 * 192.168.0.100:6000.  This case comes up in unit tests.
 *
 * When reading the hosts files, we try to find the IP address for each
 * entry.  This is important because it allows us to de-duplicate entries.
 * If the user specifies a node as foo.bar.com in the include file, but
 * 192.168.0.100 in the exclude file, we need to realize that these are 
 * the same node.  Resolving the IP address also allows us to give more
 * information back to getDatanodeListForReport, which makes the web UI 
 * look nicer (among other things.)  See HDFS-3934 for more details.
 *
 * DNS resolution can be slow.  For this reason, we ONLY do it when (re)reading
 * the hosts files.  In all other cases, we rely on the cached values either
 * in the DatanodeID objects, or in HostFileManager#Entry.
 * We also don't want to be holding locks when doing this.
 * See HDFS-3990 for more discussion of DNS overheads.
 * 
 * Not all entries in the hosts files will have an associated IP address. 
 * Some entries may be "registration names."  The "registration name" of 
 * a DataNode is either the actual hostname, or an arbitrary string configured
 * by dfs.datanode.hostname.  It's possible to add registration names to the
 * include or exclude files.  If we can't find an IP address associated with
 * a host file entry, we assume it's a registered hostname and act accordingly.
 * The "registration name" feature is a little odd and it may be removed in the
 * future (I hope?)
 */
public class HostFileManager {
  private static final Log LOG = LogFactory.getLog(HostFileManager.class);

  public static class Entry {
    /**
     * This what the user put on the line before the colon, or the whole line
     * if there is no colon.
     */
    private final String prefix;
    
    /**
     * This is the port which was specified after the colon.  It is 0 if no
     * port was given.
     */
    private final int port;

    /**
     * If we can resolve the IP address, this is it.  Otherwise, it is the 
     * empty string.
     */
    private final String ipAddress;

    /**
     * Parse a hosts file Entry.
     */
    static Entry parse(String fileName, String entry) throws IOException {
      final String prefix;
      final int port;
      String ipAddress = "";
      
      int idx = entry.indexOf(':');
      if (-1 == idx) {
        prefix = entry;
        port = 0;
      } else {
        prefix = entry.substring(0, idx);
        String portStr = entry.substring(idx + 1);
        try {
          port = Integer.valueOf(portStr);
        } catch (NumberFormatException e) {
          throw new IOException("unable to parse port number for " +
              "'" + entry + "'", e);
        }
      }
      try {
        // Let's see if we can resolve this prefix to an IP address.
        // This may fail; one example is with a registered hostname
        // which is not actually a real DNS name.
        InetAddress addr = InetAddress.getByName(prefix);
        ipAddress = addr.getHostAddress();
      } catch (UnknownHostException e) {
        LOG.info("When reading " + fileName + ", could not look up " +
            "IP address for " + prefix + ".  We will assume this is a " +
            "registration name.", e);
      }
      return new Entry(prefix, port, ipAddress);
    }

    public String getIdentifier() {
      return ipAddress.isEmpty() ? prefix : ipAddress;
    }

    public Entry(String prefix, int port, String ipAddress) {
      this.prefix = prefix;
      this.port = port;
      this.ipAddress = ipAddress;
    }

    public String getPrefix() {
      return prefix;
    }

    public int getPort() {
      return port;
    }

    public String getIpAddress() {
      return ipAddress;
    }

    public String toString() {
      StringBuilder bld = new StringBuilder();
      bld.append("Entry{").append(prefix).append(", port=").
          append(port).append(", ipAddress=").append(ipAddress).append("}");
      return bld.toString();
    }
  }

  public static class EntrySet implements Iterable<Entry> {
    /**
     * The index.  Each Entry appears in here exactly once.
     *
     * It may be indexed by one of:
     *     ipAddress:port
     *     ipAddress
     *     registeredHostname:port
     *     registeredHostname
     *     
     * The different indexing strategies reflect the fact that we may or may
     * not have a port or IP address for each entry.
     */
    final TreeMap<String, Entry> index = new TreeMap<String, Entry>();

    public boolean isEmpty() {
      return index.isEmpty();
    }

    public Entry find(DatanodeID datanodeID) {
      Entry entry;
      int xferPort = datanodeID.getXferPort();
      assert(xferPort > 0);
      String datanodeIpAddr = datanodeID.getIpAddr();
      if (datanodeIpAddr != null) {
        entry = index.get(datanodeIpAddr + ":" + xferPort);
        if (entry != null) {
          return entry;
        }
        entry = index.get(datanodeIpAddr);
        if (entry != null) {
          return entry;
        }
      }
      String registeredHostName = datanodeID.getHostName();
      if (registeredHostName != null) {
        entry = index.get(registeredHostName + ":" + xferPort);
        if (entry != null) {
          return entry;
        }
        entry = index.get(registeredHostName);
        if (entry != null) {
          return entry;
        }
      }
      return null;
    }

    public Entry find(Entry toFind) {
      int port = toFind.getPort();
      if (port != 0) {
        return index.get(toFind.getIdentifier() + ":" + port);
      } else {
        // An Entry with no port matches any entry with the same identifer.
        // In other words, we treat 0 as "any port."
        Map.Entry<String, Entry> ceil =
            index.ceilingEntry(toFind.getIdentifier());
        if ((ceil != null) &&
            (ceil.getValue().getIdentifier().equals(
                toFind.getIdentifier()))) {
          return ceil.getValue();
        }
        return null;
      }
    }

    public String toString() {
      StringBuilder bld = new StringBuilder();
      
      bld.append("HostSet(");
      for (Map.Entry<String, Entry> entry : index.entrySet()) {
        bld.append("\n\t");
        bld.append(entry.getKey()).append("->").
            append(entry.getValue().toString());
      }
      bld.append("\n)");
      return bld.toString();
    }

    @Override
    public Iterator<Entry> iterator() {
      return index.values().iterator();
    }
  }

  public static class MutableEntrySet extends EntrySet {
    public void add(DatanodeID datanodeID) {
      Entry entry = new Entry(datanodeID.getHostName(),
          datanodeID.getXferPort(), datanodeID.getIpAddr());
      index.put(datanodeID.getIpAddr() + ":" + datanodeID.getXferPort(),
          entry);
    }

    public void add(Entry entry) {
      int port = entry.getPort();
      if (port != 0) {
        index.put(entry.getIdentifier() + ":" + port, entry);
      } else {
        index.put(entry.getIdentifier(), entry);
      }
    }

    void readFile(String type, String filename) throws IOException {
      if (filename.isEmpty()) {
        return;
      }
      HashSet<String> entrySet = new HashSet<String>();
      HostsFileReader.readFileToSet(type, filename, entrySet);
      for (String str : entrySet) {
        Entry entry = Entry.parse(filename, str);
        add(entry);
      }
    }
  }

  private EntrySet includes = new EntrySet();
  private EntrySet excludes = new EntrySet();

  public HostFileManager() {
  }

  public void refresh(String includeFile, String excludeFile)
      throws IOException {
    MutableEntrySet newIncludes = new MutableEntrySet();
    IOException includeException = null;
    try {
      newIncludes.readFile("included", includeFile);
    } catch (IOException e) {
      includeException = e;
    }
    MutableEntrySet newExcludes = new MutableEntrySet();
    IOException excludeException = null;
    try {
      newExcludes.readFile("excluded", excludeFile);
    } catch (IOException e) {
      excludeException = e;
    }
    synchronized(this) {
      if (includeException == null) {
        includes = newIncludes;
      }
      if (excludeException == null) {
        excludes = newExcludes;
      }
    }
    if (includeException == null) {
      LOG.info("read includes:\n" + newIncludes);
    } else {
      LOG.error("failed to read include file '" + includeFile + "'. " +
          "Continuing to use previous include list.",
          includeException);
    }
    if (excludeException == null) {
      LOG.info("read excludes:\n" + newExcludes);
    } else {
      LOG.error("failed to read exclude file '" + excludeFile + "'." +
          "Continuing to use previous exclude list.",
          excludeException);
    }
    if (includeException != null) {
      throw new IOException("error reading hosts file " + includeFile,
          includeException);
    }
    if (excludeException != null) {
      throw new IOException("error reading exclude file " + excludeFile,
          excludeException);
    }
  }

  public synchronized boolean isIncluded(DatanodeID dn) {
    if (includes.isEmpty()) {
      // If the includes list is empty, act as if everything is in the
      // includes list.
      return true;
    } else {
      return includes.find(dn) != null;
    }
  }

  public synchronized boolean isExcluded(DatanodeID dn) {
    return excludes.find(dn) != null;
  }

  public synchronized boolean hasIncludes() {
    return !includes.isEmpty();
  }

  /**
   * @return          the includes as an immutable set.
   */
  public synchronized EntrySet getIncludes() {
    return includes;
  }

  /**
   * @return          the excludes as an immutable set.
   */
  public synchronized EntrySet getExcludes() {
    return excludes;
  }
}
