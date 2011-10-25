/**
 * Copyright 2011 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.zookeeper;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.hadoop.hbase.util.RetryCounterFactory;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

/**
 * A zookeeper that can handle 'recoverable' errors.
 * To handle recoverable errors, developers need to realize that there are two 
 * classes of requests: idempotent and non-idempotent requests. Read requests 
 * and unconditional sets and deletes are examples of idempotent requests, they 
 * can be reissued with the same results. 
 * (Although, the delete may throw a NoNodeException on reissue its effect on 
 * the ZooKeeper state is the same.) Non-idempotent requests need special 
 * handling, application and library writers need to keep in mind that they may 
 * need to encode information in the data or name of znodes to detect 
 * retries. A simple example is a create that uses a sequence flag. 
 * If a process issues a create("/x-", ..., SEQUENCE) and gets a connection 
 * loss exception, that process will reissue another 
 * create("/x-", ..., SEQUENCE) and get back x-111. When the process does a 
 * getChildren("/"), it sees x-1,x-30,x-109,x-110,x-111, now it could be 
 * that x-109 was the result of the previous create, so the process actually 
 * owns both x-109 and x-111. An easy way around this is to use "x-process id-" 
 * when doing the create. If the process is using an id of 352, before reissuing
 * the create it will do a getChildren("/") and see "x-222-1", "x-542-30", 
 * "x-352-109", x-333-110". The process will know that the original create 
 * succeeded an the znode it created is "x-352-109".
 * @see "http://wiki.apache.org/hadoop/ZooKeeper/ErrorHandling"
 */
public class RecoverableZooKeeper {
  private static final Log LOG = LogFactory.getLog(RecoverableZooKeeper.class);
  // the actual ZooKeeper client instance
  private ZooKeeper zk;
  private final RetryCounterFactory retryCounterFactory;
  // An identifier of this process in the cluster
  private final String identifier;
  private final byte[] id;
  private int retryIntervalMillis;

  private static final int ID_OFFSET =  Bytes.SIZEOF_INT;
  // the magic number is to be backward compatible
  private static final byte MAGIC =(byte) 0XFF;
  private static final int MAGIC_OFFSET = Bytes.SIZEOF_BYTE;

  public RecoverableZooKeeper(String quorumServers, int seesionTimeout,
      Watcher watcher, int maxRetries, int retryIntervalMillis) 
  throws IOException {
    this.zk = new ZooKeeper(quorumServers, seesionTimeout, watcher);
    this.retryCounterFactory =
      new RetryCounterFactory(maxRetries, retryIntervalMillis);
    this.retryIntervalMillis = retryIntervalMillis;

    // the identifier = processID@hostName
    this.identifier = ManagementFactory.getRuntimeMXBean().getName();
    LOG.info("The identifier of this process is " + identifier);
    this.id = Bytes.toBytes(identifier);
  }

  /**
   * delete is an idempotent operation. Retry before throw out exception.
   * This function will not throw out NoNodeException if the path is not existed
   * @param path
   * @param version
   * @throws InterruptedException
   * @throws KeeperException
   */
  public void delete(String path, int version)
  throws InterruptedException, KeeperException {
    RetryCounter retryCounter = retryCounterFactory.create();
    boolean isRetry = false; // False for first attempt, true for all retries.
    while (true) {
      try {
        zk.delete(path, version);
        return;
      } catch (KeeperException e) {
        switch (e.code()) {
          case NONODE:
            if (isRetry) {
              LOG.info("Node " + path + " already deleted. Assuming that a " +
                  "previous attempt succeeded.");
              return;
            }
            LOG.warn("Node " + path + " already deleted, and this is not a " +
                     "retry");
            throw e;

          case CONNECTIONLOSS:
          case OPERATIONTIMEOUT:
            LOG.warn("Possibly transient ZooKeeper exception: " + e);
            if (!retryCounter.shouldRetry()) {
              LOG.error("ZooKeeper delete failed after "
                + retryCounter.getMaxRetries() + " retries");
              throw e;
            }
            break;

          default:
            throw e;
        }
      }
      retryCounter.sleepUntilNextRetry();
      retryCounter.useRetry();
      isRetry = true;
    }
  }

  /**
   * exists is an idempotent operation. Retry before throw out exception
   * @param path
   * @param watcher
   * @return A Stat instance
   * @throws KeeperException
   * @throws InterruptedException
   */
  public Stat exists(String path, Watcher watcher)
  throws KeeperException, InterruptedException {
    RetryCounter retryCounter = retryCounterFactory.create();
    while (true) {
      try {
        return zk.exists(path, watcher);
      } catch (KeeperException e) {
        switch (e.code()) {
          case CONNECTIONLOSS:
          case OPERATIONTIMEOUT:
            LOG.warn("Possibly transient ZooKeeper exception: " + e);
            if (!retryCounter.shouldRetry()) {
              LOG.error("ZooKeeper exists failed after "
                + retryCounter.getMaxRetries() + " retries");
              throw e;
            }
            break;

          default:
            throw e;
        }
      }
      retryCounter.sleepUntilNextRetry();
      retryCounter.useRetry();
    }
  }

  /**
   * exists is an idempotent operation. Retry before throw out exception
   * @param path
   * @param watch
   * @return A Stat instance
   * @throws KeeperException
   * @throws InterruptedException
   */
  public Stat exists(String path, boolean watch)
  throws KeeperException, InterruptedException {
    RetryCounter retryCounter = retryCounterFactory.create();
    while (true) {
      try {
        return zk.exists(path, watch);
      } catch (KeeperException e) {
        switch (e.code()) {
          case CONNECTIONLOSS:
          case OPERATIONTIMEOUT:
            LOG.warn("Possibly transient ZooKeeper exception: " + e);
            if (!retryCounter.shouldRetry()) {
              LOG.error("ZooKeeper exists failed after "
                + retryCounter.getMaxRetries() + " retries");
              throw e;
            }
            break;

          default:
            throw e;
        }
      }
      retryCounter.sleepUntilNextRetry();
      retryCounter.useRetry();
    }
  }

  /**
   * getChildren is an idempotent operation. Retry before throw out exception
   * @param path
   * @param watcher
   * @return List of children znodes
   * @throws KeeperException
   * @throws InterruptedException
   */
  public List<String> getChildren(String path, Watcher watcher)
    throws KeeperException, InterruptedException {
    RetryCounter retryCounter = retryCounterFactory.create();
    while (true) {
      try {
        return zk.getChildren(path, watcher);
      } catch (KeeperException e) {
        switch (e.code()) {
          case CONNECTIONLOSS:
          case OPERATIONTIMEOUT:
            LOG.warn("Possibly transient ZooKeeper exception: " + e);
            if (!retryCounter.shouldRetry()) {
              LOG.error("ZooKeeper getChildren failed after "
                + retryCounter.getMaxRetries() + " retries");
              throw e;
            }
            break;

          default:
            throw e;
        }
      }
      retryCounter.sleepUntilNextRetry();
      retryCounter.useRetry();
    }
  }

  /**
   * getChildren is an idempotent operation. Retry before throw out exception
   * @param path
   * @param watch
   * @return List of children znodes
   * @throws KeeperException
   * @throws InterruptedException
   */
  public List<String> getChildren(String path, boolean watch)
  throws KeeperException, InterruptedException {
    RetryCounter retryCounter = retryCounterFactory.create();
    while (true) {
      try {
        return zk.getChildren(path, watch);
      } catch (KeeperException e) {
        switch (e.code()) {
          case CONNECTIONLOSS:
          case OPERATIONTIMEOUT:
            LOG.warn("Possibly transient ZooKeeper exception: " + e);
            if (!retryCounter.shouldRetry()) {
              LOG.error("ZooKeeper getChildren failed after "
                + retryCounter.getMaxRetries() + " retries");
              throw e;
            }
            break;

          default:
            throw e;
        }
      }
      retryCounter.sleepUntilNextRetry();
      retryCounter.useRetry();
    }
  }

  /**
   * getData is an idempotent operation. Retry before throw out exception
   * @param path
   * @param watcher
   * @param stat
   * @return Data
   * @throws KeeperException
   * @throws InterruptedException
   */
  public byte[] getData(String path, Watcher watcher, Stat stat)
  throws KeeperException, InterruptedException {
    RetryCounter retryCounter = retryCounterFactory.create();
    while (true) {
      try {
        byte[] revData = zk.getData(path, watcher, stat);       
        return this.removeMetaData(revData);
      } catch (KeeperException e) {
        switch (e.code()) {
          case CONNECTIONLOSS:
          case OPERATIONTIMEOUT:
            LOG.warn("Possibly transient ZooKeeper exception: " + e);
            if (!retryCounter.shouldRetry()) {
              LOG.error("ZooKeeper getData failed after "
                + retryCounter.getMaxRetries() + " retries");
              throw e;
            }
            break;

          default:
            throw e;
        }
      }
      retryCounter.sleepUntilNextRetry();
      retryCounter.useRetry();
    }
  }

  /**
   * getData is an idemnpotent operation. Retry before throw out exception
   * @param path
   * @param watch
   * @param stat
   * @return Data
   * @throws KeeperException
   * @throws InterruptedException
   */
  public byte[] getData(String path, boolean watch, Stat stat)
  throws KeeperException, InterruptedException {
    RetryCounter retryCounter = retryCounterFactory.create();
    while (true) {
      try {
        byte[] revData = zk.getData(path, watch, stat);
        return this.removeMetaData(revData);
      } catch (KeeperException e) {
        switch (e.code()) {
          case CONNECTIONLOSS:
          case OPERATIONTIMEOUT:
            LOG.warn("Possibly transient ZooKeeper exception: " + e);
            if (!retryCounter.shouldRetry()) {
              LOG.error("ZooKeeper getData failed after "
                + retryCounter.getMaxRetries() + " retries");
              throw e;
            }
            break;

          default:
            throw e;
        }
      }
      retryCounter.sleepUntilNextRetry();
      retryCounter.useRetry();
    }
  }

  /**
   * setData is NOT an idempotent operation. Retry may cause BadVersion Exception
   * Adding an identifier field into the data to check whether 
   * badversion is caused by the result of previous correctly setData
   * @param path
   * @param data
   * @param version
   * @return Stat instance
   * @throws KeeperException
   * @throws InterruptedException
   */
  public Stat setData(String path, byte[] data, int version)
  throws KeeperException, InterruptedException {
    RetryCounter retryCounter = retryCounterFactory.create();
    byte[] newData = appendMetaData(data);
    while (true) {
      try {
        return zk.setData(path, newData, version);
      } catch (KeeperException e) {
        switch (e.code()) {
          case CONNECTIONLOSS:
          case OPERATIONTIMEOUT:
            LOG.warn("Possibly transient ZooKeeper exception: " + e);
            if (!retryCounter.shouldRetry()) {
              LOG.error("ZooKeeper setData failed after "
                + retryCounter.getMaxRetries() + " retries");
              throw e;
            }
            break;
          case BADVERSION:
            // try to verify whether the previous setData success or not
            try{
              Stat stat = new Stat();
              byte[] revData = zk.getData(path, false, stat);
              int idLength = Bytes.toInt(revData, ID_OFFSET);
              int dataLength = revData.length-ID_OFFSET-idLength;
              int dataOffset = ID_OFFSET+idLength;
              
              if(Bytes.compareTo(revData, ID_OFFSET, id.length, 
                  revData, dataOffset, dataLength) == 0) {
                // the bad version is caused by previous successful setData
                return stat;
              }
            } catch(KeeperException keeperException){
              // the ZK is not reliable at this moment. just throw out exception
              throw keeperException;
            }            
          
          // throw out other exceptions and verified bad version exceptions
          default:
            throw e;
        }
      }
      retryCounter.sleepUntilNextRetry();
      retryCounter.useRetry();
    }
  }

  /**
   * <p>
   * NONSEQUENTIAL create is idempotent operation. 
   * Retry before throw out exceptions.
   * But this function will not throw out the NodeExist exception back to the
   * application.
   * </p>
   * <p>
   * But SEQUENTIAL is NOT idempotent operation. It is necessary to add 
   * identifier to the path to verify, whether the previous one is successful 
   * or not.
   * </p>
   * 
   * @param path
   * @param data
   * @param acl
   * @param createMode
   * @return Path
   * @throws KeeperException
   * @throws InterruptedException
   */
  public String create(String path, byte[] data, List<ACL> acl,
      CreateMode createMode)
  throws KeeperException, InterruptedException {
    byte[] newData = appendMetaData(data);
    switch (createMode) {
      case EPHEMERAL:
      case PERSISTENT:
        return createNonSequential(path, newData, acl, createMode);

      case EPHEMERAL_SEQUENTIAL:
      case PERSISTENT_SEQUENTIAL:
        return createSequential(path, newData, acl, createMode);

      default:
        throw new IllegalArgumentException("Unrecognized CreateMode: " + 
            createMode);
    }
  }

  private String createNonSequential(String path, byte[] data, List<ACL> acl, 
      CreateMode createMode) throws KeeperException, InterruptedException {
    RetryCounter retryCounter = retryCounterFactory.create();
    boolean isRetry = false; // False for first attempt, true for all retries.
    while (true) {
      try {
        return zk.create(path, data, acl, createMode);
      } catch (KeeperException e) {
        switch (e.code()) {
          case NODEEXISTS:
            if (isRetry) {
              // If the connection was lost, there is still a possibility that
              // we have successfully created the node at our previous attempt,
              // so we read the node and compare. 
              byte[] currentData = zk.getData(path, false, null);
              if (currentData != null &&
                  Bytes.compareTo(currentData, data) == 0) { 
                // We successfully created a non-sequential node
                return path;
              }
              LOG.error("Node " + path + " already exists with " + 
                  Bytes.toStringBinary(currentData) + ", could not write " +
                  Bytes.toStringBinary(data));
              throw e;
            }
            LOG.error("Node " + path + " already exists and this is not a " +
                "retry");
            throw e;

          case CONNECTIONLOSS:
          case OPERATIONTIMEOUT:
            LOG.warn("Possibly transient ZooKeeper exception: " + e);
            if (!retryCounter.shouldRetry()) {
              LOG.error("ZooKeeper create failed after "
                + retryCounter.getMaxRetries() + " retries");
              throw e;
            }
            break;

          default:
            throw e;
        }
      }
      retryCounter.sleepUntilNextRetry();
      retryCounter.useRetry();
      isRetry = true;
    }
  }
  
  private String createSequential(String path, byte[] data, 
      List<ACL> acl, CreateMode createMode)
  throws KeeperException, InterruptedException {
    RetryCounter retryCounter = retryCounterFactory.create();
    boolean first = true;
    String newPath = path+this.identifier;
    while (true) {
      try {
        if (!first) {
          // Check if we succeeded on a previous attempt
          String previousResult = findPreviousSequentialNode(newPath);
          if (previousResult != null) {
            return previousResult;
          }
        }
        first = false;
        return zk.create(newPath, data, acl, createMode);
      } catch (KeeperException e) {
        switch (e.code()) {
          case CONNECTIONLOSS:
          case OPERATIONTIMEOUT:
            LOG.warn("Possibly transient ZooKeeper exception: " + e);
            if (!retryCounter.shouldRetry()) {
              LOG.error("ZooKeeper create failed after "
                + retryCounter.getMaxRetries() + " retries");
              throw e;
            }
            break;

          default:
            throw e;
        }
      }
      retryCounter.sleepUntilNextRetry();
      retryCounter.useRetry();
    }
  }

  private String findPreviousSequentialNode(String path)
    throws KeeperException, InterruptedException {
    int lastSlashIdx = path.lastIndexOf('/');
    assert(lastSlashIdx != -1);
    String parent = path.substring(0, lastSlashIdx);
    String nodePrefix = path.substring(lastSlashIdx+1);

    List<String> nodes = zk.getChildren(parent, false);
    List<String> matching = filterByPrefix(nodes, nodePrefix);
    for (String node : matching) {
      String nodePath = parent + "/" + node;
      Stat stat = zk.exists(nodePath, false);
      if (stat != null) {
        return nodePath;
      }
    }
    return null;
  }
  
  public byte[] removeMetaData(byte[] data) {
    if(data == null || data.length == 0) {
      return data;
    }
    // check the magic data; to be backward compatible
    byte magic = data[0];
    if(magic != MAGIC) {
      return data;
    }
    
    int idLength = Bytes.toInt(data, MAGIC_OFFSET);
    int dataLength = data.length-MAGIC_OFFSET-ID_OFFSET-idLength;
    int dataOffset = MAGIC_OFFSET+ID_OFFSET+idLength;

    byte[] newData = new byte[dataLength];
    System.arraycopy(data, dataOffset, newData, 0, dataLength);
    
    return newData;
    
  }
  
  private byte[] appendMetaData(byte[] data) {
    if(data == null || data.length == 0){
      return data;
    }
    
    byte[] newData = new byte[MAGIC_OFFSET+ID_OFFSET+id.length+data.length];
    int pos = 0;
    pos = Bytes.putByte(newData, pos, MAGIC);
    pos = Bytes.putInt(newData, pos, id.length);
    pos = Bytes.putBytes(newData, pos, id, 0, id.length);
    pos = Bytes.putBytes(newData, pos, data, 0, data.length);

    return newData;
  }

  public long getSessionId() {
    return zk.getSessionId();
  }

  public void close() throws InterruptedException {
    zk.close();
  }

  public States getState() {
    return zk.getState();
  }

  public ZooKeeper getZooKeeper() {
    return zk;
  }

  public byte[] getSessionPasswd() {
    return zk.getSessionPasswd();
  }

  public void sync(String path, AsyncCallback.VoidCallback cb, Object ctx) {
    this.zk.sync(path, null, null);
  }

  /**
   * Filters the given node list by the given prefixes.
   * This method is all-inclusive--if any element in the node list starts
   * with any of the given prefixes, then it is included in the result.
   *
   * @param nodes the nodes to filter
   * @param prefixes the prefixes to include in the result
   * @return list of every element that starts with one of the prefixes
   */
  private static List<String> filterByPrefix(List<String> nodes, 
      String... prefixes) {
    List<String> lockChildren = new ArrayList<String>();
    for (String child : nodes){
      for (String prefix : prefixes){
        if (child.startsWith(prefix)){
          lockChildren.add(child);
          break;
        }
      }
    }
    return lockChildren;
  }
}
