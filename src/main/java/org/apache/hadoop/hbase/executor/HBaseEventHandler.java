/**
 * Copyright 2010 The Apache Software Foundation
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
package org.apache.hadoop.hbase.executor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.executor.HBaseExecutorService.HBaseExecutorServiceType;
import org.apache.hadoop.hbase.master.ServerManager;


/**
 * Abstract base class for all HBase event handlers. Subclasses should 
 * implement the process() method where the actual handling of the event 
 * happens.
 * 
 * HBaseEventType is a list of ALL events (which also corresponds to messages - 
 * either internal to one component or between components). The event type 
 * names specify the component from which the event originated, and the 
 * component which is supposed to handle it.
 * 
 * Listeners can listen to all the events by implementing the interface 
 * HBaseEventHandlerListener, and by registering themselves as a listener. They 
 * will be called back before and after the process of every event.
 * 
 * TODO: Rename HBaseEvent and HBaseEventType to EventHandler and EventType 
 * after ZK refactor as it currently would clash with EventType from ZK and 
 * make the code very confusing.
 */
public abstract class HBaseEventHandler implements Runnable
{
  private static final Log LOG = LogFactory.getLog(HBaseEventHandler.class);
  // type of event this object represents
  protected HBaseEventType eventType = HBaseEventType.NONE;
  // is this a region server or master?
  protected boolean isRegionServer;
  // name of the server - this is needed for naming executors in case of tests 
  // where region servers may be co-located.
  protected String serverName;
  // listeners that are called before and after an event is processed
  protected static List<HBaseEventHandlerListener> eventHandlerListeners = 
    Collections.synchronizedList(new ArrayList<HBaseEventHandlerListener>());  
  // static instances needed by the handlers
  protected static ServerManager serverManager;
  
  /**
   * Note that this has to be called first BEFORE the subclass constructors.
   * 
   * TODO: take out after refactor
   */
  public static void init(ServerManager serverManager) {
    HBaseEventHandler.serverManager = serverManager;
  }
  
  /**
   * This interface provides hooks to listen to various events received by the 
   * queue. A class implementing this can listen to the updates by calling 
   * registerListener and stop receiving updates by calling unregisterListener
   */
  public interface HBaseEventHandlerListener {
    /**
     * Called before any event is processed
     */
    public void beforeProcess(HBaseEventHandler event);
    /**
     * Called after any event is processed
     */
    public void afterProcess(HBaseEventHandler event);
  }

  /**
   * These are a list of HBase events that can be handled by the various
   * HBaseExecutorService's. All the events are serialized as byte values.
   */
  public enum HBaseEventType {
    NONE (-1),
    // Messages originating from RS (NOTE: there is NO direct communication from 
    // RS to Master). These are a result of RS updates into ZK.
    RS2ZK_REGION_CLOSING      (1),   // RS is in process of closing a region
    RS2ZK_REGION_CLOSED       (2),   // RS has finished closing a region
    RS2ZK_REGION_OPENING      (3),   // RS is in process of opening a region
    RS2ZK_REGION_OPENED       (4),   // RS has finished opening a region
    
    // Updates from master to ZK. This is done by the master and there is 
    // nothing to process by either Master or RS
    M2ZK_REGION_OFFLINE       (50);  // Master adds this region as offline in ZK
    
    private final byte value;
    
    /**
     * Called by the HMaster. Returns a name of the executor service given an 
     * event type. Every event type has en entry - if the event should not be 
     * handled just add the NONE executor.
     * @return name of the executor service
     */
    public HBaseExecutorServiceType getMasterExecutorForEvent() {
      HBaseExecutorServiceType executorServiceType = null;
      switch(this) {
      
      case RS2ZK_REGION_CLOSING:
      case RS2ZK_REGION_CLOSED:
        executorServiceType = HBaseExecutorServiceType.MASTER_CLOSEREGION;
        break;

      case RS2ZK_REGION_OPENING:
      case RS2ZK_REGION_OPENED:
        executorServiceType = HBaseExecutorServiceType.MASTER_CLOSEREGION;
        break;
        
      case M2ZK_REGION_OFFLINE:
        executorServiceType = HBaseExecutorServiceType.NONE;
        break;
        
      default:
        throw new RuntimeException("Unhandled event type in the master.");
      }
      
      return executorServiceType;
    }

    /**
     * Called by the RegionServer. Returns a name of the executor service given an 
     * event type. Every event type has en entry - if the event should not be 
     * handled just return a null executor name.
     * @return name of the event service
     */
    public static String getRSExecutorForEvent(String serverName) {
      throw new RuntimeException("Unsupported operation.");
    }
    
    /**
     * Start the executor service that handles the passed in event type. The 
     * server that starts these event executor services wants to handle these 
     * event types.
     */
    public void startMasterExecutorService(String serverName) {
      HBaseExecutorServiceType serviceType = getMasterExecutorForEvent();
      if(serviceType == HBaseExecutorServiceType.NONE) {
        throw new RuntimeException("Event type " + toString() + " not handled on master.");
      }
      serviceType.startExecutorService(serverName);
    }

    public static void startRSExecutorService() {
      
    }

    HBaseEventType(int intValue) {
      this.value = (byte)intValue;
    }
    
    public byte getByteValue() {
      return value;
    }

    public static HBaseEventType fromByte(byte value) {
      switch(value) {
        case  -1: return HBaseEventType.NONE;
        case  1 : return HBaseEventType.RS2ZK_REGION_CLOSING;
        case  2 : return HBaseEventType.RS2ZK_REGION_CLOSED;
        case  3 : return HBaseEventType.RS2ZK_REGION_OPENING;
        case  4 : return HBaseEventType.RS2ZK_REGION_OPENED;
        case  50: return HBaseEventType.M2ZK_REGION_OFFLINE;

        default:
          throw new RuntimeException("Invalid byte value for conversion to HBaseEventType");
      }
    }
  }
  
  /**
   * Default base class constructor.
   * 
   * TODO: isRegionServer and serverName will go away once we do the HMaster 
   * refactor. We will end up passing a ServerStatus which should tell us both 
   * the name and if it is a RS or master.
   */
  public HBaseEventHandler(boolean isRegionServer, String serverName, HBaseEventType eventType) {
    this.isRegionServer = isRegionServer;
    this.eventType = eventType;
    this.serverName = serverName;
  }
  
  /**
   * This is a wrapper around process, used to update listeners before and after 
   * events are processed. 
   */
  public void run() {
    // fire all beforeProcess listeners
    for(HBaseEventHandlerListener listener : eventHandlerListeners) {
      listener.beforeProcess(this);
    }
    
    // call the main process function
    process();

    // fire all afterProcess listeners
    for(HBaseEventHandlerListener listener : eventHandlerListeners) {
      LOG.debug("Firing " + listener.getClass().getName() + 
                ".afterProcess event listener for event " + eventType);
      listener.afterProcess(this);
    }
  }
  
  /**
   * This method is the main processing loop to be implemented by the various 
   * subclasses.
   */
  public abstract void process();
  
  /**
   * Subscribe to updates before and after processing events
   */
  public static void registerListener(HBaseEventHandlerListener listener) {
    eventHandlerListeners.add(listener);
  }
  
  /**
   * Stop receiving updates before and after processing events
   */
  public static void unregisterListener(HBaseEventHandlerListener listener) {
    eventHandlerListeners.remove(listener);
  }
  
  public boolean isRegionServer() {
    return isRegionServer;
  }

  /**
   * Return the name for this event type.
   * @return
   */
  public HBaseExecutorServiceType getEventHandlerName() {
    // TODO: check for isRegionServer here
    return eventType.getMasterExecutorForEvent();
  }
  
  /**
   * Return the event type
   * @return
   */
  public HBaseEventType getHBEvent() {
    return eventType;
  }

  /**
   * Submits this event object to the correct executor service. This is causes
   * this object to get executed by the correct ExecutorService.
   */
  public void submit() {
    HBaseExecutorServiceType serviceType = getEventHandlerName();
    if(serviceType == null) {
      throw new RuntimeException("Event " + eventType + " not handled on this server " + serverName);
    }
    serviceType.getExecutor(serverName).submit(this);
  }
  
  /**
   * Executes this event object in the caller's thread. This is a synchronous 
   * way of executing the event.
   */
  public void execute() {
    this.run();
  }
}
