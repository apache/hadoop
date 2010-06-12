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
package org.apache.hadoop.hbase.master.handler;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HMsg;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.executor.RegionTransitionEventData;
import org.apache.hadoop.hbase.executor.HBaseEventHandler;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.util.Writables;

/**
 * This is the event handler for all events relating to opening regions on the
 * HMaster. This could be one of the following:
 *   - notification that a region server is "OPENING" a region
 *   - notification that a region server has "OPENED" a region
 * The following event types map to this handler:
 *   - RS_REGION_OPENING
 *   - RS_REGION_OPENED
 */
public class MasterOpenRegionHandler extends HBaseEventHandler {
  private static final Log LOG = LogFactory.getLog(MasterOpenRegionHandler.class);
  // other args passed in a byte array form
  protected byte[] serializedData;
  private String regionName;
  private RegionTransitionEventData hbEventData;

  public MasterOpenRegionHandler(HBaseEventType eventType, String regionName, byte[] serData) {
    super(false, HMaster.MASTER, eventType);
    this.regionName = regionName;
    this.serializedData = serData;
  }

  /**
   * Handle the various events relating to opening regions. We can get the 
   * following events here:
   *   - RS_REGION_OPENING : Keep track to see how long the region open takes. 
   *                         If the RS is taking too long, then revert the 
   *                         region back to closed state so that it can be 
   *                         re-assigned.
   *   - RS_REGION_OPENED  : The region is opened. Add an entry into META for  
   *                         the RS having opened this region. Then delete this 
   *                         entry in ZK.
   */
  @Override
  public void process()
  {
    LOG.debug("Event = " + getHBEvent() + ", region = " + regionName);
    if(this.getHBEvent() == HBaseEventType.RS2ZK_REGION_OPENING) {
      handleRegionOpeningEvent();
    }
    else if(this.getHBEvent() == HBaseEventType.RS2ZK_REGION_OPENED) {
      handleRegionOpenedEvent();
    }
  }
  
  private void handleRegionOpeningEvent() {
    // TODO: not implemented. 
    LOG.debug("NO-OP call to handling region opening event");
    // Keep track to see how long the region open takes. If the RS is taking too 
    // long, then revert the region back to closed state so that it can be 
    // re-assigned.
  }

  private void handleRegionOpenedEvent() {
    try {
      if(hbEventData == null) {
        hbEventData = new RegionTransitionEventData();
        Writables.getWritable(serializedData, hbEventData);
      }
    } catch (IOException e) {
      LOG.error("Could not deserialize additional args for Open region", e);
    }
    LOG.debug("RS " + hbEventData.getRsName() + " has opened region " + regionName);
    HServerInfo serverInfo = serverManager.getServerInfo(hbEventData.getRsName());
    ArrayList<HMsg> returnMsgs = new ArrayList<HMsg>();
    serverManager.processRegionOpen(serverInfo, hbEventData.getHmsg().getRegionInfo(), returnMsgs);
    if(returnMsgs.size() > 0) {
      LOG.error("Open region tried to send message: " + returnMsgs.get(0).getType() + 
                " about " + returnMsgs.get(0).getRegionInfo().getRegionNameAsString());
    }
  }
}
