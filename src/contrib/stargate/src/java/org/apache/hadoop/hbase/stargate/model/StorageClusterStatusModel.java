/*
 * Copyright 2009 The Apache Software Foundation
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

package org.apache.hadoop.hbase.stargate.model;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.hbase.stargate.protobuf.generated.StorageClusterStatusMessage.StorageClusterStatus;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.ByteString;

@XmlRootElement(name="ClusterStatus")
public class StorageClusterStatusModel 
    implements Serializable, IProtobufWrapper {
	private static final long serialVersionUID = 1L;

	public static class Node {
	  
	  public static class Region {
	    private byte[] name;

	    public Region() {}

	    public Region(byte[] name) {
	      this.name = name;
	    }

	    @XmlAttribute
	    public byte[] getName() {
	      return name;
	    }

	    public void setName(byte[] name) {
	      this.name = name;
	    }
	  }

	  private String name;
    private long startCode;
    private int requests;
    private List<Region> regions = new ArrayList<Region>();

    public void addRegion(byte[] name) {
      regions.add(new Region(name));
    }

    public Region getRegion(int i) {
      return regions.get(i);
    }

    public Node() {}

    public Node(String name, long startCode) {
      this.name = name;
      this.startCode = startCode;
    }

    @XmlAttribute
    public String getName() {
      return name;
    }

    @XmlAttribute
    public long getStartCode() {
      return startCode;
    }

    @XmlElement(name="Region")
    public List<Region> getRegions() {
      return regions;
    }

    @XmlAttribute
    public int getRequests() {
      return requests;
    }

    public void setName(String name) {
      this.name = name;
    }

    public void setStartCode(long startCode) {
      this.startCode = startCode;
    }

    public void setRegions(List<Region> regions) {
      this.regions = regions;
    }

    public void setRequests(int requests) {
      this.requests = requests;
    }
	}

	private List<Node> liveNodes = new ArrayList<Node>();
	private List<String> deadNodes = new ArrayList<String>();
	private int regions;
	private int requests;
	private double averageLoad;

	public Node addLiveNode(String name, long startCode) {
	  Node node = new Node(name, startCode);
	  liveNodes.add(node);
	  return node;
	}

	public Node getLiveNode(int i) {
	  return liveNodes.get(i);
	}

	public void addDeadNode(String node) {
	  deadNodes.add(node);
	}
	
	public String getDeadNode(int i) {
	  return deadNodes.get(i);
	}

	public StorageClusterStatusModel() {}

	@XmlElement(name="Node")
	@XmlElementWrapper(name="LiveNodes")
	public List<Node> getLiveNodes() {
	  return liveNodes;
	}

  @XmlElement(name="Node")
  @XmlElementWrapper(name="DeadNodes")
  public List<String> getDeadNodes() {
    return deadNodes;
  }

  @XmlAttribute
  public int getRegions() {
    return regions;
  }
  
  @XmlAttribute
  public int getRequests() {
    return requests;
  }

  @XmlAttribute
  public double getAverageLoad() {
    return averageLoad;
  }

  public void setLiveNodes(List<Node> nodes) {
    this.liveNodes = nodes;
  }

  public void setDeadNodes(List<String> nodes) {
    this.deadNodes = nodes;
  }

  public void setRegions(int regions) {
    this.regions = regions;
  }
  
  public void setRequests(int requests) {
    this.requests = requests;
  }

  public void setAverageLoad(double averageLoad) {
    this.averageLoad = averageLoad;
  }

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
	  StringBuilder sb = new StringBuilder();
	  sb.append(String.format("%d live servers, %d dead servers, " + 
      "%.4f average load\n\n", liveNodes.size(), deadNodes.size(),
      averageLoad));
    if (!liveNodes.isEmpty()) {
      sb.append(liveNodes.size());
      sb.append(" live servers\n");
      for (Node node: liveNodes) {
        sb.append("    ");
        sb.append(node.name);
        sb.append(' ');
        sb.append(node.startCode);
        sb.append("\n        requests=");
        sb.append(node.requests);
        sb.append(", regions=");
        sb.append(node.regions.size());
        sb.append("\n\n");
        for (Node.Region region: node.regions) {
          sb.append("        ");
          sb.append(Bytes.toString(region.name));
          sb.append('\n');
        }
        sb.append('\n');
      }
    }
    if (!deadNodes.isEmpty()) {
      sb.append('\n');
      sb.append(deadNodes.size());
      sb.append(" dead servers\n");
      for (String node: deadNodes) {
        sb.append("    ");
        sb.append(node);
        sb.append('\n');
      }
    }
	  return sb.toString();
	}

  @Override
  public byte[] createProtobufOutput() {
    StorageClusterStatus.Builder builder = StorageClusterStatus.newBuilder();
    builder.setRegions(regions);
    builder.setRequests(requests);
    builder.setAverageLoad(averageLoad);
    for (Node node: liveNodes) {
      StorageClusterStatus.Node.Builder nodeBuilder = 
        StorageClusterStatus.Node.newBuilder();
      nodeBuilder.setName(node.name);
      nodeBuilder.setStartCode(node.startCode);
      nodeBuilder.setRequests(node.requests);
      for (Node.Region region: node.regions) {
        nodeBuilder.addRegions(ByteString.copyFrom(region.name));
      }
      builder.addLiveNodes(nodeBuilder);
    }
    for (String node: deadNodes) {
      builder.addDeadNodes(node);
    }
    return builder.build().toByteArray();
  }

  @Override
  public IProtobufWrapper getObjectFromMessage(byte[] message)
      throws IOException {
    StorageClusterStatus.Builder builder = StorageClusterStatus.newBuilder();
    builder.mergeFrom(message);
    if (builder.hasRegions()) {
      regions = builder.getRegions();
    }
    if (builder.hasRequests()) {
      requests = builder.getRequests();
    }
    if (builder.hasAverageLoad()) {
      averageLoad = builder.getAverageLoad();
    }
    for (StorageClusterStatus.Node node: builder.getLiveNodesList()) {
      long startCode = node.hasStartCode() ? node.getStartCode() : -1;
      StorageClusterStatusModel.Node nodeModel = 
        addLiveNode(node.getName(), startCode);
      int requests = node.hasRequests() ? node.getRequests() : 0;
      nodeModel.setRequests(requests);
      for (ByteString region: node.getRegionsList()) {
        nodeModel.addRegion(region.toByteArray());
      }
    }
    for (String node: builder.getDeadNodesList()) {
      addDeadNode(node);
    }
    return this;
  }
}
