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

/**
 * Representation of the status of a storage cluster:
 * <p>
 * <ul>
 * <li>regions: the total number of regions served by the cluster</li>
 * <li>requests: the total number of requests per second handled by the
 * cluster in the last reporting interval</li>
 * <li>averageLoad: the average load of the region servers in the cluster</li>
 * <li>liveNodes: detailed status of the live region servers</li>
 * <li>deadNodes: the names of region servers declared dead</li>
 * </ul>
 */
@XmlRootElement(name="ClusterStatus")
public class StorageClusterStatusModel 
    implements Serializable, IProtobufWrapper {
	private static final long serialVersionUID = 1L;

	/**
	 * Represents a region server.
	 */
	public static class Node {
	  
	  /**
	   * Represents a region hosted on a region server.
	   */
	  public static class Region {
	    private byte[] name;

	    /**
	     * Default constructor
	     */
	    public Region() {}

	    /**
	     * Constructor
	     * @param name the region name
	     */
	    public Region(byte[] name) {
	      this.name = name;
	    }

	    /**
	     * @return the region name
	     */
	    @XmlAttribute
	    public byte[] getName() {
	      return name;
	    }

	    /**
	     * @param name the region name
	     */
	    public void setName(byte[] name) {
	      this.name = name;
	    }
	  }

	  private String name;
    private long startCode;
    private int requests;
    private List<Region> regions = new ArrayList<Region>();

    /**
     * Add a region name to the list
     * @param name the region name
     */
    public void addRegion(byte[] name) {
      regions.add(new Region(name));
    }

    /**
     * @param index the index
     * @return the region name
     */
    public Region getRegion(int index) {
      return regions.get(index);
    }

    /**
     * Default constructor
     */
    public Node() {}

    /**
     * Constructor
     * @param name the region server name
     * @param startCode the region server's start code
     */
    public Node(String name, long startCode) {
      this.name = name;
      this.startCode = startCode;
    }

    /**
     * @return the region server's name
     */
    @XmlAttribute
    public String getName() {
      return name;
    }

    /**
     * @return the region server's start code
     */
    @XmlAttribute
    public long getStartCode() {
      return startCode;
    }

    /**
     * @return the list of regions served by the region server
     */
    @XmlElement(name="Region")
    public List<Region> getRegions() {
      return regions;
    }

    /**
     * @return the number of requests per second processed by the region server
     */
    @XmlAttribute
    public int getRequests() {
      return requests;
    }

    /**
     * @param name the region server's hostname
     */
    public void setName(String name) {
      this.name = name;
    }

    /**
     * @param startCode the region server's start code
     */
    public void setStartCode(long startCode) {
      this.startCode = startCode;
    }

    /**
     * @param regions a list of regions served by the region server
     */
    public void setRegions(List<Region> regions) {
      this.regions = regions;
    }

    /**
     * @param requests the number of requests per second processed by the
     * region server
     */
    public void setRequests(int requests) {
      this.requests = requests;
    }
	}

	private List<Node> liveNodes = new ArrayList<Node>();
	private List<String> deadNodes = new ArrayList<String>();
	private int regions;
	private int requests;
	private double averageLoad;

	/**
	 * Add a live node to the cluster representation.
	 * @param name the region server name
	 * @param startCode the region server's start code
	 */
	public Node addLiveNode(String name, long startCode) {
	  Node node = new Node(name, startCode);
	  liveNodes.add(node);
	  return node;
	}

	/**
	 * @param index the index
	 * @return the region server model
	 */
	public Node getLiveNode(int index) {
	  return liveNodes.get(index);
	}

	/**
	 * Add a dead node to the cluster representation.
	 * @param node the dead region server's name
	 */
	public void addDeadNode(String node) {
	  deadNodes.add(node);
	}
	
	/**
	 * @param index the index
	 * @return the dead region server's name
	 */
	public String getDeadNode(int index) {
	  return deadNodes.get(index);
	}

	/**
	 * Default constructor
	 */
	public StorageClusterStatusModel() {}

	/**
	 * @return the list of live nodes
	 */
	@XmlElement(name="Node")
	@XmlElementWrapper(name="LiveNodes")
	public List<Node> getLiveNodes() {
	  return liveNodes;
	}

	/**
	 * @return the list of dead nodes
	 */
  @XmlElement(name="Node")
  @XmlElementWrapper(name="DeadNodes")
  public List<String> getDeadNodes() {
    return deadNodes;
  }

  /**
   * @return the total number of regions served by the cluster
   */
  @XmlAttribute
  public int getRegions() {
    return regions;
  }

  /**
   * @return the total number of requests per second handled by the cluster in
   * the last reporting interval
   */
  @XmlAttribute
  public int getRequests() {
    return requests;
  }

  /**
   * @return the average load of the region servers in the cluster
   */
  @XmlAttribute
  public double getAverageLoad() {
    return averageLoad;
  }

  /**
   * @param nodes the list of live node models
   */
  public void setLiveNodes(List<Node> nodes) {
    this.liveNodes = nodes;
  }

  /**
   * @param nodes the list of dead node names
   */
  public void setDeadNodes(List<String> nodes) {
    this.deadNodes = nodes;
  }

  /**
   * @param regions the total number of regions served by the cluster
   */
  public void setRegions(int regions) {
    this.regions = regions;
  }

  /**
   * @param requests the total number of requests per second handled by the
   * cluster
   */
  public void setRequests(int requests) {
    this.requests = requests;
  }

  /**
   * @param averageLoad the average load of region servers in the cluster
   */
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
