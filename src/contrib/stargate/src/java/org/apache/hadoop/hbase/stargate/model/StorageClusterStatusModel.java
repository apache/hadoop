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

import org.apache.hadoop.hbase.stargate.ProtobufMessageHandler;
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
 * 
 * <pre>
 * &lt;complexType name="StorageClusterStatus"&gt;
 *   &lt;sequence&gt;
 *     &lt;element name="liveNode" type="tns:Node"
 *       maxOccurs="unbounded" minOccurs="0"&gt;
 *     &lt;/element&gt;
 *     &lt;element name="deadNode" type="string" maxOccurs="unbounded"
 *       minOccurs="0"&gt;
 *     &lt;/element&gt;
 *   &lt;/sequence&gt;
 *   &lt;attribute name="regions" type="int"&gt;&lt;/attribute&gt;
 *   &lt;attribute name="requests" type="int"&gt;&lt;/attribute&gt;
 *   &lt;attribute name="averageLoad" type="float"&gt;&lt;/attribute&gt;
 * &lt;/complexType&gt;
 *
 * &lt;complexType name="Node"&gt;
 *   &lt;sequence&gt;
 *     &lt;element name="region" type="tns:Region" 
 *       maxOccurs="unbounded" minOccurs="0"&gt;&lt;/element&gt;
 *   &lt;/sequence&gt;
 *   &lt;attribute name="name" type="string"&gt;&lt;/attribute&gt;
 *   &lt;attribute name="startCode" type="int"&gt;&lt;/attribute&gt;
 *   &lt;attribute name="requests" type="int"&gt;&lt;/attribute&gt;
 *   &lt;attribute name="heapSizeMB" type="int"&gt;&lt;/attribute&gt;
 *   &lt;attribute name="maxHeapSizeMB" type="int"&gt;&lt;/attribute&gt;
 * &lt;/complexType&gt;
 *
 * &lt;complexType name="Region"&gt;
 *   &lt;attribute name="name" type="base64Binary"&gt;&lt;/attribute&gt;
 *   &lt;attribute name="stores" type="int"&gt;&lt;/attribute&gt;
 *   &lt;attribute name="storefiles" type="int"&gt;&lt;/attribute&gt;
 *   &lt;attribute name="storefileSizeMB" type="int"&gt;&lt;/attribute&gt;
 *   &lt;attribute name="memstoreSizeMB" type="int"&gt;&lt;/attribute&gt;
 *   &lt;attribute name="storefileIndexSizeMB" type="int"&gt;&lt;/attribute&gt;
 * &lt;/complexType&gt;
 * </pre>
 */
@XmlRootElement(name="ClusterStatus")
public class StorageClusterStatusModel 
    implements Serializable, ProtobufMessageHandler {
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
	    private int stores;
	    private int storefiles;
	    private int storefileSizeMB;
	    private int memstoreSizeMB;
	    private int storefileIndexSizeMB;

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
	     * Constructor
	     * @param name the region name
	     * @param stores the number of stores
	     * @param storefiles the number of store files
	     * @param storefileSizeMB total size of store files, in MB
	     * @param memstoreSizeMB total size of memstore, in MB
	     * @param storefileIndexSizeMB, total size of store file indexes, in MB
	     */
	    public Region(byte[] name, int stores, int storefiles,
          int storefileSizeMB, int memstoreSizeMB, int storefileIndexSizeMB) {
        this.name = name;
        this.stores = stores;
        this.storefiles = storefiles;
        this.storefileSizeMB = storefileSizeMB;
        this.memstoreSizeMB = memstoreSizeMB;
        this.storefileIndexSizeMB = storefileIndexSizeMB;
      }

      /**
	     * @return the region name
	     */
	    @XmlAttribute
	    public byte[] getName() {
	      return name;
	    }

	    /**
	     * @return the number of stores 
	     */
	    @XmlAttribute
	    public int getStores() {
        return stores;
      }

      /**
       * @return the number of store files 
       */
      @XmlAttribute
      public int getStorefiles() {
        return storefiles;
      }

      /**
       * @return the total size of store files, in MB
       */
      @XmlAttribute
      public int getStorefileSizeMB() {
        return storefileSizeMB;
      }

      /**
       * @return memstore size, in MB
       */
      @XmlAttribute
      public int getMemstoreSizeMB() {
        return memstoreSizeMB;
      }

      /**
       * @return the total size of store file indexes, in MB
       */
      @XmlAttribute
      public int getStorefileIndexSizeMB() {
        return storefileIndexSizeMB;
      }

      /**
	     * @param name the region name
	     */
	    public void setName(byte[] name) {
	      this.name = name;
	    }

	    /**
	     * @param stores the number of stores
	     */
      public void setStores(int stores) {
        this.stores = stores;
      }

      /**
       * @param storefiles the number of store files
       */
      public void setStorefiles(int storefiles) {
        this.storefiles = storefiles;
      }

      /**
       * @param storefileSizeMB total size of store files, in MB
       */
      public void setStorefileSizeMB(int storefileSizeMB) {
        this.storefileSizeMB = storefileSizeMB;
      }

      /**
       * @param memstoreSizeMB memstore size, in MB
       */
      public void setMemstoreSizeMB(int memstoreSizeMB) {
        this.memstoreSizeMB = memstoreSizeMB;
      }

      /**
       * @param storefileIndexSizeMB total size of store file indexes, in MB
       */
      public void setStorefileIndexSizeMB(int storefileIndexSizeMB) {
        this.storefileIndexSizeMB = storefileIndexSizeMB;
      }
	  }

	  private String name;
    private long startCode;
    private int requests;
    private int heapSizeMB;
    private int maxHeapSizeMB;
    private List<Region> regions = new ArrayList<Region>();

    /**
     * Add a region name to the list
     * @param name the region name
     */
    public void addRegion(byte[] name, int stores, int storefiles,
        int storefileSizeMB, int memstoreSizeMB, int storefileIndexSizeMB) {
      regions.add(new Region(name, stores, storefiles, storefileSizeMB,
        memstoreSizeMB, storefileIndexSizeMB));
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
     * @return the current heap size, in MB
     */
    @XmlAttribute
    public int getHeapSizeMB() {
      return heapSizeMB;
    }

    /**
     * @return the maximum heap size, in MB
     */
    @XmlAttribute
    public int getMaxHeapSizeMB() {
      return maxHeapSizeMB;
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
     * @param heapSizeMB the current heap size, in MB
     */
    public void setHeapSizeMB(int heapSizeMB) {
      this.heapSizeMB = heapSizeMB;
    }

    /**
     * @param maxHeapSizeMB the maximum heap size, in MB
     */
    public void setMaxHeapSizeMB(int maxHeapSizeMB) {
      this.maxHeapSizeMB = maxHeapSizeMB;
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
	 * @param heapSizeMB the current heap size, in MB
	 * @param maxHeapSizeMB the maximum heap size, in MB
	 */
	public Node addLiveNode(String name, long startCode, int heapSizeMB,
	    int maxHeapSizeMB) {
	  Node node = new Node(name, startCode);
	  node.setHeapSizeMB(heapSizeMB);
	  node.setMaxHeapSizeMB(maxHeapSizeMB);
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
        sb.append("\n        heapSizeMB=");
        sb.append(node.heapSizeMB);
        sb.append("\n        maxHeapSizeMB=");
        sb.append(node.maxHeapSizeMB);
        sb.append("\n\n");
        for (Node.Region region: node.regions) {
          sb.append("        ");
          sb.append(Bytes.toString(region.name));
          sb.append("\n            stores=");
          sb.append(region.stores);
          sb.append("\n            storefiless=");
          sb.append(region.storefiles);
          sb.append("\n            storefileSizeMB=");
          sb.append(region.storefileSizeMB);
          sb.append("\n            memstoreSizeMB=");
          sb.append(region.memstoreSizeMB);
          sb.append("\n            storefileIndexSizeMB=");
          sb.append(region.storefileIndexSizeMB);
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
      nodeBuilder.setHeapSizeMB(node.heapSizeMB);
      nodeBuilder.setMaxHeapSizeMB(node.maxHeapSizeMB);
      for (Node.Region region: node.regions) {
        StorageClusterStatus.Region.Builder regionBuilder =
          StorageClusterStatus.Region.newBuilder();
        regionBuilder.setName(ByteString.copyFrom(region.name));
        regionBuilder.setStores(region.stores);
        regionBuilder.setStorefiles(region.storefiles);
        regionBuilder.setStorefileSizeMB(region.storefileSizeMB);
        regionBuilder.setMemstoreSizeMB(region.memstoreSizeMB);
        regionBuilder.setStorefileIndexSizeMB(region.storefileIndexSizeMB);
        nodeBuilder.addRegions(regionBuilder);
      }
      builder.addLiveNodes(nodeBuilder);
    }
    for (String node: deadNodes) {
      builder.addDeadNodes(node);
    }
    return builder.build().toByteArray();
  }

  @Override
  public ProtobufMessageHandler getObjectFromMessage(byte[] message)
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
        addLiveNode(node.getName(), startCode, node.getHeapSizeMB(),
          node.getMaxHeapSizeMB());
      int requests = node.hasRequests() ? node.getRequests() : 0;
      nodeModel.setRequests(requests);
      for (StorageClusterStatus.Region region: node.getRegionsList()) {
        nodeModel.addRegion(
          region.getName().toByteArray(),
          region.getStores(),
          region.getStorefiles(),
          region.getStorefileSizeMB(),
          region.getMemstoreSizeMB(),
          region.getStorefileIndexSizeMB());
      }
    }
    for (String node: builder.getDeadNodesList()) {
      addDeadNode(node);
    }
    return this;
  }
}
