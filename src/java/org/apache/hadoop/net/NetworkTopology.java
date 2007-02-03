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
package org.apache.hadoop.net;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.dfs.DatanodeDescriptor;

/** The class represents a cluster of computer with a tree hierarchical
 * network topology.
 * For example, a cluster may be consists of many data centers filled 
 * with racks of computers.
 * In a network topology, leaves represent data nodes (computers) and inner
 * nodes represent switches/routers that manage traffic in/out of data centers
 * or racks.  
 * 
 * @author hairong
 *
 */
public class NetworkTopology {
    public final static String DEFAULT_RACK = "/default-rack";
    public static final Log LOG = 
        LogFactory.getLog("org.apache.hadoop.net.NetworkTopology");
    
    /* Inner Node represent a switch/router of a data center or rack.
     * Different from a leave node, it has non-null children.
     */
    private class InnerNode extends NodeBase {
        private HashMap<String, Node> children = 
            new HashMap<String, Node>(); // maps a name to a node 
        
        /** Construct an InnerNode from a path-like string */
        InnerNode( String path ) {
            super( path );
        }
        
        /** Construct an InnerNode from its name and its network location */
        InnerNode( String name, String location ) {
            super( name, location );
        }
        
        /** Get its children */
        HashMap<String, Node> getChildren() {return children;}
        
        /** Return the number of children this node has */
        int getNumOfChildren() {
            return children.size();
        }
        
        /** Judge if this node represents a rack 
         * Return true if it has no child or its children are not InnerNodes
         */ 
        boolean isRack() {
            if(children.isEmpty()) {
                return true;
            }
            
            Node firstChild = children.values().iterator().next();
            if(firstChild instanceof InnerNode) {
                return false;
            }
            
            return true;
        }
        
        /** Judge if this node is an ancestor of node <i>n</i>
         * 
         * @param n: a node
         * @return true if this node is an ancestor of <i>n</i>
         */
        boolean isAncestor(Node n) {
            return n.getNetworkLocation().startsWith(getPath());
        }
        
        /** Judge if this node is the parent of node <i>n</i>
         * 
         * @param n: a node
         * @return true if this node is the parent of <i>n</i>
         */
        boolean isParent( Node n ) {
            return n.getNetworkLocation().equals( getPath() );
        }
        
        /* Return a child name of this node who is an ancestor of node <i>n</i> */
        private String getNextAncestorName( Node n ) {
            if( !isAncestor(n)) {
                throw new IllegalArgumentException( 
                        this + "is not an ancestor of " + n);
            }
            String name = n.getNetworkLocation().substring(getPath().length());
            if(name.charAt(0) == PATH_SEPARATOR) {
                name = name.substring(1);
            }
            int index=name.indexOf(PATH_SEPARATOR);
            if( index !=-1 )
                name = name.substring(0, index);
            return name;
        }
        
        /** Add node <i>n</i> to the subtree of this node 
         * @param n node to be added
         * @return true if the node is added; false otherwise
         */
        boolean add( Node n ) {
            String parent = n.getNetworkLocation();
            String currentPath = getPath();
            if( !isAncestor( n ) )
                throw new IllegalArgumentException( n.getName()+", which is located at "
                        +parent+", is not a decendent of "+currentPath);
            if( isParent( n ) ) {
                // this node is the parent of n; add n directly
                return (null == children.put(n.getName(), n) );
            } else {
                // find the next ancestor node
                String parentName = getNextAncestorName( n );
                InnerNode parentNode = (InnerNode)children.get(parentName);
                if( parentNode == null ) {
                    // create a new InnerNode
                    parentNode = new InnerNode( parentName, currentPath );
                    children.put(parentName, parentNode);
                }
                // add n to the subtree of the next ancestor node
                return parentNode.add(n);
            }
        }
        
        /** Remove node <i>n</i> from the subtree of this node
         * @parameter n node to be deleted 
         * @return true if the node is deleted; false otherwise
         */
        boolean remove( Node n ) {
            String parent = n.getNetworkLocation();
            String currentPath = getPath();
            if(!isAncestor(n))
                throw new IllegalArgumentException( n.getName()+", which is located at "
                        +parent+", is not a decendent of "+currentPath);
            if( isParent(n) ) {
                // this node is the parent of n; remove n directly
                return (n == children.remove(n.getName()));
            } else {
                // find the next ancestor node: the parent node
                String parentName = getNextAncestorName( n );
                InnerNode parentNode = (InnerNode)children.get(parentName);
                if(parentNode==null) {
                    throw new IllegalArgumentException( n.getName()
                            + ", which is located at "
                            + parent+", is not a decendent of " + currentPath);
                }
                // remove n from the parent node
                boolean isRemoved = parentNode.remove( n );
                // if the parent node has no children, remove the parent node too
                if(parentNode.getNumOfChildren() == 0 ) {
                    children.remove(parentName);
                }
                return isRemoved;
            }
        } // end of remove
        
        /** Given a node's string representation, return a reference to the node */ 
        Node getLoc( String loc ) {
            if( loc == null || loc.length() == 0 ) return this;
            String[] path = loc.split(PATH_SEPARATOR_STR, 2);
            Node childnode = children.get( path[0] );
            if(childnode == null ) return null; // non-existing node
            if( path.length == 1 ) return childnode;
            if( childnode instanceof InnerNode ) {
                return ((InnerNode)childnode).getLoc(path[1]);
            } else {
                return null;
            }
        }
        
        /** Get all the data nodes belonged to the subtree of this node */
        void getLeaves( Collection<DatanodeDescriptor> results ) {
            for( Iterator<Node> iter = children.values().iterator();
            iter.hasNext(); ) {
                Node childNode = iter.next();
                if( childNode instanceof InnerNode ) {
                    ((InnerNode)childNode).getLeaves(results);
                } else {
                    results.add( (DatanodeDescriptor)childNode );
                }
            }
        }
    } // end of InnerNode
    
    InnerNode clusterMap = new InnerNode( InnerNode.ROOT ); //the root of the tree
    private int numOfLeaves = 0; // data nodes counter
    private int numOfRacks = 0;  // rack counter
    
    public NetworkTopology() {
    }
    
    /** Add a data node
     * Update data node counter & rack counter if neccessary
     * @param node
     *          data node to be added
     * @exception IllegalArgumentException if add a data node to an existing leave
     */
    public synchronized void add( DatanodeDescriptor node ) {
        if( node==null ) return;
        LOG.info("Adding a new node: "+node.getPath());
        Node rack = getNode(node.getNetworkLocation());
        if(rack != null && !(rack instanceof InnerNode) ) {
            throw new IllegalArgumentException( "Unexpected data node " 
                    + node.toString() 
                    + " at an illegal network location");
        }
        if( clusterMap.add( node) ) {
            numOfLeaves++;
            if( rack == null ) {
                numOfRacks++;
            }
        }
        LOG.debug("NetworkTopology became:\n" + this.toString());
    }
    
    /** Remove a data node
     * Update data node counter & rack counter if neccessary
     * @param node
     *          data node to be removed
     */ 
    public synchronized void remove( DatanodeDescriptor node ) {
        if( node==null ) return;
        LOG.info("Removing a node: "+node.getPath());
        if( clusterMap.remove( node ) ) {
            numOfLeaves--;
            InnerNode rack = (InnerNode)getNode(node.getNetworkLocation());
            if(rack == null) {
                numOfRacks--;
            }
        }
        LOG.debug("NetworkTopology became:\n" + this.toString());
    }
       
    /** Check if the tree contains data node <i>node</i>
     * 
     * @param node
     *          a data node
     * @return true if <i>node</i> is already in the tree; false otherwise
     */
    public boolean contains( DatanodeDescriptor node ) {
        if( node == null ) return false;
        Node rNode = getNode(node.getPath());
        return (rNode == node); 
    }
    
    /** Given a string representation of a node, return the reference to the node
     * 
     * @param loc
     *          a path-like string representation of a node
     * @return a reference to the node; null if the node is not in the tree
     */
    public synchronized Node getNode( String loc ) {
        loc = NodeBase.normalize(loc);
        if(!NodeBase.ROOT.equals(loc))
            loc = loc.substring(1);
        return clusterMap.getLoc( loc );
    }
    
    /* Add all the data nodes that belong to 
     * the subtree of the node <i>loc</i> to <i>results</i>*/
    private synchronized void getLeaves( String loc,
            Collection<DatanodeDescriptor> results ) {
        Node node = getNode(loc);
        if( node instanceof InnerNode )
            ((InnerNode)node).getLeaves(results);
        else {
            results.add((DatanodeDescriptor)node);
        }
    }
    
    /** Return all the data nodes that belong to the subtree of <i>loc</i>
     * @param loc
     *          a path-like string representation of a node
     * @return an array of data nodes that belong to the subtree of <i>loc</i>
     */
    public synchronized DatanodeDescriptor[] getLeaves( String loc ) {
        Collection<DatanodeDescriptor> results = new ArrayList<DatanodeDescriptor>();
        getLeaves(loc, results);
        return results.toArray(new DatanodeDescriptor[results.size()]);
    }
    
    /** Return all the data nodes that belong to the subtrees of <i>locs</i>
     * @param locs
     *          a collection of strings representing nodes
     * @return an array of data nodes that belong to subtrees of <i>locs</i>
     */
    public synchronized DatanodeDescriptor[] getLeaves(
            Collection<String> locs ) {
        Collection<DatanodeDescriptor> nodes = new ArrayList<DatanodeDescriptor>();
        if( locs != null ) { 
            Iterator<String> iter = locs.iterator();
            while(iter.hasNext()) {
                getLeaves( iter.next(), nodes );
            }
        }
        return nodes.toArray(new DatanodeDescriptor[nodes.size()]);
    }
    
    /** Return the total number of racks */
    public int getNumOfRacks( ) {
        return numOfRacks;
    }
    
    /** Return the total number of data nodes */
    public int getNumOfLeaves() {
        return numOfLeaves;
    }
    
    private void checkArgument( DatanodeDescriptor node ) {
        if( node == null ) {
            throw new IllegalArgumentException( 
                    "Unexpected null pointer argument" );
        }
        if( !contains(node) ) {
            String path = node.getPath();
            LOG.warn("The cluster does not contain data node: " + path);
            throw new IllegalArgumentException(
                    "Unexpected non-existing data node: " +path);
        }
    }
    
    /** Return the distance between two data nodes
     * It is assumed that the distance from one node to its parent is 1
     * The distance between two nodes is calculated by summing up their distances
     * to their closest common  ancestor.
     * @param node1 one data node
     * @param node2 another data node
     * @return the distance between node1 and node2
     * @exception IllegalArgumentException when either node1 or node2 is null, or
     * node1 or node2 do not belong to the cluster
     */
    public int getDistance(DatanodeDescriptor node1, DatanodeDescriptor node2 ) {
        checkArgument( node1 );
        checkArgument( node2 );
        /*
        if( !contains(node1) || !contains(node2) ) {
            return Integer.MAX_VALUE;
        }
        */
        if( node1 == node2 || node1.equals(node2)) {
            return 0;
        }
        String[] path1 = node1.getNetworkLocation().split("/");
        String[] path2 = node2.getNetworkLocation().split("/");
        
        int i;
        for(i=0; i<Math.min(path1.length, path2.length); i++) {
            if( path1[i]!=path2[i] && (path1[i]!=null 
                    && !path1[i].equals(path2[i]))) {
                break;
            }
        }
        return 2+(path1.length-i)+(path2.length-i);
    } 
    
    /** Check if two data nodes are on the same rack
     * @param node1 one data node
     * @param node2 another data node
     * @return true if node1 and node2 are pm the same rack; false otherwise
     * @exception IllegalArgumentException when either node1 or node2 is null, or
     * node1 or node2 do not belong to the cluster
     */
    public boolean isOnSameRack(
            DatanodeDescriptor node1, DatanodeDescriptor node2) {
        checkArgument( node1 );
        checkArgument( node2 );
        if( node1 == node2 || node1.equals(node2)) {
            return true;
        }
        
        String location1 = node1.getNetworkLocation();
        String location2 = node2.getNetworkLocation();
        
        if(location1 == location2 ) return true;
        if(location1 == null || location2 == null) return false;
        return location1.equals(location2);
    }
    
    /** convert a network tree to a string */
    public String toString() {
        // print the number of racks
        StringBuffer tree = new StringBuffer();
        tree.append( "Number of racks: " );
        tree.append( numOfRacks );
        tree.append( "\n" );
        // print the number of leaves
        tree.append( "Expected number of leaves:" );
        tree.append( numOfLeaves );
        tree.append( "\n" );
        // get all datanodes
        DatanodeDescriptor[] datanodes = getLeaves( NodeBase.ROOT );
        // print the number of leaves
        tree.append( "Actual number of leaves:" );
        tree.append( datanodes.length );
        tree.append( "\n" );
        // print datanodes
        for( int i=0; i<datanodes.length; i++ ) {
            tree.append( datanodes[i].getPath() );
            tree.append( "\n");
        }
        return tree.toString();
    }
}
