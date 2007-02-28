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
import java.util.List;
import java.util.Random;

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
        private ArrayList<Node> children=new ArrayList<Node>();
        private int numOfLeaves;
        
        /** Construct an InnerNode from a path-like string */
        InnerNode( String path ) {
            super( path );
        }
        
        /** Construct an InnerNode from its name and its network location */
        InnerNode( String name, String location ) {
            super( name, location );
        }
        
        /** Get its children */
        Collection<Node> getChildren() {return children;}
        
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
            
            Node firstChild = children.get(0);
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
            return getPath().equals(NodeBase.PATH_SEPARATOR_STR) ||
                   (n.getNetworkLocation()+NodeBase.PATH_SEPARATOR_STR).
                    startsWith(getPath()+NodeBase.PATH_SEPARATOR_STR);
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
        boolean add( DatanodeDescriptor n ) {
            String parent = n.getNetworkLocation();
            String currentPath = getPath();
            if( !isAncestor( n ) )
                throw new IllegalArgumentException( n.getName()+", which is located at "
                        +parent+", is not a decendent of "+currentPath);
            if( isParent( n ) ) {
                // this node is the parent of n; add n directly
                for(int i=0; i<children.size(); i++) {
                    if(children.get(i).getName().equals(n.getName())) {
                        children.set(i, n);
                        return false;
                    }
                }
                children.add(n);
                numOfLeaves++;
                return true;
            } else {
                // find the next ancestor node
                String parentName = getNextAncestorName( n );
                InnerNode parentNode = null;
                for(int i=0; i<children.size(); i++) {
                    if(children.get(i).getName().equals(parentName)) {
                        parentNode = (InnerNode)children.get(i);
                    }
                }
                if( parentNode == null ) {
                    // create a new InnerNode
                    parentNode = new InnerNode( parentName, currentPath );
                    children.add(parentNode);
                }
                // add n to the subtree of the next ancestor node
                if( parentNode.add(n) ) {
                    numOfLeaves++;
                    return true;
                } else {
                    return false;
                }
            }
        }
        
        /** Remove node <i>n</i> from the subtree of this node
         * @parameter n node to be deleted 
         * @return true if the node is deleted; false otherwise
         */
        boolean remove( DatanodeDescriptor n ) {
            String parent = n.getNetworkLocation();
            String currentPath = getPath();
            if(!isAncestor(n))
                throw new IllegalArgumentException( n.getName()
                        +", which is located at "
                        +parent+", is not a decendent of "+currentPath);
            if( isParent(n) ) {
                // this node is the parent of n; remove n directly
                for(int i=0; i<children.size(); i++) {
                    if(children.get(i).getName().equals(n.getName())) {
                        children.remove(i);
                        numOfLeaves--;
                        return true;
                    }
                }
                return false;
            } else {
                // find the next ancestor node: the parent node
                String parentName = getNextAncestorName( n );
                InnerNode parentNode = null;
                int i;
                for(i=0; i<children.size(); i++) {
                    if(children.get(i).getName().equals(parentName)) {
                        parentNode = (InnerNode)children.get(i);
                        break;
                    }
                }
                if(parentNode==null) {
                    throw new IllegalArgumentException( n.getName()
                            + ", which is located at "
                            + parent+", is not a decendent of " + currentPath);
                }
                // remove n from the parent node
                boolean isRemoved = parentNode.remove( n );
                // if the parent node has no children, remove the parent node too
                if(isRemoved) {
                    if(parentNode.getNumOfChildren() == 0 ) {
                        children.remove(i);
                    }
                    numOfLeaves--;
                }
                return isRemoved;
            }
        } // end of remove
        
        /** Given a node's string representation, return a reference to the node */ 
        Node getLoc( String loc ) {
            if( loc == null || loc.length() == 0 ) return this;
            
            String[] path = loc.split(PATH_SEPARATOR_STR, 2);
            Node childnode = null;
            for(int i=0; i<children.size(); i++) {
                if(children.get(i).getName().equals(path[0])) {
                    childnode = children.get(i);
                }
            }
            if(childnode == null ) return null; // non-existing node
            if( path.length == 1 ) return childnode;
            if( childnode instanceof InnerNode ) {
                return ((InnerNode)childnode).getLoc(path[1]);
            } else {
                return null;
            }
        }
        
        /** get <i>leaveIndex</i> leaf of this subtree 
         * if it is not in the <i>excludedNode</i>*/
        private DatanodeDescriptor getLeaf(int leaveIndex, Node excludedNode) {
            int count=0;
            int numOfExcludedLeaves = 1;
            if( excludedNode instanceof InnerNode ) {
                numOfExcludedLeaves = ((InnerNode)excludedNode).getNumOfLeaves();
            }
            if( isRack() ) { // children are leaves
                // range check
                if(leaveIndex<0 || leaveIndex>=this.getNumOfChildren()) {
                    return null;
                }
                DatanodeDescriptor child =
                    (DatanodeDescriptor)children.get(leaveIndex);
                if(excludedNode == null || excludedNode != child) {
                    // child is not the excludedNode
                    return child;
                } else { // child is the excludedNode so return the next child
                    if(leaveIndex+1>=this.getNumOfChildren()) {
                        return null;
                    } else {
                        return (DatanodeDescriptor)children.get(leaveIndex+1);
                    }
                }
            } else {
                for( int i=0; i<children.size(); i++ ) {
                    InnerNode child = (InnerNode)children.get(i);
                    if(excludedNode == null || excludedNode != child) {
                        // not the excludedNode
                        int numOfLeaves = child.getNumOfLeaves();
                        if( excludedNode != null && child.isAncestor(excludedNode) ) {
                            numOfLeaves -= numOfExcludedLeaves;
                        }
                        if( count+numOfLeaves > leaveIndex ) {
                            // the leaf is in the child subtree
                            return child.getLeaf(leaveIndex-count, excludedNode);
                        } else {
                            // go to the next child
                            count = count+numOfLeaves;
                        }
                    } else { // it is the excluededNode
                        // skip it and set the excludedNode to be null
                        excludedNode = null;
                    }
                }
                return null;
            }
        }
        
        int getNumOfLeaves() {
            return numOfLeaves;
        }
    } // end of InnerNode
    
    InnerNode clusterMap = new InnerNode( InnerNode.ROOT ); // the root
    private int numOfRacks = 0;  // rack counter
    
    public NetworkTopology() {
    }
    
    /** Add a data node
     * Update data node counter & rack counter if neccessary
     * @param node
     *          data node to be added
     * @exception IllegalArgumentException if add a data node to a leave
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
    
    /** Given a string representation of a node, return its reference
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
    
    /** Return the total number of racks */
    public synchronized int getNumOfRacks( ) {
        return numOfRacks;
    }
    
    /** Return the total number of data nodes */
    public synchronized int getNumOfLeaves() {
        return clusterMap.getNumOfLeaves();
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
    
    final private static Random r = new Random();
    /** randomly choose one node from <i>scope</i>
     * if scope starts with ~, choose one from the all datanodes except for the
     * ones in <i>scope</i>; otherwise, choose one from <i>scope</i>
     * @param scope range of datanodes from which a node will be choosen
     * @return the choosen data node
     */
    public DatanodeDescriptor chooseRandom(String scope) {
        if(scope.startsWith("~")) {
            return chooseRandom(NodeBase.ROOT, scope.substring(1));
        } else {
            return chooseRandom(scope, null);
        }
    }
    
    private DatanodeDescriptor chooseRandom(String scope, String excludedScope){
        if(excludedScope != null) {
            if(scope.startsWith(excludedScope)) {
                return null;
            }
            if(!excludedScope.startsWith(scope)) {
                excludedScope = null;
            }
        }
        Node node = getNode(scope);
        if(node instanceof DatanodeDescriptor) {
            return (DatanodeDescriptor)node;
        }
       InnerNode innerNode = (InnerNode)node;
       int numOfDatanodes = innerNode.getNumOfLeaves();
       if(excludedScope == null) {
           node = null;
       } else {
           node = getNode(excludedScope);
           if(node instanceof DatanodeDescriptor) {
               numOfDatanodes -= 1;
           } else {
               numOfDatanodes -= ((InnerNode)node).getNumOfLeaves();
           }
       }
       int leaveIndex = r.nextInt(numOfDatanodes);
       return innerNode.getLeaf(leaveIndex, node);
    }
       
    /** return the number of leaves in <i>scope</i> but not in <i>excludedNodes</i>
     * if scope starts with ~, return the number of datanodes that are not
     * in <i>scope</i> and <i>excludedNodes</i>; 
     * @param scope a path string that may start with ~
     * @param excludedNodes a list of data nodes
     * @return number of available data nodes
     */
    public int countNumOfAvailableNodes(String scope,
            List<DatanodeDescriptor> excludedNodes) {
        boolean isExcluded=false;
        if(scope.startsWith("~")) {
            isExcluded=true;
            scope=scope.substring(1);
        }
        scope = NodeBase.normalize(scope);
        int count=0; // the number of nodes in both scope & excludedNodes
        for( DatanodeDescriptor node:excludedNodes) {
            if( (node.getPath()+NodeBase.PATH_SEPARATOR_STR).
                    startsWith(scope+NodeBase.PATH_SEPARATOR_STR)) {
                count++;
            }
        }
        Node n=getNode(scope);
        int scopeNodeCount=1;
        if(n instanceof InnerNode) {
            scopeNodeCount=((InnerNode)n).getNumOfLeaves();
        }
        if(isExcluded) {
            return clusterMap.getNumOfLeaves()-
                scopeNodeCount-excludedNodes.size()+count;
        } else {
            return scopeNodeCount-count;
        }
    }
    
    /** convert a network tree to a string */
    public String toString() {
        // print the number of racks
        StringBuffer tree = new StringBuffer();
        tree.append( "Number of racks: " );
        tree.append( numOfRacks );
        tree.append( "\n" );
        // print the number of leaves
        int numOfLeaves = getNumOfLeaves();
        tree.append( "Expected number of leaves:" );
        tree.append( numOfLeaves );
        tree.append( "\n" );
        // print datanodes
        for( int i=0; i<numOfLeaves; i++ ) {
            tree.append( clusterMap.getLeaf(i, null).getPath() );
            tree.append( "\n");
        }
        return tree.toString();
    }
}
