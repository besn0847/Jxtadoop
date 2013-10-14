package org.apache.jxtadoop.net;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.jxtadoop.net.NetworkTopology.InnerNode;

public class Peer2peerTopology {
	public final static String DEFAULT_RACK = "/datanodes";
	public static final Log LOG = 
		    LogFactory.getLog(Peer2peerTopology.class);

	protected class BroadcastDomain  {
		protected String broadcast; 
		private ArrayList<Node> children=new ArrayList<Node>();
		
		/**
		 * Path can be the biradcast domain id (aka hash code) or the full path
		 * @param path
		 */
		BroadcastDomain(String path) {
		      if (path.matches("^[0-9]+$")) {
		    	  this.broadcast = path;
		      } else {
		    	  path = Peer2PeerNode.normalize(path);
		    	  String[] elements = path.split(Peer2PeerNode.PATH_SEPARATOR_STR); 
		    	  this.broadcast = elements[2];
		      }
		}
		
		/**
		 * Return all the nodes part of this broadcast domain 
		 * @return
		 */
		Collection<Node> getChildren() {return children;}
		
		/**
		 * Return number of nodes in the domain
		 * @return
		 */
		int size() {
			return children.size();
		}
		
		/**
		 * Return true if node already in the domain
		 * @param n
		 * @return
		 */
		public boolean contains(Node n) {
			return children.contains(n);
		}
		
		/**
		 * Return the size of the broadcast domain
		 * @return
		 */
	    int getNumOfChildren() {
	        return children.size();
	      }
	    
	    /**
	     * Check if the Node is part of the Broadcast domain
	     * @param Peer2PeerNode
	     * @return
	     */
	    boolean isBroadcast(Peer2PeerNode n) {
	    	return n.getPath().equals(Peer2peerTopology.DEFAULT_RACK+Peer2PeerNode.PATH_SEPARATOR_STR+this.broadcast);
	    }
	    
	    /**
	     * Add the node to the broadcast domain
	     * 
	     * @param peer2peer node
	     * @return successful ort unsucesfull
	     */
	    boolean add(Peer2PeerNode n) {
	    	if(n.getPath().equals(Peer2peerTopology.DEFAULT_RACK+Peer2PeerNode.PATH_SEPARATOR_STR+this.broadcast)) {
	    		children.add(n);
	    		return true;
	    	} else {
	    		return false;
	    	}
	    }
	    
	    /**
	     * Remove the node from the broadcast domain
	     * 
	     * @param n
	     * @return
	     */
	    boolean remove(Peer2PeerNode n) {
	    	return children.remove(n);
	    }
	
	    /**
	     * Return the broadcast ID
	     * @return
	     */
	    public String getID() {
	    	return this.broadcast;
	    }
	}
	private ArrayList<BroadcastDomain> broadcastMap =new ArrayList<BroadcastDomain>();
	private String defaultPath = Peer2peerTopology.DEFAULT_RACK+Peer2PeerNode.PATH_SEPARATOR_STR+"0";
	private BroadcastDomain defaultDomain = new BroadcastDomain(defaultPath);
	protected ReadWriteLock netlock;
	protected int numOfNodes = 0;
	protected int numOfBroadcast = 0;
	
	public Peer2peerTopology() {
		netlock = new ReentrantReadWriteLock();
		broadcastMap.add(defaultDomain);
	}

	  /** Add a peer to peer node
	   * 
	   * @param node
	   * @exception IllegalArgumentException 
	   */
	  public void add(Node n) {
		  Peer2PeerNode node = (Peer2PeerNode)n;
		  BroadcastDomain bd;
		  
		  if (node==null) return;
		  if ((node.getName().length()==0) || (node.getNetworkLocation().length()==0)) return;
		  
		  netlock.writeLock().lock();
		  
		  try {
				  if(node.getNetworkLocation().equals(defaultPath)) { // If the node has default path, then add it to the worldwide domain
					  if(!broadcastMap.get(0).contains(node)) {
						  broadcastMap.get(0).add(node);
						  numOfNodes++;
					  } else if (broadcastMap.size() == 1) { 
						  bd = new BroadcastDomain(node.getPath());
						  bd.add(node);
						  numOfNodes++;
						  broadcastMap.add(bd);
						  numOfBroadcast++;
					  }
		      } else {
				   if (broadcastMap.size() == 1) { 
					  bd = new BroadcastDomain(node.getPath());
					  bd.add(node);
					  numOfNodes++;
					  broadcastMap.add(bd);
					  numOfBroadcast++;
				  } else {  
					  	// Looking if node is already in a broadcast domain or if a broadcast domain already exist
					  for(int i=1; i<broadcastMap.size();i++) {
					  		bd = broadcastMap.get(i);
					  		// Check 1 : Node already in the broadcast domain
					  		if(bd.contains(node)) {
					  			return; // Found; do nothing
					  		} else if (bd.isBroadcast(node)) {
					  			bd.add(node); // Found adding the node to the domain
					  			numOfNodes++;
					  			return;
					  		} 
					  }
					  	
				  	 // If we reach here, then a new broadcast domain is required
				  	 bd = new BroadcastDomain(node.getPath());
				  	 bd.add(node);
				  	 numOfNodes++;
				  	 broadcastMap.add(bd);
				  	 numOfBroadcast++;
				  }
			  }
		  }  finally {
			  netlock.writeLock().unlock();
		  }
	  }
	  
	  /** Remove a peer to peer node
	   * 
	   * @param node
	   * @exception IllegalArgumentException 
	   */
	  public void remove(Node n) {
		  Peer2PeerNode node = (Peer2PeerNode)n;
		  BroadcastDomain bd;
		  
		  if (node==null) return;
		  if ((node.getName().length()==0) || (node.getNetworkLocation().length()==0)) return;
		  
		  netlock.writeLock().lock();
		  
		  try {
			  for(int i=0; i<broadcastMap.size();i++) {
				  bd = broadcastMap.get(i);
				  if(bd.contains(node)) {
					  bd.remove(node);
					  numOfNodes--;
				  }
				  if(bd.size()==0) {
					  broadcastMap.remove(bd);
					  bd =null;
					  numOfBroadcast--;
				  }
			  }
		  } finally {
			  netlock.writeLock().unlock();
		  }
	  }
	  
	  /** Check if the topology  contains the node
	   * 
	   * @param node
	   * @return true if node is in the topology; false otherwise
	   */
	  public boolean contains(Node node) {
		  BroadcastDomain bd;
		  
		  if (node==null) return false;
		  
		  netlock.writeLock().lock();
		  
		  try {
			  for(int i=0; i<broadcastMap.size();i++) {
				  bd = broadcastMap.get(i);
				  if (bd.contains(node)) {
					  return true;
				  }
			  }
			  return false;
		  } finally {
			  netlock.writeLock().unlock();
		  }
	  }
	  
	  /**
	   * Return the number of broadcast domains as the number of racks
	   * and add the default one
	   * 
	   * @return
	   */
	  public int getNumOfRacks() {
		  return this.numOfBroadcast+1;
	  }
	  
	  /**
	   * Return the number of nodes as the number of leaves
	   * 
	   * @return
	   */
	  public int getNumOfLeaves() {
		  return this.numOfNodes;
	  }
	  
	  /**
	   * Return the distance between 2 nodes
	   * 
	   * @param node1
	   * @param node2
	   * @return
	   */
	  public int getDistance(Node node1, Node node2) {
		  if(isOnSameRack(node1, node2)) {
			  return 1;
		  } else if ((broadcastMap.get(0).contains(node1)) || (broadcastMap.get(0).contains(node2))) {
			  return 2;
		  } else {
			  return 3;
		  }
	  }
	  
	  /**
	   * Simply returns the number of nodes in the peer-to-peer cloud
	   * 
	   * @param scope
	   * @param excludedNodes
	   * @return
	   */
	  public int countNumOfAvailableNodes(String scope, List<Node> excludedNodes) {
		  return this.numOfNodes;
	  }
	  
	  /**
	   * Return true if both nodes are in the same broadcast domain
	   * 
	   * @param n1
	   * @param n2
	   * @return
	   */
	  public boolean isOnSameRack(Node n1, Node n2) {
		  BroadcastDomain bd;
		  
		  if (n1==null || n2==null) return false;
		  if ((n1.getName().length()==0) || (n1.getNetworkLocation().length()==0)) return false;
		  if ((n2.getName().length()==0) || (n2.getNetworkLocation().length()==0)) return false;
		  
		  netlock.writeLock().lock();
		  
		  try {
			  for(int i=0; i<broadcastMap.size();i++) {
				  bd = broadcastMap.get(i);
				  if (bd.contains(n1) && bd.contains(n2)) {
					  return true;
				  }
			  }
			  return false;
		  } finally {
			  netlock.writeLock().unlock();
		  }
	  }
	  
	  final private static Random r = new Random();
	  
	  /**
	   * Choose a random node in the input domain
	   * 
	   * @param d
	   * @return
	   */
	  public Node chooseRandom(String d) {
		  BroadcastDomain bd;
		  
		  netlock.writeLock().lock();
		  
		  try {
			  for(int i=0; i<broadcastMap.size();i++) {
				  bd = broadcastMap.get(i);
				  
				  if (bd.getID().equals(d)) {
					  Collection<Node> cn = bd.getChildren();
					  Iterator<Node> in = cn.iterator();
					  
					  int index = r.nextInt(cn.size());
					  int j = 0;
					  while (j < index) {
						  j++;
						  in.next();
					  }
					  
					  return in.next();
				  }
			  }
		  } finally {
			  netlock.writeLock().unlock();
		  }
		  
		  return null;
	  }
	  
	  
	  /**
	   * Do nothing for now
	   * 
	   * @param reader
	   * @param nodes
	   */
	  public void pseudoSortByDistance( Node reader, Node[] nodes ) {
		  // To be done
	  }

	  /**
	   * Returns all the brodcast domains as a string array
	   * 
	   * @return
	   */
	  public String[] getDomains() {
		  String[] domains =new String[this.numOfBroadcast+1];
		  
		  for(int i=0;i<=this.numOfBroadcast;i++) {
			  domains[i] = broadcastMap.get(i).getID();
		  }
		  
		  return domains;
	  }
}
