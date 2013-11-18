package org.apache.jxtadoop.net;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.jxtadoop.hdfs.server.namenode.DatanodeDescriptor;
import org.apache.jxtadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.jxtadoop.io.Writable;
import org.apache.jxtadoop.io.WritableFactories;
import org.apache.jxtadoop.io.WritableFactory;

/**
 * This is a replacement for the NetworkTopology class.
 * This new class uses the concept of Broadcast Domain instead of Racks.
 * Nodes will be part of a broadcast domain : the goal is to ensure fast replication on the local subnet for at least 1 block
 * and then replication to default domain ("0") or other broadcast domains. 
 * 
 * @author Franck Besnard
 *
 */
public class Peer2peerTopology implements Writable {
	// The default rack id where all the datanodes will be registered
	public final static String DEFAULT_RACK = "/datanodes";
	public static final Log LOG = 
		    LogFactory.getLog(Peer2peerTopology.class);

	// The broadcast domain class where the Peer2PeerNodes will be registered
	protected class BroadcastDomain  {
		protected String broadcast; // the broadcast domain id
		private ArrayList<Node> children=new ArrayList<Node>(); // the list of nodes part of the broadcast domain
		
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
		Collection<Node> getChildren() {
			return children;
		}
		
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
		 * Return true if node id already in the domain
		 * @param n
		 * @return
		 */
		public boolean contains(String n) {
			String c;
			
			for(int i=0;i<children.size();i++) {
				c = children.get(i).getName();
				if (c.equals(n))
					return true;
			}
			return false;
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
	     * Return node based on id
	     * @param n
	     * @return
	     */
	    Peer2PeerNode getNode(String n) {
	    	for(int i=0;i<children.size();i++) {
	    		if (n.equals(children.get(i).getName())) {
	    			return (Peer2PeerNode)children.get(i);
	    		}
	    	}
	    	return null;
	    }
	    
	    /**
	     * Return the broadcast ID
	     * @return
	     */
	    public String getID() {
	    	return this.broadcast;
	    }
	}
	
	// List of all BD discovered by the Multicast Discovery mechanism.
	// The first entry is always the default ID ("0")
	private ArrayList<BroadcastDomain> broadcastMap =new ArrayList<BroadcastDomain>();
	// The default domain id (aka internet or standalone peers)
	private String defaultId = "0";
	// The default path to the default domain e.g. /detanodes/0
	private String defaultPath = Peer2peerTopology.DEFAULT_RACK+Peer2PeerNode.PATH_SEPARATOR_STR+defaultId;
	// The default BD where all datanodes are registered before being moved to other BD based on discovery
	private BroadcastDomain defaultDomain = new BroadcastDomain(defaultPath);
	// Lock used to update the topology
	protected ReadWriteLock netlock;
	// Number of datanodes in the Cloud
	protected int numOfNodes = 0;
	// Number of Broadcast Domains including the default one 
	protected int numOfBroadcast = 0;
	// FS name system
	private FSNamesystem namesystem = null;
	
	/**
	 * Creating the P2P topology and add the default domain
	 */
	public Peer2peerTopology() {
		netlock = new ReentrantReadWriteLock();
		broadcastMap.add(defaultDomain);
		this.numOfBroadcast=1;
	}
	
	/**
	 * Creating the P2P topology, save the FSNameSystem and add the default domain
	 */
	public Peer2peerTopology(FSNamesystem fsn) {
		this();
		this.namesystem = fsn;
	}
	
	/**
	 * Create a p2p node from a descriptor
	 * @param DatanodeDescriptor
	 * @return Peer2PeerNode
	 */
	private Peer2PeerNode createFromDatanodeDescriptor(DatanodeDescriptor ddn) {
		String path =  ddn.getNetworkLocation() + Peer2PeerNode.PATH_SEPARATOR_STR + ddn.getName();
		
		LOG.debug("Descriptor to path : " + path);
		
		if(path.split(Peer2PeerNode.PATH_SEPARATOR_STR).length == 1)
			path = defaultId + Peer2PeerNode.PATH_SEPARATOR_STR + path;
		
		if((path.split(Peer2PeerNode.PATH_SEPARATOR_STR).length == 3)) {
				if((path.startsWith(DEFAULT_RACK + Peer2PeerNode.PATH_SEPARATOR_STR))) {
					path = DEFAULT_RACK
							+ Peer2PeerNode.PATH_SEPARATOR_STR + defaultId + Peer2PeerNode.PATH_SEPARATOR_STR
							+ (path.split(Peer2PeerNode.PATH_SEPARATOR_STR))[2];
				}
		}
		
		if(!path.startsWith(DEFAULT_RACK + Peer2PeerNode.PATH_SEPARATOR_STR)) 
			path = DEFAULT_RACK + Peer2PeerNode.PATH_SEPARATOR_STR + path;		
		
		LOG.debug("Path is : " + path);
		
		return new Peer2PeerNode(path);
	}

	/** 
	 * Add a peer to peer node
	 * @param Node
	 */
	public void add(Node n) {
		// If this is a datanode descriptor, then create the associated Node
		if(n instanceof DatanodeDescriptor) {
			n = createFromDatanodeDescriptor((DatanodeDescriptor)n);			  
		}
		  
		Peer2PeerNode node = (Peer2PeerNode)n;
		BroadcastDomain bd = null;
		String domain;
		  
		if (node==null) return;
		if ((node.getName().length()==0) || (node.getNetworkLocation().length()==0)) return;
		  
		netlock.writeLock().lock();
		  
		try {
			LOG.debug("Adding a node with network location : "+node.getNetworkLocation());
			if(node.getNetworkLocation().equals(defaultId)) { // Node should be part of the default domain
				if(!broadcastMap.get(0).contains(node.getName())) { // Node not in the map; adding it to the default domain
					broadcastMap.get(0).add(node);
					numOfNodes++;
				}
			} else { // Start looking in the broadcast map for associated domain & if node is already included
				for(int i=0; i<broadcastMap.size();i++) {
					domain = broadcastMap.get(i).getID();
					if(domain.equals(node.getNetworkLocation())) {
						bd =  broadcastMap.get(i);
						broadcastMap.get(i).add(node);
						numOfNodes++;
					}
				}
				  
				if (bd == null) { // This means that no domain already exists; Creating a new one & add the node
					bd = new BroadcastDomain(node.getPath());
					bd.add(node);
					numOfNodes++;
					broadcastMap.add(bd);
					numOfBroadcast++;
				}
			}
		} finally {
			LOG.debug("Added 1 node to topology map; Domains : "+this.getNumOfRacks()+"; Nodes : "+this.getNumOfLeaves());
			netlock.writeLock().unlock();
		}
	  }
	  
	/** 
	 * Remove a peer to peer node
	 * @param Node 
	 */
	public void remove(Node n) {		  
		if(n instanceof DatanodeDescriptor) {
			n = createFromDatanodeDescriptor((DatanodeDescriptor)n);			  
		}
		  
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
				
				if(bd.size()==0 && !bd.getID().equals("0")) {
					broadcastMap.remove(bd);
					bd = null;
					numOfBroadcast--;
				}
			}
		} finally {
			LOG.debug("Removed 1 node to topology map; Domains : "+this.getNumOfRacks()+"; Nodes : "+this.getNumOfLeaves());
			netlock.writeLock().unlock();
		}
	}
	  
	/** 
	 * Check if the topology  contains the node
	 * @param Node
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
	 * Get domain id of the domain containing the node
	 * @param Node
	 * @return The domain name of the node
	 */
	public String getDomain(String node) {
		BroadcastDomain bd;
		  
		if (node==null) return "";
		  
		netlock.writeLock().lock();
		  
		try {
			for(int i=0; i<broadcastMap.size();i++) {
				bd = broadcastMap.get(i);
				if (bd.contains(node)) {
					return bd.getID();
				}
			}
			return "";
		} finally {
			netlock.writeLock().unlock();
		}
	}
	  
	/**
	 * Return node from peer2peer topology
	 * @param domain, node
	 * @return Node
	 */
	public Peer2PeerNode getNode(String domain, String node) {
		BroadcastDomain bd = null;  
			
		for(int i=0;i<=this.numOfBroadcast;i++) {
			if (domain.equals(broadcastMap.get(i).getID())) {
				bd = broadcastMap.get(i);
				break;
			}
		}
			
		if(bd != null) {
			return bd.getNode(node);
		}
			
		return null;
	}
	  
	/**
	 * Return the number of broadcast domains as the number of racks
	 * and add the default one
	 * @return number of broadcast domains
	 */
	public int getNumOfRacks() {
		return this.numOfBroadcast;
	}
	  
	/**
	 * Return the number of nodes as the number of leaves
	 * @return number of data nodes
	 */
	public int getNumOfLeaves() {
		return this.numOfNodes;
	}
	  
	/**
	 * Return the distance between 2 nodes
	 * @param node1, node2
	 * @return number of hops
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
	 * @param scope, excludedNodes
	 * @return
	 */
	public int countNumOfAvailableNodes(String scope, List<Node> excludedNodes) {
		return this.numOfNodes;
	}
	  
	/**
	 * Return true if both nodes are in the same broadcast domain
	 * @param node1, node2
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
	 * @param domain
	 * @return node
	 */
	public Node chooseRandom(String d) {
		BroadcastDomain bd;
		Node n = null;
		boolean exclude = false;
		boolean browse;
		  
		LOG.debug("Choosing in domain : "+d);
		
		// Remove trailing ~
		if (d.startsWith("~")) {
			exclude = true;
			d = d.substring(1);
		}
		
		// Set default rack id
		if(d.equals(DEFAULT_RACK) || d.equals(DEFAULT_RACK+Peer2PeerNode.PATH_SEPARATOR_STR)) {
			d = "0";
		}
		
		// Remove trailing rack
		if(d.startsWith(DEFAULT_RACK+Peer2PeerNode.PATH_SEPARATOR_STR)) {
			d = d.replaceAll(DEFAULT_RACK+Peer2PeerNode.PATH_SEPARATOR_STR, "");
		}
		
		LOG.debug("Searching a node in domain : "+d);
		
		netlock.writeLock().lock();  
		
		try {
			for(int i=0; i<broadcastMap.size();i++) {
				bd = broadcastMap.get(i);
				LOG.debug("Browsing domain : "+bd.getID());
				
				if (bd.getID().equals(d) && !exclude) {
					browse =true;
				} else if (!bd.getID().equals(d) && exclude) {
					browse = true;
				} else {
					browse = false;
				}
				
				if (browse) {
					Collection<Node> cn = bd.getChildren();
					LOG.debug("Number of children : "+cn.size());
					Iterator<Node> in = cn.iterator();
					
					int index = -1;
					if(cn.size() > 0) index = r.nextInt(cn.size());
					int j = 0;
					
					while (j <= index) {
						j++;
						n = in.next();
						LOG.debug("Browsed node : "+n.getName());
					}
				}
			}
			
			if (n != null) LOG.debug("Returning node : "+n.getName());
			
			if (namesystem != null && n != null)
				return namesystem.getHost2DataNodeMap().getDatanodeByName(n.getName());
			else
				return null;
		} finally {
			netlock.writeLock().unlock();
		}
	}
	  
	/**
	 * Do nothing for now
	 * @param reader, nodes
	 */
	public void pseudoSortByDistance( Node reader, Node[] nodes ) {
		// To be done
	}

	  
	/**
	 * Returns all the brodcast domains as a string array
	 * @return all domains
	 */
	public String[] getDomains() {
		String[] domains =new String[this.numOfBroadcast+1];
		  
		for(int i=0;i<=this.numOfBroadcast;i++) {
			domains[i] = broadcastMap.get(i).getID();
		}
		  
		return domains;
	}
	  
	  
	/**
	 * Return the network topology
	 * @return the topology description
	 */
	public String printNetworkTopology() {
		BroadcastDomain bd;
		Iterator<Node> in;
		Node n;
		String append;
		  
		netlock.writeLock().lock();
		try {
			append = "\nNumber of broadcast domains :  \t" + this.numOfBroadcast;
			append += "\nNumber of peer to peer datanodes : \t" + this.numOfNodes;
			append += "\n";
		  
			append += "\nList of broadcast domains : ";
			for(int i=0; i<broadcastMap.size();i++) {
				bd = broadcastMap.get(i);
				append += "\n\t" + bd.getID();
			}
			  
			for(int i=0; i<broadcastMap.size();i++) {
				bd = broadcastMap.get(i);
				append += "\n\t- Domain : " + bd.getID();
				in = bd.getChildren().iterator();

				while(in.hasNext()) {
					n = in.next();
					append += "\n\t\t . " + n.getName();
				}
			}
		} finally {
			netlock.writeLock().unlock();
		}
		  
		return append;
	  }
		  

	/////////////////////////////////////////////////
	// Writable
	/////////////////////////////////////////////////
	static {                                      // register a ctor
		WritableFactories.setFactory(
				Peer2peerTopology.class,
				new WritableFactory() {
					public Writable newInstance() { return new Peer2peerTopology(); }
				}
		);
	}


	@Override
	public void write(DataOutput out) throws IOException {
		BroadcastDomain bd;
		Iterator<Node> in;
		
		out.writeInt(this.numOfNodes);
		out.writeInt(this.numOfBroadcast);
		
		for(int i=0; i<broadcastMap.size();i++) {
			bd = broadcastMap.get(i);
			in = bd.getChildren().iterator();
			
			org.apache.jxtadoop.io.Text.writeString(out,bd.getID());
			out.writeInt(bd.getChildren().size());
			
			while(in.hasNext()) {
				org.apache.jxtadoop.io.Text.writeString(out,in.next().getName());
			}
		}
	}


	@Override
	public void readFields(DataInput in) throws IOException {
		String bname, nname, npath;
		int nsize, msize, bsize;
		
		netlock.writeLock().lock();
		try {
			nsize = in.readInt();
			msize = in.readInt();
			
			LOG.debug("Broadcast : "+msize+"; Nodes : "+this.numOfNodes);
			
			for(int i=0; i<msize;i++) {
				bname = org.apache.jxtadoop.io.Text.readString(in);
				bsize = in.readInt();
				
				for(int j=0; j<bsize; j++) {
					nname = org.apache.jxtadoop.io.Text.readString(in);
					npath = Peer2peerTopology.DEFAULT_RACK 
									+ Peer2PeerNode.PATH_SEPARATOR_STR
									+ bname 
									+ Peer2PeerNode.PATH_SEPARATOR_STR
									+ nname;
					this.add(new Peer2PeerNode(npath));
				}
			}
			this.numOfNodes = nsize;
			this.numOfBroadcast = msize;
		} finally {
			netlock.writeLock().unlock();
		}
	}
}
