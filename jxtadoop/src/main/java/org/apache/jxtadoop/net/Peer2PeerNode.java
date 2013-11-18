package org.apache.jxtadoop.net;

import org.apache.jxtadoop.hdfs.server.namenode.DatanodeDescriptor;

public class Peer2PeerNode implements Node {
	public final static char PATH_SEPARATOR = '/';
	public final static String PATH_SEPARATOR_STR = "/";
	public final static String ROOT = "";
	
	protected String peer; // urn
	protected String broadcast; // broadcast domain
	
	/**
	 * Create a P2P node without any name
	 */
	public Peer2PeerNode() {
	}
	  
	/**
	 * Create a P2P node from a path : 
	 * 		path -> /datanodes/18798732/59616261646162614E504720503250334BF8CE1FAFDDF001A3775FF8FB52662B03
	 * @param path
	 */
	public Peer2PeerNode(String path) {
		path = normalize(path);
		String[] elements = path.split("/");
	set(elements[2],elements[3]);
	}
	  
	/**
	 * Create P2P node from a name and a path
	 * 		name	-> 59616261646162614E504720503250334BF8CE1FAFDDF001A3775FF8FB52662B03
	 * 		path	->  /datanodes/18798732
	 * @param name
	 * @param location
	 */
	public Peer2PeerNode(String name, String location) {
		location = normalize(location);
		String path = location + PATH_SEPARATOR + name;
		path = normalize(path);
		String[] elements = path.split("/");
		set(elements[2],elements[3]);
	}
	  
	/**
	 * Set the peer id and the broadcast domain id (aka the hash code)
	 * @param domain id
	 * @param peer id
	 */
	private void set(String d, String p) {
		this.peer = p;
		this.broadcast = d;
	}
	  
	/**
	 * Return the broadcast domain
	 *  @return broadcastid
	 */
	@Override
	public String getNetworkLocation() {
		return broadcast;
	}

	/**
	 * Set the broadcast domain
	 * @param broadcastid
	 */
	@Override
	public void setNetworkLocation(String location) {
		this.broadcast = location;
	}

	/**
	 * Return the peer id
	 * @return peerid
	 */
	@Override
	public String getName() {
		return peer;
	}

	/**
	 * Need to comply with interface requirements but do nothing 
	 */
	@Override
	public Node getParent() {
		return null;
	}

	/**
	 * Need to comply with interface requirements  but do nothing
	 */
	@Override
	public void setParent(Node parent) {}

	/**
	 * Need to comply with interface requirements  but do nothing
	 */
	@Override
	public int getLevel()  {
		return -1;
	}

	/**
	 * Need to comply with interface requirements  but do nothing
	 */
	@Override
	public void setLevel(int i) {
	}
	
	/**
	 * Return the full path including peer id
	 * @return path
	 */
	public String getFullPath() {
		return Peer2peerTopology.DEFAULT_RACK + PATH_SEPARATOR + broadcast + PATH_SEPARATOR + peer;
	}
	
	/**
	 * Return the path without peerid
	 * @return path
	 */
	public String getPath() {
		return Peer2peerTopology.DEFAULT_RACK + PATH_SEPARATOR + broadcast;
	}

	/**
	 * Normalize the path to ensure it complies with standards 
	 * 		Std1 -> /datanodes/18798732/59616261646162614E504720503250334BF8CE1FAFDDF001A3775FF8FB52662B03A
	 * 		Std2 -> /datanodes/18798732/
	 * 		Std3 -> /datanodes/18798732
	 * 
	 * @param path
	 * @return
	 * @throws IllegalArgumentException
	 */
	static public String normalize(String path) throws IllegalArgumentException {
		if (path == null || path.length() == 0) return ROOT;
		    
		if (path.charAt(0) != PATH_SEPARATOR) {
			throw new IllegalArgumentException(
					"Network Location path does not start with "
					+PATH_SEPARATOR_STR+ ": "+path);
		}
		    
		String[] details = path.split("/");
		IllegalArgumentException iae = new IllegalArgumentException(
					"Network Location path is not conform not peer to peer standard :"
					+ Peer2peerTopology.DEFAULT_RACK + "/<integer>/<peerid>");
		    
		if(details.length == 4) {
			// Case #1 : /datanodes/18798732/59616261646162614E504720503250334BF8CE1FAFDDF001A3775FF8FB52662B03A
			if(!(PATH_SEPARATOR+details[1]).equals(Peer2peerTopology.DEFAULT_RACK)) {
				throw iae;
			} else if (!details[2].matches("^[0-9]+$")) {
				throw iae;
			} else if ((details[3].length() != 66) || !details[2].matches("^[0-9A-F]+$")) {
				throw iae;
			}

			return PATH_SEPARATOR + details[1] + PATH_SEPARATOR + details[2] + PATH_SEPARATOR + details[3];
		} else if(details.length == 3) {
			if(!(PATH_SEPARATOR+details[1]).equals(Peer2peerTopology.DEFAULT_RACK)) {
				throw iae;
			} else if (details[2].matches("^[0-9]+$")) {
				// Case #2 : /datanodes/18798732
				return PATH_SEPARATOR + details[1] + PATH_SEPARATOR + details[2];	
			} else if (details[2].matches("^[0-9A-F]+$")) {
				// Case #2 : /datanodes/59616261646162614E504720503250334BF8CE1FAFDDF001A3775FF8FB52662B03A
				return PATH_SEPARATOR + details[1] + PATH_SEPARATOR + "0" + PATH_SEPARATOR + details[2];
			}
		} 

		throw iae;
	}
}