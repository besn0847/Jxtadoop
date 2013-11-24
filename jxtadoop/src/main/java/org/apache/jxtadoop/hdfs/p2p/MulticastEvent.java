package org.apache.jxtadoop.hdfs.p2p;

import java.util.Collection;
import java.util.EventObject;
import java.util.HashMap;

/**
 * Class used to notify the Namenode about a multicast discovery by the NamenodePeer
 * @author franck
 *
 */
public class MulticastEvent extends EventObject {
	private static final long serialVersionUID = -6873478936739471516L;
	
	private String peerid;
	private HashMap<String, Long> domain;
	
	private int hash;
	
	public MulticastEvent(Object source, String peerid, HashMap<String, Long> domain) {
		super(source);
		this.peerid = peerid;
		this.domain = domain;
		this.hash = domain.hashCode();
	}
	
	public String getPeerID() {
		return this.peerid;
	}
	
	public HashMap<String, Long> getDomain() {
		return this.domain;
	}

	public int getHash() {
		return this.hash;
	}
}
