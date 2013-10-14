package org.apache.jxtadoop.hdfs.p2p;

import java.util.Collection;
import java.util.EventObject;

/**
 * Class used to notify the Namenode about a multicast discovery by the NamenodePeer
 * @author franck
 *
 */
public class MulticastEvent extends EventObject {
	private String peerid;
	private Collection<String> domain;
	private int hash;
	
	public MulticastEvent(Object source, String peerid, Collection<String> domain) {
		super(source);
		this.peerid = peerid;
		this.domain = domain;
		this.hash = domain.hashCode();
	}
	
	public String getPeerID() {
		return this.peerid;
	}
	
	public Collection<String> getDomain() {
		return this.domain;
	}

	public int getHash() {
		return this.hash;
	}
}
