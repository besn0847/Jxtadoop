package org.apache.jxtadoop.hdfs.p2p;

import java.util.EventObject;

import net.jxta.peer.PeerID;

public class DatanodeEvent extends EventObject {
	
	private PeerID datanodeId;

	public DatanodeEvent(Object source,PeerID pid) {
		super(source);
		
		this.datanodeId = pid;	
	}
	
	public String toString() {
		return super.toString() + " : disconnect for [" + datanodeId.toString() + "]";		
	}

	public PeerID getPeerID() {
		return datanodeId;
	}
}
