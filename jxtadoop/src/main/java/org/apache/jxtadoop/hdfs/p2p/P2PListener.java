package org.apache.jxtadoop.hdfs.p2p;

import java.util.EventListener;

public interface P2PListener extends EventListener {
	public void handleDisconnectEvent(DatanodeEvent e);
}
