package org.apache.jxtadoop.hdfs.p2p;

import java.util.EventListener;

public interface MulticastListener extends EventListener {
	public void multicastDetected(MulticastEvent me);
}
