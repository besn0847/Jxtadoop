package org.apache.jxtadoop.hdfs.server.namenode;

import org.apache.jxtadoop.conf.Configuration;
import org.apache.jxtadoop.net.Peer2peerTopology;

public class VerifyReplicationTargetChooser {
	  static{
		    Configuration.addDefaultResource("hdfs-default.xml");
		    Configuration.addDefaultResource("hdfs-site.xml");
		    Configuration.addDefaultResource("hdfs-p2p.xml");
		  }
	  
	private Peer2peerTopology topology;
	private FSNamesystem namesystem;
	private ReplicationTargetChooser chooser;
	private Configuration conf;
	
	public void init() {
		 topology = new Peer2peerTopology();
		 conf = new Configuration();
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		

	}

}
