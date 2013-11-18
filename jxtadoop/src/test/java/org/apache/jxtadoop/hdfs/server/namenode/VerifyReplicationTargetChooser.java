package org.apache.jxtadoop.hdfs.server.namenode;

import java.io.IOException;

import org.apache.jxtadoop.conf.Configuration;
import org.apache.jxtadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.jxtadoop.net.Peer2peerTopology;

@SuppressWarnings("unused")
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
	private NameNode namenode;
	
	public void init() throws IOException {
		 topology = new Peer2peerTopology();
		 conf = new Configuration();
		 namenode = (NameNode)null;
		 namesystem = new FSNamesystem(namenode,conf);
	}
	
	public void run() throws IOException {
		namesystem.computeDatanodeWork();
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {		
		VerifyReplicationTargetChooser vrtc = new VerifyReplicationTargetChooser();
		
		try {
			vrtc.init();
			vrtc.run();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
