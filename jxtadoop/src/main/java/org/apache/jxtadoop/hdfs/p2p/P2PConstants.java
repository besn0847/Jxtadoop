package org.apache.jxtadoop.hdfs.p2p;

/**
 * All the constants and default values are published through this interface. 
 * 
 * @author Franck Besnard <franck@besnard.mobi>
 * @version 1.0
 * @since November, 2011
 * 
 */
public interface P2PConstants {
	/**
	 * The default RPC Pipe ID used by all peers; 
	 */
	public static final String RPCPIPEID = "urn:jxta:uuid-CECDC14D1F334B0EB0A4A269F7A38C918F0350E117A049B89FC3D60D98BD07EF04";
	/**
	 * The default RPC Pipe name used by all peers; 
	 */
	public static final String RPCPIPENAME = "Jxtadoop P2P RPC Pipe";
	/**
	 * The default RPC Pipe description.
	 */
	public static final String RPCPIPEDESC = "Used for remote procedure calls between DN & NN";
	/**
	 * The default INFO Pipe ID used by all peers; 
	 */
	public static final String INFOPIPEID = "urn:jxta:uuid-CECDC14D1F334B0EB0A4A269F7A38C91496E666F20504970A5209365656404";
	/**
	 * The default RPC Pipe name used by all peers; 
	 */
	public static final String INFOPIPENAME = "Jxtadoop P2P INFO Pipe";
	/**
	 * The default RPC Pipe description.
	 */
	public static final String INFOPIPEDESC = "Used for block data transfert between DNs";
	/**
	 * The key store provider
	 */
	public static final String RPCKEYSTOREPROVIDER = "The Jxtadoop Key Store Provider";
	/**
	 * The default namenode peer listening port for peer-to-peer communications.
	 */
	public static final String RPCNAMENODEPORT = "9100";
	/**
	 * The default datanode peer listening port for peer-to-peer communications.
	 */
	public static final String RPCDATANODEPORT = "9101";
	/**
	 * The default dfs client peer listening port for peer-to-peer communications.
	 */
	public static final String RPCDFSCLIENTPORT = "9102";
	/**
	 * The namenode tag used in the advertisement from those peers
	 */
	public static final String RPCNAMENODETAG = "NAMENODE";
	/**
	 * The datanode tag used in the advertisement from those peers
	 */
	public static final String RPCDATANODETAG = "DATANODE";
	/**
	 * The dfs client tag used in the advertisement from those peers
	 */
	public static final String RPCDFSCLIENTTAG = "DFSCLIENT";
	/**
	 * The timeout for a peer remote discovery used by the peer monitor.
	 */
	public static final int PEERDELETIONTIMEOUT = 20000;
	/**
	 * The maximum number of retries before considering a datanode is no longer part of the peer cloud.
	 */
	public static final int PEERDELETIONRETRIES = 3;
	/**
	 * The maximum number of peers in the cloud
	 */
	public static final int MAXCLOUDPEERCOUNT = 100;
	/**
	 * Socket send buffer size
	 */
	public static final int JXTA_SOCKET_SENDBUFFER_SIZE = 64 * 1024;
	/**
	 * Socket receive buffer size
	 */
	public static final int JXTA_SOCKET_RECVBUFFER_SIZE = 512 * 1024;
	/**
	 * I/O file buffer size
	 */
	public static final int IO_FILE_BUFFER_SIZE = 1024;
	/**
	 * Default DFS replication factor
	 */
	public static final int DEFAULT_DFS_REPLICATION = 3;
	/**
	 * Default DFS block size
	 */
	public static final long DEFAULT_BLOCK_SIZE = 1 * 1024 * 1024;
	/**
	 * Default local block size
	 */
	public static final long DEFAULT_LOCAL_BLOCK_SIZE = 1 * 1024 * 1024;
	/**
	 * Default I/O bytes per checksum
	 */
	public static final int IO_BYTES_PER_CHECKSUM = 512;
	/**
	 * Number of chars in the peer id
	 */
	public static final int PEER_ID_LENGTH = 66;
}
