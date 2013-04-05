package org.apache.jxtadoop.hdfs.p2p;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.security.auth.login.LoginException;

import net.jxta.discovery.DiscoveryService;
import net.jxta.exception.PeerGroupException;
import net.jxta.peer.PeerID;
import net.jxta.platform.NetworkManager;
import net.jxta.protocol.PeerAdvertisement;
import net.jxta.rendezvous.RendezVousService;
import net.jxta.rendezvous.RendezvousEvent;
import net.jxta.rendezvous.RendezvousListener;
import net.jxta.socket.JxtaServerSocket;
import net.jxta.socket.JxtaSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.jxtadoop.conf.Configuration;
import org.apache.jxtadoop.security.UserGroupInformation;

/**
 * The namenode peer is an extension of the Jxtadoop peer. <br>
 * It implements the methods from the RendezvousListener interface as well as overrides specific peer methods :<br>
 * <p>setupnetworking()<br>
 * <p>start()<br>
 * <br>
 * The method to return the RPC socket server is also implemented as this is required for the Hadoop RPC server.<br> 
 * 
 * 
 * @author Franck Besnard <franck@besnard.mobi>
 * @version 1.0
 * @since November, 2011
 *
 */
public class NamenodePeer extends Peer implements RendezvousListener {
	public static final Log LOG = LogFactory.getLog(NamenodePeer.class);
	/**
	 * The jxta server socket on which the RPC server will liste,.
	 */
	private JxtaServerSocket jss;
	private RendezVousService rdv;
	/**
	 * The listener array
	 */
	private List _dnlisteners = new ArrayList();
	
	/**
	 * Constructor with the peer name unique ID. This is important for the peer ID and key generation.
	 * @param s The peer unique name
	 */
	public NamenodePeer(String s) {
		super(s);
	}
	/**
	 * Constructor with the peer name unique ID and the configuration to be used. This is important for the peer ID and key generation.
	 * @param s The peer unique name
	 * @param c The configuration to be used
	 */
	public NamenodePeer(String s, Configuration c) {
		super(s,c);
	}
	/**
	 * This method set up the jxta peer network configuration.
	 * <br> The peer is configured as a Rendez-Vous and as a Relay.
	 */
	@Override
	public void setupNetworking() throws LoginException, IOException, javax.security.cert.CertificateException, PeerGroupException {
		nm = new NetworkManager(NetworkManager.ConfigMode.RENDEZVOUS_RELAY, UserGroupInformation.login(pc).getUserName()+"Namenode Peer",p2pdir.toURI());
		
		nm.setConfigPersistent(false);
		
		nc = nm.getConfigurator();
				
		//if (!nc.exists()) {
			nc.setTcpPort(Integer.parseInt(pc.get("hadoop.p2p.namenode.port", P2PConstants.RPCNAMENODEPORT)));
			nc.setTcpEnabled(true);
			nc.setTcpIncoming(true);
			nc.setTcpOutgoing(true);
			nc.setHttpEnabled(false);
			nc.setHttp2Enabled(false);
			
			nc.setTcpStartPort(-1);
			nc.setTcpEndPort(-1);
			nc.setTcpInterfaceAddress(pc.get("hadoop.p2p.rpc.relay").replaceAll("tcp://","").substring(
					0, 
					pc.get("hadoop.p2p.rpc.relay").replaceAll("tcp://","").indexOf(":"))
				);
			nc.setTcpPublicAddress(pc.get("hadoop.p2p.rpc.relay").replaceAll("tcp://",""),true);
			nc.setUseMulticast(Boolean.getBoolean(pc.get("hadoop.p2p.use.multicast")));
			
			nc.setPeerID(pid);
			nc.setName(P2PConstants.RPCNAMENODETAG+" - "+nc.getName());
	        nc.setKeyStoreLocation(KeyStoreFile.toURI());
	        nc.setPassword(p2ppass);
	        
	    //    nc.save();
		/*} else {
	        nc.setKeyStoreLocation(KeyStoreFile.toURI());
	        nc.setPassword(p2ppass);
	        
	        nc.load();
		}*/
		
		npg = nm.startNetwork();
		
		rdv = npg.getRendezVousService();
		rdv.addListener(this);
		
		while(!nm.isStarted()) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {}
		}
		
		ds = npg.getDiscoveryService();
	}
	/**
	 * The start-up includes 2 steps.<br>
	 * First, the pipe advertisement is published on the datanode cloud.<br>
	 * Then the peer monitor thread is started to monitor the datanode cloud.
	 */
	@Override
	public void start() {
		try {			
			publishPipeAdvertisement();
		} catch (IOException e) {
			LOG.fatal("Cannot start the Namenode peer; Aborting");
			e.printStackTrace();
			throw new RuntimeException();
		}
		
		Thread thread = new Thread(this.new PeerMonitor(),"Peer Monitor Thread");
		thread.start();
	}
	/**
	 * The datanode list is managed for the namenode using the connection events. 
	 */
	@Override
	public void rendezvousEvent(RendezvousEvent event) {			
		if ( event.getType() == RendezvousEvent.CLIENTCONNECT || event.getType() == RendezvousEvent.CLIENTRECONNECT) {
			LOG.info("\tClient connected - PeerID : "+event.getPeerID());
			datanodepeers.put((PeerID)event.getPeerID(),(PeerAdvertisement)null);
			LOG.debug("Total number of datanode in the cloud : "+datanodepeers.size());
		} else if (event.getType() == RendezvousEvent.CLIENTDISCONNECT || event.getType() == RendezvousEvent.CLIENTFAILED) {
			LOG.info("\tClient disconnected - PeerID : "+event.getPeerID());
			if(datanodepeers.containsKey((PeerID)event.getPeerID()))
				datanodepeers.remove((PeerID)event.getPeerID());
			LOG.debug("Total number of datanode in the cloud : "+datanodepeers.size());
		} else {
			LOG.warn("Something weird happenned : "+event.getType() );
		}
	}
	/**
	 * Published the pipe advertisement in the cloud and set the server socket object.
	 * @throws IOException The publishing failed.
	 */
	protected void publishPipeAdvertisement () throws IOException {
		ds.publish(rpcPipeAdv);
		ds.remotePublish(rpcPipeAdv,DiscoveryService.NO_EXPIRATION);
		
		jssad = new JxtaSocketAddress(npg,rpcPipeAdv,npg.getPeerAdvertisement());
	}
	/**
	 * Returns the RPC socket address of the server
	 * @return The RPC server socket address
	 */
	public JxtaServerSocket getRpcServerSocket() {
		return this.jss;
	}
	/**
	 * Adding a new listener to the track list
	 * 
	 * @param listener
	 */
	public synchronized void addP2PEventListener(P2PListener listener)	{
	    _dnlisteners.add(listener);
	  }
	/**
	 * Removing a new listener from the track list
	 * 
	 * @param listener
	 */
	public synchronized void removeEventListener(P2PListener listener)	{
	    _dnlisteners.remove(listener);
	  }
	/**
	 * Triggering a disconnection event and notifying the subscribers
	 * 
	 * @param event
	 */
	@Override
	public synchronized void fireEvent(DatanodeEvent event)	{
		Iterator i = _dnlisteners.iterator();

		while(i.hasNext())	{
			((P2PListener) i.next()).handleDisconnectEvent(event);
		}
	}
}
