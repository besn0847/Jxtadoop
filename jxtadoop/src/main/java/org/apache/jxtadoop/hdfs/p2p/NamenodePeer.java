package org.apache.jxtadoop.hdfs.p2p;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import javax.security.auth.login.LoginException;

import net.jxta.discovery.DiscoveryEvent;
import net.jxta.discovery.DiscoveryListener;
import net.jxta.discovery.DiscoveryService;
import net.jxta.document.Advertisement;
import net.jxta.document.MimeMediaType;
import net.jxta.endpoint.EndpointAddress;
import net.jxta.endpoint.router.RouteController;
import net.jxta.exception.PeerGroupException;
import net.jxta.id.IDFactory;
import net.jxta.impl.protocol.PeerAdv;
import net.jxta.peer.PeerID;
import net.jxta.platform.NetworkManager;
import net.jxta.protocol.DiscoveryResponseMsg;
import net.jxta.protocol.PeerAdvertisement;
import net.jxta.protocol.RouteAdvertisement;
import net.jxta.rendezvous.RendezVousService;
import net.jxta.rendezvous.RendezvousEvent;
import net.jxta.rendezvous.RendezvousListener;
import net.jxta.socket.JxtaServerSocket;
import net.jxta.socket.JxtaSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.jxtadoop.conf.Configuration;
import org.apache.jxtadoop.hdfs.server.namenode.NameNode;
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
@SuppressWarnings({"rawtypes","unchecked"})
public class NamenodePeer extends Peer implements RendezvousListener, DiscoveryListener {
	public static final Log LOG = LogFactory.getLog(NamenodePeer.class);
	/**
	 * The Namonode parent; needed to access the cluster map
	 */
	private NameNode namenodeObject;
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
	 *  The map linking peers to multi cast domain 
	 */
	private HashMap<String,Collection<String>> multicastMap = new HashMap<String,Collection<String>>();
	/**
	 * All the listeners listening to multicast event
	 */
	private transient Vector multicastListeners;
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
	 * Constructor with the peer name unique ID and the configuration to be used. This is important for the peer ID and key generation.
	 * @param s The peer unique name
	 * @param c The configuration to be used
	 * @param n The namenode parent
	 */
	public NamenodePeer(String s, Configuration c, NameNode n) {
		super(s,c);
		this.namenodeObject = n;
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
		ds.addDiscoveryListener(this);
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
			LOG.error(e.getMessage());
			//e.printStackTrace();
			throw new RuntimeException();
		}
		
		Thread thread = new Thread(this.new PeerMonitor(),"Peer Monitor Thread");
		thread.start();
	}
	/**
	 * The datanode list is managed for the namenode using the connection events. 
	 */
	public void rendezvousEvent(RendezvousEvent event) {			
		synchronized(datanodepeers) {
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
	}
	/**
	 * Process multicast advertisment from Datanode peers 
	 */
	@Override
	public void discoveryEvent(DiscoveryEvent event) {
		DiscoveryResponseMsg response = event.getResponse();
		
		if (response.getDiscoveryType() == DiscoveryService.ADV) {
			Enumeration<Advertisement> en = response.getAdvertisements();
			
			if(en!=null) {
				while (en.hasMoreElements()) {
					Advertisement adv = (Advertisement) en.nextElement();
									
					// A multicast adv has been found : updating the topology hash
					if (adv.getAdvType().equals(MulticastAdvertisement.AdvertisementType)) {
							MulticastAdvertisement madv = (MulticastAdvertisement)adv;
							String remote = madv.getRemote();
							String local = madv.getLocal();
							String msg = "Multicast domain : "
									+ "\n\t Local : \t" + local
									+ "\n\t Remote : \t" + remote
									+ "\n" ;
							
							// 
							if(namenodeObject.namesystem.contains(local) && namenodeObject.namesystem.contains(remote))							
								// LOG.debug(msg);
								if(!multicastMap.containsKey(local) && !multicastMap.containsKey(remote)) {
									LOG.debug("Adding local & remote peers to the multicast map");
									Collection<String> domain = (Collection)new HashSet<String>();
									domain.add(local);
									domain.add(remote);
									multicastMap.put(local, domain);
									multicastMap.put(remote, domain);
								} else if (!multicastMap.containsKey(local) && multicastMap.containsKey(remote)) {
									LOG.debug("Adding local peer to the multicast map");
									Collection<String> domain = multicastMap.get(remote);
									domain.add(local);
									multicastMap.put(local, domain);
								} else if (multicastMap.containsKey(local) && !multicastMap.containsKey(remote)) {
									LOG.debug("Adding remote peer to the multicast map");
									Collection<String> domain = multicastMap.get(local);
									domain.add(remote);
									multicastMap.put(remote, domain);
								} else {
									LOG.debug("Peers already in the multicast map");
									Collection<String> domain = multicastMap.get(local);
									//String mcd = "--Multicast domain contains : ";
								
									String s;
									Iterator<String> is = domain.iterator();
									while(is.hasNext()) {
										//s = is.next();
										//mcd += "\n\t" + s;
										fireMulticastEvent(new MulticastEvent(this,local,domain));
									}
									//mcd += "\n";
									//LOG.debug(mcd);
								}
							} 
						}
					}
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
	/**
	 * Add the multicast listener to the track list
	 **/
	synchronized public void addMulticastListener(MulticastListener l) {
		if(multicastListeners == null) {
			multicastListeners = new Vector();
		}
		multicastListeners.addElement(l);
	}
	/**
	 * Remove the multicast listener from the track list
	 **/
	synchronized public void removeMulticastListener(MulticastListener l) {
		if(multicastListeners == null) {
			multicastListeners = new Vector();
		}
		multicastListeners.removeElement(l);
	}
	/**
	 * Fire multicast event
	 */
	public synchronized void fireMulticastEvent(MulticastEvent event)	{
		Iterator i = multicastListeners.iterator();
		
		while(i.hasNext())	{
			((MulticastListener) i.next()).multicastDetected(event);
		}
	}
}
