package org.apache.jxtadoop.hdfs.p2p;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateEncodingException;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Iterator;

import javax.security.auth.login.LoginException;
import javax.security.cert.CertificateException;

import net.jxta.discovery.DiscoveryEvent;
import net.jxta.discovery.DiscoveryListener;
import net.jxta.discovery.DiscoveryService;
import net.jxta.document.Advertisement;
import net.jxta.document.AdvertisementFactory;
import net.jxta.document.MimeMediaType;
import net.jxta.endpoint.EndpointService;
import net.jxta.endpoint.router.RouteController;
import net.jxta.exception.PeerGroupException;
import net.jxta.impl.protocol.PeerAdv;
import net.jxta.platform.NetworkManager;
import net.jxta.protocol.DiscoveryResponseMsg;
import net.jxta.protocol.RouteAdvertisement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.jxtadoop.security.UserGroupInformation;

public class DiscoveryPeer extends Peer implements Runnable, DiscoveryListener {
	public static final Log LOG = LogFactory.getLog(DiscoveryPeer.class);

	private boolean running = false;
	/*
	 * Thread used to monitor peers
	 */
	Thread discover;
	/**
	 * Associated datanode
	 */
	DatanodePeer datanodepeer;
	/**
	 * Few routing services
	 */
	EndpointService endpoint;
	RouteController rcontrol;
	
	public DiscoveryPeer(String s) {
		super(s,"disco");
	}
	
	/**
	 * This method set up the jxta peer network configuration.
	 * <br> The peer is configured as an Edge. There is no connexion to the NN; only using multicast
	 * @throws LoginException Thrown upon user login error
	 * @throws IOException Thrown upon failure to connect to the namenode
	 * @throws CertificateException Thrown if the peer cannot unlock the keystore
	 * @throws PeerGroupException Thrown if the peer cannot join the default net peergroup
	 */
	 
	@Override
	public void setupNetworking() throws LoginException, IOException, javax.security.cert.CertificateException, PeerGroupException {
		nm = new NetworkManager(NetworkManager.ConfigMode.ADHOC, UserGroupInformation.login(pc).getUserName()+"Discovery Peer",p2pdir.toURI());
		
		nm.setConfigPersistent(false);
		
		nc = nm.getConfigurator();
		
		nc.setPeerID(pid);
		nc.setName(P2PConstants.P2PDISCOTAG+" - "+nc.getName());
	    nc.setKeyStoreLocation(KeyStoreFile.toURI());
	    nc.setPassword(p2ppass);
	    
	    nc.clearRendezvousSeeds();
	    nc.clearRelaySeeds();
	    nc.setUseOnlyRelaySeeds(true);
	 }

	public void startNetworking() throws PeerGroupException, IOException {
		npg = nm.startNetwork();
		
		while(!nm.isStarted()) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {}
		}
		
		ds = npg.getDiscoveryService();
	}
	
	public void init() throws LoginException, SecurityException, URISyntaxException, CertificateEncodingException, UnrecoverableKeyException, NoSuchProviderException, KeyStoreException, NoSuchAlgorithmException, IOException, PeerGroupException, CertificateException {
		loadPeerId();
		loadKeyStoreManager();
		setupNetworking();
		startNetworking();
	}
	
	@Override
	public void start() {
		AdvertisementFactory.registerAdvertisementInstance(MulticastAdvertisement.getAdvertisementType(),new  MulticastAdvertisement.Instantiator());
		running = true;
		
		endpoint = npg.getEndpointService();
		rcontrol = endpoint.getEndpointRouter().getRouteController();
		
		discover = new Thread(this,"Discovery Peer Manager");
		discover.start(); 
	}
	
	public void stop() {
		if(nm!=null) nm.stopNetwork();
	}
	
	@Override
	public void run() {
		while(running) {
			try {
				ds.getRemoteAdvertisements(null, DiscoveryService.PEER, "Name", "*Datanode Peer*", P2PConstants.MAXADVDISCOVERYCOUNT,this);
			} catch(Exception e) {
				LOG.error(e.getMessage());
				e.printStackTrace();
			}		
			try {
				Thread.sleep(P2PConstants.MULTICASTADVLIFETIME);
			} catch (InterruptedException e) {
				//e.printStackTrace();
			}
		}
	}
	
	@Override
	public void discoveryEvent(DiscoveryEvent event) {
		DiscoveryResponseMsg response = event.getResponse();
		MulticastAdvertisement multiadv = null;
		RouteAdvertisement routeadv;
		Collection<RouteAdvertisement> routeadvs;
		Iterator<RouteAdvertisement> routes;
		boolean isDirect;
		
		if (response.getDiscoveryType() == DiscoveryService.PEER) {
			Enumeration<Advertisement> en = response.getAdvertisements();
			PeerAdv adv;
			 
			while (en.hasMoreElements()) {
				adv = (PeerAdv) en.nextElement();
				if((adv.getName()).contains("Datanode Peer")) {
					LOG.debug("Found a datanode peer : " + adv.getPeerID());
					isDirect = false;
					
					routeadvs = rcontrol.getRoutes(adv.getPeerID());
					LOG.debug("Route exist or possible ? "+rcontrol.isConnected(adv.getPeerID()));
					routes = routeadvs.iterator();
					while(routes.hasNext()) {
						routeadv = routes.next();
						LOG.debug("Route length : "+ routeadv.size());
						LOG.debug("Route : "+routeadv.getDocument(MimeMediaType.TEXTUTF8));
						if(routeadv.size()==0)
							isDirect = true;
					}
					
					if (isDirect && !datanodepeer.isRelay()) {
						multiadv = new MulticastAdvertisement();
						multiadv.setRemote(adv.getPeerID().toString());
						if(datanodepeer != null) {
							multiadv.setLocal(this.datanodepeer.getPeerID().toString());
							try {
								LOG.debug("Publishing a multicast adv with : \n\t local : "
										+ multiadv.getLocal() + "\n\t remote : "
										+ multiadv.getRemote() + "\n");
								ds.publish(multiadv,2*P2PConstants.MULTICASTADVLIFETIME,2*P2PConstants.MULTICASTADVLIFETIME);
								ds.remotePublish(null,multiadv,2*P2PConstants.MULTICASTADVLIFETIME);
							} catch (IOException e) {
								e.printStackTrace();
							}
						}
					}
				}
			}
		}
	}

	public void setDatanode(DatanodePeer dnpeer) {
		this.datanodepeer = dnpeer;
	}
}
