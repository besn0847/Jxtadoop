package org.apache.jxtadoop.hdfs.p2p;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateEncodingException;
import java.util.Enumeration;

import javax.security.auth.login.LoginException;
import javax.security.cert.CertificateException;

import net.jxta.discovery.DiscoveryEvent;
import net.jxta.discovery.DiscoveryListener;
import net.jxta.discovery.DiscoveryService;
import net.jxta.document.Advertisement;
import net.jxta.document.AdvertisementFactory;
import net.jxta.document.Element;
import net.jxta.document.MimeMediaType;
import net.jxta.exception.PeerGroupException;
import net.jxta.impl.protocol.PeerAdv;
import net.jxta.platform.NetworkManager;
import net.jxta.protocol.DiscoveryResponseMsg;
import net.jxta.protocol.PipeAdvertisement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.jxtadoop.conf.Configuration;
import org.apache.jxtadoop.hdfs.p2p.Peer.PeerMonitor;
import org.apache.jxtadoop.security.UserGroupInformation;

public class DiscoveryPeer extends Peer implements Runnable, DiscoveryListener {
	public static final Log LOG = LogFactory.getLog(DiscoveryPeer.class);

	private boolean running = false;
	/*
	 * Thread used to monitor peers
	 */
	Thread discover;
	
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
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				//e.printStackTrace();
			}
		}
	}
	
	@Override
	public void discoveryEvent(DiscoveryEvent event) {
		DiscoveryResponseMsg response = event.getResponse();
		MulticastAdvertisement multiadv = null;
		
		if (response.getDiscoveryType() == DiscoveryService.PEER) {
			Enumeration<Advertisement> en = response.getAdvertisements();
			PeerAdv adv;
			 
			while (en.hasMoreElements()) {
				adv = (PeerAdv) en.nextElement();
				if((adv.getName()).contains("Datanode Peer")) {
					LOG.debug("Found a datanode peer : " + adv.getPeerID());
					multiadv = new MulticastAdvertisement();
					multiadv.setRemote(adv.getPeerID().toString());
					try {
						ds.publish(multiadv);
						ds.remotePublish(multiadv);
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		} 
	}
}