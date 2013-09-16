package org.apache.jxtadoop.hdfs.p2p;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Vector;

import javax.security.auth.login.LoginException;

import org.apache.jxtadoop.security.UserGroupInformation;

import net.jxta.discovery.DiscoveryEvent;
import net.jxta.discovery.DiscoveryListener;
import net.jxta.discovery.DiscoveryService;
import net.jxta.document.Advertisement;
import net.jxta.document.MimeMediaType;
import net.jxta.exception.PeerGroupException;
import net.jxta.id.ID;
import net.jxta.impl.document.LiteXMLElement;
import net.jxta.impl.protocol.PeerAdv;
import net.jxta.platform.NetworkManager;
import net.jxta.protocol.DiscoveryResponseMsg;
import net.jxta.protocol.PeerAdvertisement;

public class P2PClientPeer extends Peer implements DiscoveryListener {
	protected List<PeerAdvertisement> datanodes;
	protected List<PeerAdvertisement> namenodes;
	protected List<Advertisement> advertisements;
	
	public P2PClientPeer(String s) {
		super(s,"p2pclient");
		datanodes = new ArrayList<PeerAdvertisement>();
		namenodes = new ArrayList<PeerAdvertisement>();
		advertisements = new ArrayList<Advertisement>();
	}
	
	@Override
	public void setupNetworking() throws LoginException, IOException, javax.security.cert.CertificateException, PeerGroupException {
		Boolean useMulticast = new Boolean(pc.get("hadoop.p2p.use.multicast"));
		
		nm = new NetworkManager(NetworkManager.ConfigMode.EDGE, UserGroupInformation.login(pc).getUserName()+"P2P Client Peer",p2pdir.toURI());
		
		nm.setConfigPersistent(false);
		
		nc = nm.getConfigurator();
		
		nc.setTcpPort(Integer.parseInt(pc.get("hadoop.p2p.p2pclient.port", P2PConstants.P2PCLIENTPORT)));
		nc.setTcpEnabled(true);
		nc.setTcpIncoming(true);
		nc.setTcpOutgoing(true);
		nc.setHttpEnabled(false);
		nc.setHttp2Enabled(false);
			
		nc.setTcpStartPort(-1);
		nc.setTcpEndPort(-1);
		if(!useMulticast) {
			nc.setUseMulticast(true);
		} else {
			nc.setUseMulticast(true);
		}
		
		nc.setPeerID(pid);
		nc.setName(P2PConstants.P2PCLIENTTAG+" - "+nc.getName());
	    nc.setKeyStoreLocation(KeyStoreFile.toURI());
	    nc.setPassword(p2ppass);
	    
	    nc.clearRendezvousSeeds();
		nc.addSeedRendezvous(URI.create(pc.get("hadoop.p2p.rpc.rdv")));
		if(!useMulticast) {
			nc.setUseOnlyRendezvousSeeds(true);
		} else {
			nc.setUseOnlyRendezvousSeeds(false);	
		}
		
		nc.clearRelaySeeds();
        nc.addSeedRelay(URI.create(pc.get("hadoop.p2p.rpc.relay")));
        if(!useMulticast) {
        	nc.setUseOnlyRelaySeeds(true);
        } else {
        	nc.setUseOnlyRelaySeeds(false);
        }
	}
	
	public void startNetworking() throws PeerGroupException, IOException {
		npg = nm.startNetwork();
		
		npg.getRendezVousService().setAutoStart(false);
		
		while(!nm.isStarted()) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {}
		}
		
		if(!nm.waitForRendezvousConnection(3000))
			throw(new IOException("Failed to contact the Rendez-Vous"));
		
		ds = npg.getDiscoveryService();
	}
	
	public void stop() {
		if(nm!=null) nm.stopNetwork();
	}
	
	public void discoverNamenodes() {		
		try {
			ds.getRemoteAdvertisements(null, DiscoveryService.PEER, "Name", "*Namenode Peer*", 1,this);
		} catch(Exception e) {
			LOG.error(e.getMessage());
		}		
		
		LOG.debug("Sleeping for "+P2PConstants.DISCOTIME+" seconds for discovery to complete");
		try {
			Thread.sleep(P2PConstants.DISCOTIME);
		} catch (InterruptedException e) {}
		
		if(namenodes!=null) {
			System.out.println("Found "+namenodes.size()+" namenode(s) in the cloud - Details :");
			while(namenodes.size()>0) {
				System.out.println("\tPID : "+((PeerAdvertisement)namenodes.remove(0)).getPeerID());
			}
		}
	}
	
	public void discoverDatanodes() {		
		try {
			ds.getRemoteAdvertisements(null, DiscoveryService.PEER, "Name", "*Datanode Peer*", P2PConstants.MAXCLOUDPEERCOUNT,this);
		} catch(Exception e) {
			LOG.error(e.getMessage());
		}		
		
		LOG.debug("Sleeping for "+P2PConstants.DISCOTIME+" seconds for discovery to complete");
		try {
			Thread.sleep(P2PConstants.DISCOTIME);
		} catch (InterruptedException e) {}
		
		System.out.print("Found "+datanodes.size()+" datanode(s) in the cloud");	
		if(datanodes.size() > 0) System.out.print(" - Details :");
		System.out.print("\n");
			
		while(datanodes.size()>0) {
			System.out.println("\tPID : "+((PeerAdvertisement)datanodes.remove(0)).getPeerID());
		}
	}

	public void discoverAdvertisements() {		
		try {
			ds.getRemoteAdvertisements(null, DiscoveryService.ADV, null, null, P2PConstants.MAXADVDISCOVERYCOUNT,this);
		} catch(Exception e) {
			LOG.error(e.getMessage());
		}		
		
		LOG.debug("Sleeping for "+P2PConstants.DISCOTIME+" seconds for discovery to complete");
		try {
			Thread.sleep(P2PConstants.DISCOTIME);
		} catch (InterruptedException e) {}
		
		System.out.print("Found "+advertisements.size()+" advertisement(s) in the cloud");	
		if(advertisements.size() > 0) System.out.print(" - Details :");
		System.out.print("\n");
			
		while(advertisements.size()>0) {
			System.out.println("---------------------------------------------------------------");
			System.out.println(((Advertisement)advertisements.remove(0)).getDocument(MimeMediaType.TEXTUTF8));
		}
	}
	
	public void discoverNode(String peerid) {
		try {
			ds.getRemoteAdvertisements(null, DiscoveryService.PEER, "PID", peerid,  1,this);
		} catch(Exception e) {
			LOG.error(e.getMessage());
		}
		
		LOG.debug("Sleeping for "+P2PConstants.DISCOTIME+" seconds for discovery to complete");
		try {
			Thread.sleep(P2PConstants.DISCOTIME);
		} catch (InterruptedException e) {}
		
		if (datanodes.size() == 0)
			System.err.println("Datanode "+peerid+" not found");
		else {
			PeerAdvertisement peerAdv = (PeerAdvertisement)datanodes.remove(0);
			
			String output = "Node Peer ID : " + peerid;
			output += "\n   Name\t: "+peerAdv.getName();
			output += "\n   Desc\t: "+peerAdv.getDescription();
			output += "\n   Peer Group ID : " + peerAdv.getPeerGroupID();
			
			try {
				ID nmcid = ID.create(new URI("urn:jxta:uuid-DEADBEEFDEAFBABAFEEDBABE0000000805"));
				LiteXMLElement lxel = (LiteXMLElement)peerAdv.getServiceParam(nmcid);
				
				Vector<LiteXMLElement> eas = new Vector<LiteXMLElement>();
				
				findDST(lxel,eas);
				
				while(eas.size() > 0) {
					String tcp = ((LiteXMLElement)eas.remove(0)).getValue();
					if(tcp.startsWith("tcp://")) 
						output += "\n   Endpoint : "+tcp;
				}				
			} catch (URISyntaxException e) {	}
			
			System.out.print(output);
			
		} 
	}
	
	private void findDST(LiteXMLElement l, Vector<LiteXMLElement>  v) {
		Enumeration<LiteXMLElement> d = l.getChildren();
		
		if("Dst".equals(l.getName())) {
			while(d.hasMoreElements()) {
				LiteXMLElement i = (LiteXMLElement)d.nextElement();
				Enumeration<LiteXMLElement> e = i.getChildren();
				while(e.hasMoreElements()) 
					v.add(e.nextElement());
			}
		}
			
		while (d.hasMoreElements()) {
			findDST(d.nextElement(),v);
		}
	}
	
	@Override
	public void discoveryEvent(DiscoveryEvent event) {
		DiscoveryResponseMsg response = event.getResponse();
		
		if (response.getDiscoveryType() == DiscoveryService.PEER) {
			Enumeration<Advertisement> en = response.getAdvertisements();
			PeerAdv adv;
			
			while (en.hasMoreElements()) {
				adv = (PeerAdv) en.nextElement();
				if (adv instanceof PeerAdv) {
					if ((adv.getName()).contains("Datanode Peer")) {
						datanodes.add(adv);
					} else if ((adv.getName()).contains("Namenode Peer")) {
						namenodes.add(adv);
					}
				}
			}
		} else if (response.getDiscoveryType() == DiscoveryService.ADV) {
			Enumeration<Advertisement> en = response.getAdvertisements();
		 Advertisement adv;
			
			while (en.hasMoreElements()) {
				adv = (Advertisement) en.nextElement();
				if (adv instanceof Advertisement) {
					advertisements.add(adv);
				}
			}
		}
	}
}
