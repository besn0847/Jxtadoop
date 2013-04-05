package org.apache.jxtadoop.hdfs.p2p;

import java.io.File;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.ListIterator;

import javax.security.auth.login.LoginException;

import org.apache.jxtadoop.security.UserGroupInformation;

import net.jxta.discovery.DiscoveryEvent;
import net.jxta.discovery.DiscoveryListener;
import net.jxta.discovery.DiscoveryService;
import net.jxta.document.Advertisement;
import net.jxta.document.MimeMediaType;
import net.jxta.exception.PeerGroupException;
import net.jxta.id.IDFactory;
import net.jxta.impl.protocol.PeerAdv;
import net.jxta.peer.PeerID;
import net.jxta.platform.NetworkManager;
import net.jxta.protocol.DiscoveryResponseMsg;
import net.jxta.protocol.PeerAdvertisement;
import net.jxta.protocol.PipeAdvertisement;
import net.jxta.socket.JxtaSocket;
import net.jxta.socket.JxtaSocketAddress;

public class DFSClientPeer extends Peer implements DiscoveryListener {
	List<PipeAdvertisement> rpcpipeadvs;
	
	public DFSClientPeer(String s) {
		super(s,"client");
		
		rpcpipeadvs =  new ArrayList<PipeAdvertisement>();
		namenodepeers =  new ArrayList<PeerAdvertisement>();
	}
	
	@Override
	public void setupNetworking() throws LoginException, IOException, javax.security.cert.CertificateException, PeerGroupException {
		nm = new NetworkManager(NetworkManager.ConfigMode.EDGE, UserGroupInformation.login(pc).getUserName()+"DFS Client Peer",p2pdir.toURI());
		
		nm.setConfigPersistent(false);
		
		nc = nm.getConfigurator();
		
		//if (!nc.exists()) {
			nc.setTcpPort(Integer.parseInt(pc.get("hadoop.p2p.dfsclient.port", P2PConstants.RPCDFSCLIENTPORT)));
			nc.setTcpEnabled(true);
			nc.setTcpIncoming(true);
			nc.setTcpOutgoing(true);
			nc.setHttpEnabled(false);
			nc.setHttp2Enabled(false);
			
			nc.setTcpStartPort(-1);
			nc.setTcpEndPort(-1);
			nc.setUseMulticast(Boolean.getBoolean(pc.get("hadoop.p2p.use.multicast")));
			
			nc.setPeerID(pid);
			nc.setName(P2PConstants.RPCDFSCLIENTTAG+" - "+nc.getName());
	        nc.setKeyStoreLocation(KeyStoreFile.toURI());
	        nc.setPassword(p2ppass);
	        
	    //    nc.save();
		/*} else {
	        nc.setKeyStoreLocation(KeyStoreFile.toURI());
	        nc.setPassword(p2ppass);
	        
	        nc.load();
		}*/
		
		nc.clearRendezvousSeeds();
		nc.addSeedRendezvous(URI.create(pc.get("hadoop.p2p.rpc.rdv")));
		nc.setUseOnlyRendezvousSeeds(true);
		
		nc.clearRelaySeeds();
        nc.addSeedRelay(URI.create(pc.get("hadoop.p2p.rpc.relay")));
        nc.setUseOnlyRelaySeeds(true);
        
		npg = nm.startNetwork();
		
		npg.getRendezVousService().setAutoStart(false);
		
		while(!nm.isStarted()) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {}
		}
		
		nm.waitForRendezvousConnection(20000);
		ds = npg.getDiscoveryService();
	}
	
	@Override
	public void start() {		
		try {
			ds.getRemoteAdvertisements(null, DiscoveryService.PEER, "Name", "*Namenode Peer*", 1,this);
		} catch(Exception e) {
			e.printStackTrace();
		}		
	}

	@Override
	public void discoveryEvent(DiscoveryEvent event) {
		DiscoveryResponseMsg response = event.getResponse();
		
		if (response.getDiscoveryType() == DiscoveryService.ADV) {
			Enumeration<Advertisement> en = response.getAdvertisements();
			PipeAdvertisement adv;
			
			if(en!=null) {
				while (en.hasMoreElements()) {
					adv = (PipeAdvertisement) en.nextElement();
									
					if (adv instanceof PipeAdvertisement) {
						if ( adv.getName().equals(P2PConstants.RPCPIPENAME)) {
							LOG.debug("Found the RPC pipe; Pipe id : "+adv.getPipeID().toString());
							rpcpipeadvs.add(adv);
						}
					}
				}
			}
			
			if(rpcpipeadvs.size()==1) {
				jsad = new JxtaSocketAddress(npg, rpcpipeadvs.get(0),namenodepeers.get(0));
			} else {
				jsad = null;
			}				
		} else if (response.getDiscoveryType() == DiscoveryService.PEER) {
			Enumeration<Advertisement> en = response.getAdvertisements();
			PeerAdv adv;
			 
			while (en.hasMoreElements()) {
				adv = (PeerAdv) en.nextElement();
				if (adv instanceof PeerAdv) {
					if ((adv.getName()).contains("Namenode Peer")) {
						ListIterator<PeerAdvertisement> lipa = namenodepeers.listIterator();
						List<PeerID> lpid = new ArrayList<PeerID>();
						while(lipa.hasNext()) {
							lpid.add(lipa.next().getPeerID());
						}
						
						if(!lpid.contains(adv.getPeerID())) {
							LOG.debug("Found a namenode at "+adv.getPeerID());
							
							if(!namenodepeers.contains(adv))
									namenodepeers.add(adv);
								
							
							ds.getRemoteAdvertisements(adv.getPeerID().toString(), DiscoveryService.ADV,  PipeAdvertisement.NameTag, P2PConstants.RPCPIPENAME, 1,this);
						}
					} 
				}
			}
		} 			
		
		//if(namenodepeers.size() > 1) {
		//		LOG.fatal("*** FATAL *** There are more than 2 namenodes in the default RPC peergroup. That MUST NOT happen for now");
		//}
	}
	
	public JxtaSocket getRpcSocket() throws IOException {
		JxtaSocket js = null;
		
		if(jsad == null) 
			throw new IOException("No namenode address found; Aborting");
				
		int soTimeout = Integer.parseInt(pc.get("hadoop.p2p.rpc.timeout"));
		
		try {
			js = new JxtaSocket(jsad,soTimeout,true);
			js.setTcpNoDelay(true);
		} catch (SocketTimeoutException ste) {
			LOG.warn("RPC socket timeout");
		}
				
		return js;
	}
	
	public JxtaSocketAddress getRpcSocketAddress() throws IOException {
		return jsad;
	}	
	
	public JxtaSocketAddress getRpcSocketAddress(String pid) throws IOException {
		return getRpcSocketAddress(Peer.getPeerID(pid));
	}
	
	public JxtaSocketAddress getRpcSocketAddress(PeerID pid) throws IOException {
		PeerAdv.Instantiator pai = new PeerAdv.Instantiator();
		PeerAdvertisement peerAdv = (PeerAdvertisement) pai.newInstance();
		
		peerAdv.setPeerID(pid);
		peerAdv.setPeerGroupID(npg.getPeerGroupID());
		
		return new JxtaSocketAddress(npg, rpcPipeAdv, peerAdv);
	}	

	public JxtaSocketAddress getInfoSocketAddress(String pid) throws IOException {
		return getInfoSocketAddress(Peer.getPeerID(pid));
	}
	
	public JxtaSocketAddress getInfoSocketAddress(PeerID pid) throws IOException {
		PeerAdv.Instantiator pai = new PeerAdv.Instantiator();
		PeerAdvertisement peerAdv = (PeerAdvertisement) pai.newInstance();
		
		peerAdv.setPeerID(pid);
		peerAdv.setPeerGroupID(npg.getPeerGroupID());
		
		LOG.debug("peerAdv : "+peerAdv.getDocument(MimeMediaType.XMLUTF8));
		return new JxtaSocketAddress(npg, infoPipeAdv, peerAdv);
	}	
	
	public JxtaSocket getRpcSocket(String pid) throws IOException {
		return getRpcSocket(Peer.getPeerID(pid));
	}
	
	public JxtaSocket getRpcSocket(PeerID pid) throws IOException {
		JxtaSocket js = null;
						
		int soTimeout = Integer.parseInt(pc.get("hadoop.p2p.rpc.timeout"));
		
		try {
			js = new JxtaSocket(this.getRpcSocketAddress(pid),soTimeout,true);
			js.setTcpNoDelay(true);
		} catch (SocketTimeoutException ste) {}
				
		return js;
	}
	
	public JxtaSocket getInfoSocket(String pid) throws IOException {
		return getInfoSocket(Peer.getPeerID(pid));
	}
	
	public JxtaSocket getInfoSocket(PeerID pid) throws IOException {
		JxtaSocket js = null;
						
		int soTimeout = Integer.parseInt(pc.get("hadoop.p2p.rpc.timeout"));

		LOG.debug("pid : "+pid);
		LOG.debug("soTimeout : "+soTimeout);
		LOG.debug("pipeAdv : "+this.getInfoSocketAddress(pid).getPipeAdv());
		
		int retry = 3;
		
		while (retry >0) {
			try {
				js = new JxtaSocket(npg,pid,this.getInfoSocketAddress(pid).getPipeAdv(),soTimeout,true);
				js.setTcpNoDelay(true);
				//js.setSendBufferSize(P2PConstants.JXTA_SOCKET_SENDBUFFER_SIZE);
				//js.setReceiveBufferSize(P2PConstants.JXTA_SOCKET_RECVBUFFER_SIZE);
				// js.setRetryTimeout(1000);
			} catch (SocketTimeoutException ste) {
				ste.printStackTrace();
			}
			
			if(!js.isClosed())
				break;
			else 
				LOG.warn("Jxta INFO Socket is closed; Retrying");
			
			retry--;
		}
		
		if(retry==0)
			throw new IOException("Cannot get a Jxta INFO socket to peer "+pid);
		
		return js;
	}
	
	public PeerID getNameNodePeerId() {
		return namenodepeers.get(0).getPeerID();
	}

	public boolean hasLocalDatanode() {
		File dnPeerDirectory = new File(p2pdir,"../cert");
		
		if (!dnPeerDirectory.exists())
			return false;
		
		File[] CRTfile = dnPeerDirectory.listFiles(new CRTFilter());
		
		if (CRTfile.length == 1) 
			return true;
		
		return false;
	}
	
	public String getLocalDatanodePeerID() {
		PeerID dnpid = null;
		
		File dnPeerDirectory = new File(p2pdir,"../cert");
		
		File[] CRTfile = dnPeerDirectory.listFiles(new CRTFilter());
		
		if (CRTfile.length != 1)
			return null;
		
		return FileSystemUtils.getFilenameWithoutExtension(CRTfile[0].getName()).replace("cbid-", "");
	}
}
