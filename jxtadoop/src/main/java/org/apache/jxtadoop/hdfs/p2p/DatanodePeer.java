package org.apache.jxtadoop.hdfs.p2p;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.ListIterator;

import javax.security.auth.login.LoginException;

import net.jxta.discovery.DiscoveryEvent;
import net.jxta.discovery.DiscoveryListener;
import net.jxta.discovery.DiscoveryService;
import net.jxta.document.Advertisement;
import net.jxta.exception.PeerGroupException;
import net.jxta.impl.protocol.PeerAdv;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.jxtadoop.security.UserGroupInformation;

import net.jxta.peer.PeerID;
import net.jxta.platform.NetworkManager;
import net.jxta.protocol.DiscoveryResponseMsg;
import net.jxta.protocol.PeerAdvertisement;
import net.jxta.protocol.PipeAdvertisement;
import net.jxta.socket.JxtaServerSocket;
import net.jxta.socket.JxtaSocket;
import net.jxta.socket.JxtaSocketAddress;
/**
 * This class extends the peer one to specifiy the datanode peer.<br>
 * This kind of peer both communicates with namenode to collect instructions about what as to be performed<br>
 * and with other datanodes to request specific tasks such as block replication.<br>
 * <p>
 * The same setupNetworking() and start() methods are overriden.<br>
 * The discoveryEvent() method is also implemented to process global datanode peer discovery results.<br> 
 * 
 * @author Franck Besnard <franck@besnard.mobi>
 * @version 1.0
 * @since November, 2011
 * 
 */
public class DatanodePeer extends Peer implements DiscoveryListener {
	public static final Log LOG = LogFactory.getLog(DatanodePeer.class);
	/**
	 * Holds the list of RPC pipe advertisement.
	 * <br> There should only one as for now, only a 1-namenode configuration is supported.
	 */
	List<PipeAdvertisement> rpcpipeadvs;
	/**
	 * Constructor with the peer name unique ID. This is important for the peer ID and key generation.
	 * It also initialize the RPC advertisement and namenode lists. 
	 * @param s The peer unique name
	 */
	public DatanodePeer(String s) {
		super(s);
		
		rpcpipeadvs =  new ArrayList<PipeAdvertisement>();
		namenodepeers =  new ArrayList<PeerAdvertisement>();
	}
	/**
	 * This method set up the jxta peer network configuration.
	 * <br> The peer is configured as an Edge. The connexion to the centrale namenode rendez-vous is also set-up and intialized.
	 * @throws LoginException Thrown upon user login error
	 * @throws IOException Thrown upon failure to connect to the namenode
	 * @throws CertificateException Thrown if the peer cannot unlock the keystore
	 * @throws PeerGroupException Thrown if the peer cannot join the default net peergroup
	 */
	@Override
	public void setupNetworking() throws LoginException, IOException, javax.security.cert.CertificateException, PeerGroupException {
		nm = new NetworkManager(NetworkManager.ConfigMode.EDGE, UserGroupInformation.login(pc).getUserName()+"Datanode Peer",p2pdir.toURI());
		
		nm.setConfigPersistent(false);
		
		nc = nm.getConfigurator();
		
		
		//if (!nc.exists()) {
			nc.setTcpPort(Integer.parseInt(pc.get("hadoop.p2p.datanode.port", P2PConstants.RPCDATANODEPORT)));
			nc.setTcpEnabled(true);
			nc.setTcpIncoming(true);
			nc.setTcpOutgoing(true);
			nc.setHttpEnabled(false);
			nc.setHttp2Enabled(false);
			
			nc.setTcpStartPort(-1);
			nc.setTcpEndPort(-1);
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
	/**
	 * Start-up the datanode peer.
	 * <br>Triggers a remote discovery for DN and NN. Then it assigns a default Jxta socker adrress to the pipes in the peer group.
	 * <br>Finally it kicks off the peer monitor thread.
	 */
	@Override
	public void start() {		
		try {
			ds.getRemoteAdvertisements(null, DiscoveryService.PEER, "Name", "*Namenode Peer*", 1,this);
			ds.getRemoteAdvertisements(null, DiscoveryService.PEER, "Name", "*Datanode Peer*", 1,this);
		} catch(Exception e) {
			e.printStackTrace();
		}		
		
		jssad = new JxtaSocketAddress(npg,rpcPipeAdv,npg.getPeerAdvertisement());
		infojssad = new JxtaSocketAddress(npg,infoPipeAdv,npg.getPeerAdvertisement());
		
		Thread monitor = new Thread(this.new PeerMonitor(this),"Peer Monitor Thread");
		monitor.start();
	}
	/**
	 * Manages the pipe, namenode and peer advertisement discovery events. 
	 */
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
					} else if((adv.getName()).contains("Datanode Peer") && adv.getPeerID() != pid) {
						synchronized(datanodepeers) {
							if (!datanodepeers.containsKey(adv.getPeerID())) {
								datanodepeers.put(adv.getPeerID(),adv);
							}
						}
					}
				}
			}
		} 			
		
		if(namenodepeers.size() > 1)
				LOG.fatal("*** FATAL *** There are more than 2 namenodes in the default RPC peergroup. That MUST NOT happen for now");
	}
	/**
	 * Return the jxta socket connected to the namenode rpc server
	 * @return The NN RPC server jxta socket
	 * @throws IOException
	 */
	public JxtaSocket getRpcSocket() throws IOException {
		JxtaSocket js = null;
		
		if(jsad == null) 
			throw new IOException("No namenode address found; Aborting");
				
		int soTimeout = Integer.parseInt(pc.get("hadoop.p2p.rpc.timeout"));
		
		try {
			js = new JxtaSocket(jsad,soTimeout,true);
			js.setTcpNoDelay(true);
		} catch (SocketTimeoutException ste) {}
				
		return js;
	}
	/**
	 * Return the jxta socket address of the namenode rpc server
	 * @return The NN RPC server jxta socket address
	 * @throws IOException
	 */
	public JxtaSocketAddress getRpcSocketAddress() throws IOException {
		return jsad;
	}	
	/**
	 * Return the jxta socket address of the datanode rpc server corresponding to the peer advertisement.
	 * @param peeradv The peer advertisement where to set-up the RPC connection
	 * @return The Jxta socket address of the remote datanode RPC Server
	 */
	public JxtaSocketAddress getDNRpcSocketAddress(PeerAdvertisement peeradv) {
		return new JxtaSocketAddress(npg, rpcpipeadvs.get(0),peeradv);
	}
	
	public JxtaSocketAddress getInfoSocketAddress(String pid) throws IOException {
		return getInfoSocketAddress(Peer.getPeerID(pid));
	}
	
	public JxtaSocketAddress getInfoSocketAddress(PeerID pid) throws IOException {
		PeerAdv.Instantiator pai = new PeerAdv.Instantiator();
		PeerAdvertisement peerAdv = (PeerAdvertisement) pai.newInstance();
		
		peerAdv.setPeerID(pid);
		peerAdv.setPeerGroupID(npg.getPeerGroupID());
		
		return new JxtaSocketAddress(npg, infoPipeAdv, peerAdv);
	}	
	
	public JxtaSocket getInfoSocket(String pid) throws IOException {
		return getInfoSocket(Peer.getPeerID(pid));
	}
	
	public JxtaSocket getInfoSocket(PeerID pid) throws IOException {
		JxtaSocket js = null;
						
		int soTimeout = Integer.parseInt(pc.get("hadoop.p2p.rpc.timeout"));
		
		try {
			// js = new JxtaSocket(this.getInfoSocketAddress(pid),soTimeout,true);
			js = new JxtaSocket(npg,pid,this.getInfoSocketAddress(pid).getPipeAdv(),soTimeout,true);
			js.setNetPeerGroup(npg);
			js.setTcpNoDelay(true);
		} catch (SocketTimeoutException ste) {}
				
		return js;
	}
}
