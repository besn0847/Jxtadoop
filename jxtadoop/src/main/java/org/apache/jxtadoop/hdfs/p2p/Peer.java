package org.apache.jxtadoop.hdfs.p2p;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;

import javax.security.auth.login.LoginException;

import net.jxta.discovery.DiscoveryEvent;
import net.jxta.discovery.DiscoveryListener;
import net.jxta.discovery.DiscoveryService;
import net.jxta.document.Advertisement;
import net.jxta.document.AdvertisementFactory;
import net.jxta.exception.PeerGroupException;
import net.jxta.id.IDFactory;
import net.jxta.impl.membership.pse.FileKeyStoreManager;
import net.jxta.impl.membership.pse.PSEUtils;
import net.jxta.impl.protocol.PeerAdv;
import net.jxta.peer.PeerID;
import net.jxta.peergroup.PeerGroup;
import net.jxta.peergroup.PeerGroupID;
import net.jxta.pipe.PipeID;
import net.jxta.pipe.PipeService;
import net.jxta.platform.NetworkConfigurator;
import net.jxta.platform.NetworkManager;
import net.jxta.protocol.DiscoveryResponseMsg;
import net.jxta.protocol.PeerAdvertisement;
import net.jxta.protocol.PipeAdvertisement;
import net.jxta.socket.JxtaSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.jxtadoop.conf.Configuration;
import org.apache.jxtadoop.security.UserGroupInformation;

/**
 * The parent class for the peer nodes. 
 * This includes the following objects :<br>
 * <table>	
 * <tr><td valign=top>NetworkManager</td><td>can operate either as an Edge peer (datanode) or as a Rendez-Vous/Relay peer (namenode)<br> 
 * </td></tr><tr><td valign=top>NetworkConfigurator</td><td>defines all the required items to start the networking part including the key management<br>
 * 													The default peer group is the default network one. This will be enhanced in the future with a secured peer group.<br> 
 * 													In this version, there is no encryption for the comms between peers so the key management is limited to the local peer.<br>
 * 													PKI will be required in that enhanced version.<br>
 * </td></tr><tr><td valign=top>JxtaSocketAddress</td><td>1 or 2 socket addresses are available depending on the peer type<br>
 *   												Namenode	-> 1 to process the RPC requests from the datanodes<br>
 *   												Datanode		-> 2 : 1 to send the RPC requests to the namanode + 1 to process the ones from the other datanodes<br>
 * </td></tr><tr><td valign=top>PipeAdvertisement</td><td>The default RPC pipe advertisement to be propagated by the namenode in the peer group<br>
 *   												The RPC pipes on the datanodes will use the *SAME* advertisement for sake of simplicity<br>
 * </td></tr><tr><td valign=top>DiscoveryService</td><td>This service is only available for the datanodes which will maintain a datanode topology map.<br>
 *   												The namenode will maintain this map using the events from the Rendez-Vous listener.<br>
 * </td></tr><tr><td valign=top>HashMap</td><td>This maps keeps the mapping for the datanodes between the PeerIDs and the PeerAdvertisement;<br>
 *   												For the namenode, the advertisement will always be <i>null</i> since there is no connection FROM% the NN to the DN<br>
 *  </td></tr><tr><td valign=top>List</td><td>This keep track for the datanode of the namenode peers. In the current version, only one NN is supported until NN clustering is supported.<br>
 *  </td></tr></table> 
 *  
 * @author Franck Besnard <franck@besnard.mobi>
 * @version 1.0
 * @since November, 2011
 */
public abstract class Peer implements P2PConstants {
	public static final Log LOG = LogFactory.getLog(Peer.class);
	
	boolean running = true;
	
	/**
	 * The peer configuration
	 */
	protected Configuration pc;
	/**
	 * The peer network manager
	 */
	protected NetworkManager nm;
	/**
	 * The peer network configurator
	 */
	protected NetworkConfigurator nc;
	/**
	 * The default net peer group
	 */
	protected PeerGroup npg;
	/**
	 * The peer identifier
	 */
	protected static PeerID pid;
	/**
	 * The rpc pipe identifier
	 */
	protected static PipeID ppid;
	/**
	 * The info pipe identifier
	 */
	protected static PipeID infopid;
	/**
	 * All the jxta config is sorted there
	 */
	protected File p2pdir;
	/**
	 * The pipe advertisement for the RPC service
	 */
	protected PipeAdvertisement rpcPipeAdv;
	/**
	 * The pipe advertisement for the INFO service (aka DN to DN transfer)
	 */
	protected PipeAdvertisement infoPipeAdv;
	/**
	 * The discovery service mainly used by the datanodes
	 */
	protected DiscoveryService ds;
	private String peerseed;
	/**
	 * The socket address for the local RPC server (NN + DN)
	 */
	protected JxtaSocketAddress jssad;
	/**
	 * The socket address for the local INFO server (DN)
	 */
	protected JxtaSocketAddress infojssad;
	/**
	 * The socket address for the remote RPC server (DN)
	 */
	protected JxtaSocketAddress jsad;
	/**
	 * The socket address for the remote INFO server (DN)
	 */
	protected JxtaSocketAddress infojsad;
	/**
	 * 	The peer certificate
	 */
	protected X509Certificate x509c;
	/**
	 * The peer private key
	 */
	protected PrivateKey privkey;
	/**
	 *  The directory where to store certificates
	 **/
	protected File CertificateDirectory;
	/**
	 * The keystore directory
	 */
	protected File KeyStoreFile;
	/**
	 * The directory where to store the other peers certificate 
	 */
	protected File PeersDirectory; 
	/**
	 * The password used to protect the keystore
	 */
	protected String p2ppass;
	/**
	 * The peer keystore
	 */
	protected KeyStore ks;
	/**
	 * The keystore manager
	 */
	protected FileKeyStoreManager fksm;
	/**
	 * The list of datanode peers in the cloud with their advertisement		
	 */
	protected HashMap<PeerID,PeerAdvertisement> datanodepeers;
	/**
	 * The list of namenode peers in the cloud (for now, there should only be one)
	 */
	protected List<PeerAdvertisement> namenodepeers;
	/**
	 * Constructor with the peer name unique ID. This is important for the peer ID and key generation. 
	 * @param s The peer unique name
	 */
	public Peer(String s) {
		this(s,new Configuration());
	}
	
	public Peer(String s, String p) {
		this(s,new Configuration(),p);
	}
	/**
	 * Constructor with the peer name unique ID and the configuration to be used. This is important for the peer ID and key generation.
	 * @param s The peer unique name
	 * @param c The configuration to be used
	 */
	public Peer(String s, Configuration c) {
		this(s,new Configuration(),"");
	}
	
	public Peer(String s, Configuration c, String p) {
		System.setProperty("net.jxta.endpoint.WireFormatMessageFactory.CBJX_DISABLE", "true");
		
		this.peerseed = s;
		// init config
		pc = c;

		datanodepeers =  new HashMap<PeerID,PeerAdvertisement>();
		
		// setr rpc pipe id
		try {
			ppid=(PipeID) IDFactory.fromURI(new URI(P2PConstants.RPCPIPEID));
			infopid=(PipeID) IDFactory.fromURI(new URI(P2PConstants.INFOPIPEID));
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
		
		// create p2p secure dir
		p2pdir = new File(pc.get("hadoop.p2p.dir"));
		if(!p.equals("")) p2pdir = new File(p2pdir,p);
				
		if(!p2pdir.exists()) {
			LOG.debug("Creating P2P directory structure at "+pc.get("hadoop.p2p.dir"));
			p2pdir.mkdirs();
		}
		
		File cm = new File(p2pdir,"cm");
		if (cm.exists()) {
			LOG.debug("Suppressing existing CM directory");
			cm.delete();
		}
			
		CertificateDirectory = new File(p2pdir,"cert");
		KeyStoreFile = new File(p2pdir,"keystore");
		
		if(!CertificateDirectory.exists()) CertificateDirectory.mkdirs();	
	}
		
	/**
	 * Initialize the peer.<br>
	 * <br>
	 * <p>1 Loading the peer id from the certificate file name<br>
	 * <p>2 Load the key store with the peer certificate and private key<br>
	 * <p>3 Set up the networking (specific to DN and NN) and gather the peer group<br>
	 * <p>4 Publish the peer  advertisement on the peer group<br>
	 * <p>5 Gather the RPC pipe advertisement to be use to set up the servers and the sockets<br>
	 * @throws SecurityException
	 */
	public void initialize() throws SecurityException{
		// loading the pid, if not yet generated, create a new one
		try {
			
			loadPeerId();
			
			loadKeyStoreManager();
			
			setupNetworking();
			
			purgePeerAdvertisements();
						
			publishPeerAdvertisement();
			
			rpcPipeAdv = this.setPipeAdvertisement(ppid);
			
			infoPipeAdv = this.setInfoPipeAdvertisement(infopid);			
		} catch (Exception e) {
			LOG.error("Failed to initialize the p2p environment; Aborting");
			e.printStackTrace();
			throw new RuntimeException();
		} 
		
		
	}
	/**
	 * To be defined for each child
	 */
	
	public void start() {
		// to be overriden
	}
	/**
	 * Load the peer id from the certificate file name or generate a new one using the see and the user login name
	 * @throws LoginException Thrown upon user login error
	 * @throws URISyntaxException Thrown upon file name wrong format
	 * @throws SecurityException Thrown if the peer id cannot be define
	 */
	
	protected void loadPeerId() throws LoginException, URISyntaxException, SecurityException {
		File[] CRTfile = CertificateDirectory.listFiles(new CRTFilter());
		
		 if (CRTfile.length == 0 ) {
			 pid = IDFactory.newPeerID(PeerGroupID.defaultNetPeerGroupID, (UserGroupInformation.login(pc).getUserName()+this.peerseed).getBytes());
			 
		 } else if (CRTfile.length == 1) {
			pid = (PeerID) IDFactory.fromURI(URI.create("urn:jxta:"+FileSystemUtils.getFilenameWithoutExtension(CRTfile[0].getName())));
		 } else {
			throw new SecurityException();
		 }
	}
	/**
	 * Load the keystore manager and the keystore needed for the access membership of the default net peergoup.
	 * <br> The passwords for the keystore and the private key have to be the same.<br>
	 * This will be enhanced in the future with secured peer group + public key infrastructure.
	 * @throws NoSuchProviderException
	 * @throws KeyStoreException
	 * @throws IOException
	 * @throws CertificateEncodingException
	 * @throws UnrecoverableKeyException
	 * @throws NoSuchAlgorithmException
	 */
	
	protected void loadKeyStoreManager() throws NoSuchProviderException, KeyStoreException, IOException, CertificateEncodingException, UnrecoverableKeyException, NoSuchAlgorithmException {
		p2ppass = pc.get("hadoop.p2p.password");
		
		fksm = new FileKeyStoreManager((String)null, P2PConstants.RPCKEYSTOREPROVIDER, KeyStoreFile);
		
		if (!fksm.isInitialized()) {
			fksm.createKeyStore(p2ppass.toCharArray());
			
			PSEUtils.IssuerInfo ForPSE = PSEUtils.genCert("WorldPeerGroup", null);
			x509c = ForPSE.cert;
			privkey = ForPSE.issuerPkey;
			
			ks = fksm.loadKeyStore(p2ppass.toCharArray());
			
			X509Certificate[] Temp = { x509c };
			ks.setKeyEntry(pid.toString(), privkey, p2ppass.toCharArray(), Temp);
			
			fksm.saveKeyStore(ks, p2ppass.toCharArray());
			
			FileOutputStream cfos = new FileOutputStream(new File(CertificateDirectory,pid.getUniqueValue().toString()+".crt"));
			cfos.write(x509c.getEncoded());
			cfos.close();
			
			FileOutputStream kfos = new FileOutputStream(new File(CertificateDirectory,pid.getUniqueValue().toString()+".key"));
			kfos.write(privkey.getEncoded());
			kfos.close();
		} else {
			ks = fksm.loadKeyStore(p2ppass.toCharArray());
    		x509c = (X509Certificate) ks.getCertificate(pid.toString());
    		privkey = (PrivateKey) ks.getKey(pid.toString(), p2ppass.toCharArray());
		}
	}
	/**
	 * To be defined for each child
	 */
	
	protected void setupNetworking() throws LoginException, IOException, javax.security.cert.CertificateException, PeerGroupException {
		// To be overriden
	}
		
	/**
	 * Purge the peer advertisement from the local cache
	 * @throws IOException Failed to open the local cache
	 */
	public void purgePeerAdvertisements() throws IOException {
		Enumeration<Advertisement> ea = ds.getLocalAdvertisements(DiscoveryService.PEER, "Name", "*Datanode Peer*");
		
		while(ea.hasMoreElements()) {
			ds.flushAdvertisement(ea.nextElement());
		}
	}
	
	/**
	 * Publish the peer advertisement in the peer group
	 * @throws IOException The publishing failed
	 */
	protected void publishPeerAdvertisement () throws IOException {		
		ds.publish(npg.getPeerAdvertisement());
		ds.remotePublish(npg.getPeerAdvertisement(),P2PConstants.PEERDELETIONRETRIES*P2PConstants.PEERDELETIONTIMEOUT);
	}
	/**
	 * Setup the rpc pipe advertisement in unsecured mode
	 * @param pid The pipe identifier (default : urn:jxta:uuid-CECDC14D1F334B0EB0A4A269F7A38C918F0350E117A049B89FC3D60D98BD07EF04 )
	 * @return The unique pipe id
	 */

	protected PipeAdvertisement setPipeAdvertisement(PipeID pid) {
		rpcPipeAdv = (PipeAdvertisement) AdvertisementFactory.newAdvertisement(PipeAdvertisement.getAdvertisementType());
        rpcPipeAdv.setPipeID(ppid);
        rpcPipeAdv.setType(PipeService.UnicastType);
        rpcPipeAdv.setName(P2PConstants.RPCPIPENAME);
        rpcPipeAdv.setDescription(P2PConstants.RPCPIPEDESC);
        
        return rpcPipeAdv;
	}
	/**
	 * Setup the info pipe advertisement in unsecured mode - Only used by the DataNode
	 * @param pid The pipe identifier (default : urn:jxta:uuid-CECDC14D1F334B0EB0A4A269F7A38C91496E666F20504970A5209365656404 )
	 * @return The unique pipe id
	 */

	protected PipeAdvertisement setInfoPipeAdvertisement(PipeID pid) {
		infoPipeAdv = (PipeAdvertisement) AdvertisementFactory.newAdvertisement(PipeAdvertisement.getAdvertisementType());
        infoPipeAdv.setPipeID(infopid);
        infoPipeAdv.setType(PipeService.UnicastType);
        infoPipeAdv.setName(P2PConstants.INFOPIPENAME);
        infoPipeAdv.setDescription(P2PConstants.INFOPIPEDESC);
                
        return infoPipeAdv;
	}
	/**
	 * Return the RPC pipe advertisement
	 * @return The pipe advertisement
	 */

	public PipeAdvertisement getPipeAdvertisement() {
		return this.rpcPipeAdv;
	}
	/**
	 * Return the INFO pipe advertisement
	 * @return The INFO pipe advertisement
	 */

	public PipeAdvertisement getInfoPipeAdvertisement() {
		return this.infoPipeAdv;
	}
	/**
	 * Return the peer identifier
	 * @return The peer identifier
	 */

	public PeerID getPeerID() {
		return pid;
	}
	/**
	 * Return the peer identifier as a string without the URN header
	 * @return The peer identifier without URN header
	 */

	public String getPeerIDwithoutURN() {
		return pid.toString().replaceAll("urn:jxta:cbid-", "");
	}
	/**
	 * Return the rpc pipe identifier
	 * @return The rpc pipe identifier
	 */
	
	public PipeID getRpcPipeID() {
		return ppid;
	}
	/**
	 * Return the INFO pipe identifier
	 * @return The INFO pipe identifier
	 */
	
	public PipeID getInfoPipeID() {
		return infopid;
	}
	/**
	 * Get the RPC server socket address.
	 * <br>This is valid both  for the namenodes and datanodes which runs an RPC server.
	 * @return The RPC server socket address
	 */
	public JxtaSocketAddress getServerSocketAddress() {
		return this.jssad;
	}
	/**
	 * Get the INFO server socket address.
	 * <br>This is valid only for the datanodes
	 * @return The INFO server socket address
	 */
	public JxtaSocketAddress getInfoServerSocketAddress() {
		return this.infojssad;
	}
	
	/**
	 * Get the net peergroup in which the peer is running
	 * @return The node net peergroup
	 */
	public PeerGroup getPeerGroup() {
		return npg;
	}
	/**
	 * Check if the datanode is currently in the cloud
	 * @param pid The datanode peer identifier
	 * @return True or false
	 */
	
	public synchronized boolean isPeerAlive(PeerID pid) {
		return datanodepeers.containsKey(pid);			
	}
	
	/**
	 * Returns the map of the datanodes currently in the cloud with their peer identifiers and advertisements.
	 * <br> This is needed to set up the sockets to connect to remote RPC servers.
	 * @return
	 */
	public synchronized HashMap<PeerID,PeerAdvertisement> getDatanodeList() { 
		return datanodepeers;
	};
	
	public static PeerID getPeerID(String pid) {
		try {
			if(!pid.startsWith("urn:jxta:cbid-"))
					pid = "urn:jxta:cbid-"+pid;
			return (PeerID) IDFactory.fromURI(URI.create(pid));
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
		return null;
	}
	/**
	 * Empty class to be overridden 
	 * 
	 * @param event
	 */
	public synchronized void fireEvent(DatanodeEvent event)	{
		// Do nothing
	}
	/**
	 * Thread used to maintain the map of datanodes in the Jxtadoop cloud.
	 * <br> For each datanode in the cloud, it requests the advertisement every Constants.PEERDELETIONTIMEOUT.
	 * <br> If after Constants.PEERDELETIONRETRIES retries, there is no advertisement retrieved, then the datanode is deleted from the map.
	 */
	
	protected class PeerMonitor extends Thread implements DiscoveryListener {
		/**
		 * The map of the datanodes which did not repsond with the number of retries.
		 */
		HashMap<PeerID,Integer> notrespondedpeers;
		/**
		 * The peer discovery listener which is <i>null</i> for the namenode and the peer discovery listener for the datanode. 
		 */
		DiscoveryListener dnlist;
		
		/**
		 * This constructor is used for the namenode aka there is no discovery listener to be used.
		 */
		PeerMonitor() {
			this(null);
		}
		/**
		 * This constructor is used for the datanode.
		 * @param dl The discovery listener of the datanode peer
		 */
		PeerMonitor(DiscoveryListener dl) {
			notrespondedpeers = new HashMap<PeerID,Integer>();
			this.dnlist = dl;
		}
		/**
		 * For each datanode in the cloud, a peer discovery is performed.
		 * <br> If the datanode responded, then it is removed from the map.
		 * <br> If not, then after X retries, it is deleted from the datanode peer map and the local advertisement is also remove.
		 * <br> This is important in non-multicast configuration, otherwise every re-discovery the peer will re-appear.
		 *  <br><br> Then a global peer discovery is retriggered for the datanodes in the cloud. 
		 */
		public void run() {
			LOG.info("Starting the peer monitor");
			Object[] pidkeys;
			int pidkeyscount, increment;
			PeerID lpid;
			
			while(running) {		
				try {
					Thread.sleep(P2PConstants.PEERDELETIONTIMEOUT);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				
				synchronized(datanodepeers) {
					pidkeys = notrespondedpeers.keySet().toArray();
					pidkeyscount = notrespondedpeers.size();
					increment = 0;
					
					/* Description :
					 * 		1. Get the list of peers that did not respond
					 * 		2. If the max retry count has been reached, then just increase the retry count
					 * 		2. Else remove the peer from the not responded and datanodes in the cloud list
					*/
					while (increment < pidkeyscount ) {
						lpid = (PeerID) pidkeys[increment];
						increment++;
						int attempt = notrespondedpeers.get(lpid);
												
						if(attempt < P2PConstants.PEERDELETIONRETRIES) {
							attempt++;
							notrespondedpeers.remove(lpid);
							notrespondedpeers.put(lpid, new Integer(attempt));
						} else {
							LOG.info("Peer not responding for "+P2PConstants.PEERDELETIONRETRIES+" retries; Assuming dead : "+lpid);
							fireEvent(new DatanodeEvent(new Object(),lpid));
							notrespondedpeers.remove(lpid);
							try {
								Enumeration<Advertisement> ea =ds.getLocalAdvertisements(DiscoveryService.PEER,"PID", lpid.toString());
								while(ea.hasMoreElements()) {
									ds.flushAdvertisement(ea.nextElement());
								}
							} catch (IOException e) {
								e.printStackTrace();
							}
							datanodepeers.remove(lpid);
							LOG.info("Number of datanodes in the cloud : "+datanodepeers.size());
						}
					}
						
					pidkeys = datanodepeers.keySet().toArray();
					pidkeyscount = datanodepeers.size();
					increment = 0;
						
					/* Description:
					 * 		1. From the datanode in the cloud list, trigger a peer discovery in each dn
					 * 		2. If the peer is not in the not responded list, add it with a count of 1
					 * 
					 * Note : The DFSClients and DFSAdmin will automatically get removed.
					 */
					while (increment < pidkeyscount ) {
						lpid = (PeerID) pidkeys[increment];
						ds.getRemoteAdvertisements(lpid.toString(), DiscoveryService.PEER, "Name", "*Datanode Peer*",P2PConstants.MAXCLOUDPEERCOUNT,this);
						increment++;
							
						if(!notrespondedpeers.containsKey(lpid)) {
							LOG.debug("Adding peer to not responded list");
							notrespondedpeers.put(lpid, new Integer(1));
						}
					}	
				}
				
				/*
				 * If the peer discovery listener is not null, then this is a datanode is not null
				 * Then trigger a datanode peer discovery in the cloud.
				 * The threshold is set to a high value (here 10,000) since for a non-multicast situation,
				 * the rendez-vous will return hundreds of advertisement.				
				 */
				if(ds!=null) {
					ds.getRemoteAdvertisements(null, DiscoveryService.PEER, "Name", "*Datanode Peer*",P2PConstants.MAXCLOUDPEERCOUNT,dnlist);
					try {
						publishPeerAdvertisement();
					} catch (Exception e) {}
				}
			}
		}
		/**
		 * Remove the peer for the non-responded map if the advertisement is received.
		 */
		@Override
		public void discoveryEvent(DiscoveryEvent event) {
			DiscoveryResponseMsg response = event.getResponse();
						
			if (response.getDiscoveryType() == DiscoveryService.PEER) {
				Enumeration<Advertisement> en = response.getAdvertisements();
				PeerAdv adv;
				 
				/*
				 * If this is a datanode peer, then remove the associated entry from the not-responded list
				 * as the datanode replied to the remote discovery
				 */
				while (en.hasMoreElements()) {
					adv = (PeerAdv) en.nextElement();
					if (adv instanceof PeerAdv) {
						if ((adv.getName()).contains("Datanode Peer")) {
							// LOG.info("Datanode peer discovered : "+adv.getPeerID());
							if(notrespondedpeers.containsKey(adv.getPeerID())) {
								notrespondedpeers.remove(adv.getPeerID());
							}
						}				
					}
				}
			}
		}
		
	}
}
