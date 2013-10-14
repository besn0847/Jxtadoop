package org.apache.jxtadoop.hdfs.p2p;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateEncodingException;

import javax.security.auth.login.LoginException;
import javax.security.cert.CertificateException;

import org.apache.jxtadoop.conf.Configuration;

import net.jxta.exception.PeerGroupException;

public class VerifyDiscoveryPeer {
	static{
	    Configuration.addDefaultResource("hdfs-p2p.xml");
}
	
	DiscoveryPeer dp;
	
	public VerifyDiscoveryPeer () {
		dp = new DiscoveryPeer("DISCO - "+ System.getProperty("jxtadoop.datanode.id"));
	}
	
	public void init() throws PeerGroupException, IOException, LoginException, CertificateException, SecurityException, URISyntaxException, CertificateEncodingException, UnrecoverableKeyException, NoSuchProviderException, KeyStoreException, NoSuchAlgorithmException {
		dp.loadPeerId();
		dp.setupNetworking();
		dp.startNetworking();
	}
	
	public void discover() {
		dp.start();
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		System.out.println("Starting");
		VerifyDiscoveryPeer dp = new VerifyDiscoveryPeer();
		
		try {
			dp.init();
			dp.discover();
		} catch (PeerGroupException | IOException | LoginException | CertificateException e) {
			e.printStackTrace();
		} catch (CertificateEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnrecoverableKeyException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchProviderException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (KeyStoreException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
