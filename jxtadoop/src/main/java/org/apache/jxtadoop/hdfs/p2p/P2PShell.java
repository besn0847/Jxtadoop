package org.apache.jxtadoop.hdfs.p2p;

import java.io.IOException;

import javax.security.auth.login.LoginException;
import javax.security.cert.CertificateException;

import net.jxta.exception.PeerGroupException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.jxtadoop.conf.Configuration;
import org.apache.jxtadoop.conf.Configured;
import org.apache.jxtadoop.util.Tool;
import org.apache.jxtadoop.util.ToolRunner;

public class P2PShell extends Configured implements Tool {
	static{
	    Configuration.addDefaultResource("hdfs-p2p.xml");
	}

	private P2PClientPeer p2ppeer;
	public static final Log LOG = LogFactory.getLog(P2PShell.class);
	
	public P2PShell() {
		    // this(null);
		this (new Configuration());
	}
	
	  public P2PShell(Configuration conf) {
		    super(conf);
		    LOG.debug("Initializing P2P shell");
		    this.p2ppeer = null;
	  }
	  
	  private static void printUsage(String cmd) {
		    System.out.println("Usage: java "+P2PShell.class.getCanonicalName());
		    System.out.println("           [-namenodes]");
		    System.out.println("           [-datanodes]");
		    System.out.println("           [-node <peer id>]");
		    System.out.println("           [-advertisements]");
		    System.out.println("           [-help [cmd]]");
		    System.out.println();
	  }
	  
	  private void printHelp(String cmd) {
		  String summary = "Hadoop p2p is the command to execute p2p commands.";
		  summary += "\n\nThe full syntax is hadoop p2p [-namenodes] [-datanodes] [-node <peer id>] [-advertisements]  [-help [cmd]]\n";
		  
		  String namenodes ="-namenodes : Triggers a namenode discovery in the peer-to-peer layer.\n" + 
				  "\t All the namenodes will be discovered and their UID returned.\n" +
				  "\t Example : hadoop p2p -namenodes\n" +
				  "\t Result : \n" +
				  "\t\tFound 1 namenode(s) in the cloud - Details : \n" +
				  "\t\t\tPID : urn:jxta:cbid-59616261646162614E504720503250331724680CC86082CFD8E03154A2DD1E2E03\n";
		  
		  String datanodes = "-datanodes : Triggers a datanode discovery in the peer-to-peer layer.\n" + 
				  "\t All the datanodes will be discovered and their UID returned.\n" +
				  "\t Example : hadoop p2p -datanodes\n" +
				  "\t Result : \n" +
				  "\t\tFound 2 datanode(s) in the cloud - Details :\n" +
				  "\t\t\tPID : urn:jxta:cbid-59616261646162614E50472050325033C830997D87BF33CFBFA6B9C6830D754603\n" +
				  "\t\t\tPID : urn:jxta:cbid-59616261646162614E504720503250334F6155F383101382F8E96334D23550A203\n"
				  ;
		  
		  String node = "-node <peerid> : Triggers the discovery in the peer-to-peer layer to get the peer network details.\n" + 
				  "\t The IP address and the TCP port will be returned\n" +
				  "\t Example : hadoop p2p -node urn:jxta:cbid-59616261646162614E50472050325033C830997D87BF33CFBFA6B9C6830D754603\n" +
				  "\t Result : \n" +
				  "\t\tNode Peer ID : urn:jxta:cbid-59616261646162614E50472050325033C830997D87BF33CFBFA6B9C6830D754603\n" +
				  "\t\t\tName	: NAMENODE - rootDatanode Peer\n" +
				  "\t\t\tDesc	: Created by NetworkManager\n" +
				  "\t\t\tPeer Group ID : urn:jxta:jxta-NetGroup\n" +
				  "\t\t\tEndpoint : tcp://10.0.0.113:19101\n";
		  
		  String advertisements = "-advertisements : Triggers the discovery of all advertisements at the peer-to-peer layer.\n" +
				  "\t All the advertisements get returned except peers and groups advertisements\n" +
				  "\t Example : hadoop p2p -advertisements\n" +
				  "\t Result : \n" + 
				  "\t\tFound 11 advertisement(s) in the cloud - Details :\n" +
				  "\t\t---------------------------------------------------------------\n" +
				  "\t\tjxta:RA : \n" +
				  "\t\t  DstPID : urn:jxta:cbid-59616261646162614A787461503250333A92869570891B5A6C2254DC84D19B6003\n" +
				  "\t\t  Dst : \n" +
				  "\t\t    jxta:APA :\n" + 
				  "\t\t      EA : tcp://10.0.0.113:19103\n" +
				  "\t\t      EA : jxtatls://cbid-59616261646162614A787461503250333A92869570891B5A6C2254DC84D19B6003\n" +
				  "\t\t      EA : relay://cbid-59616261646162614A787461503250333A92869570891B5A6C2254DC84D19B6003\n" +
				  "\t\t  Hops : \n" +
				  "\t\t    jxta:APA :\n" + 
				  "\t\t      PID : urn:jxta:cbid-59616261646162614E504720503250331724680CC86082CFD8E03154A2DD1E2E03\n" +
				  "\t\t      EA : tcp://0.0.0.0:19100\n" +
				  "\t\t      EA : jxtatls://cbid-59616261646162614E504720503250331724680CC86082CFD8E03154A2DD1E2E03\n" +
		  	      ".......\n";
		  
		  if ("namenodes".equals(cmd)) {
		      System.out.println(namenodes);
		    } else if ("datanodes".equals(cmd)) {
		      System.out.println(datanodes);
		    } else if ("node".equals(cmd)) {
			   System.out.println(node);
		    } else if ("advertisements".equals(cmd)) {
			    System.out.println(advertisements);
		    } else {
		    	System.out.println(summary);
		    	System.out.println(namenodes);
		    	System.out.println(datanodes);
		    	System.out.println(node);
		    	System.out.println(advertisements);
		    }
	  }
	  
	  protected void init() throws IOException {
		  try {
			p2ppeer.setupNetworking();
			p2ppeer.startNetworking();
		} catch (LoginException | PeerGroupException | CertificateException e) {
			LOG.error(e.getMessage());
			throw(new IOException("P2P Connexion error"));
		}
	  }
	  
	@Override
	public int run(String[] args) throws Exception {
		 if (args.length < 1) {
		      printUsage(""); 
		      return -1;
		    }
		 
		 int exitCode = -1;
		 int i = 0;
		 String cmd = args[i++];
	      
	      exitCode = 0;
	      
	      if ("-help".equals(cmd)) {
	          if (i < args.length) {
	            printHelp(args[i]);
	          } else {
	            printHelp("");
	          }
	          return exitCode;
	        }
	      
	     if("-datanodes".equals(cmd) || "-namenodes".equals(cmd) ||  "-node".equals(cmd) ||  "-advertisements".equals(cmd)) {
	    	 this.p2ppeer = new P2PClientPeer("P2P - p2pshell"+ System.getProperty("jxtadoop.datanode.id"));
	     } else {
	    	 printUsage("");
	     }
	      
	      try { 
	    	  if ("-datanodes".equals(cmd)) {
	    		  init();
	    		  p2ppeer.discoverDatanodes();
	    	  } else if ("-namenodes".equals(cmd)) {
	    		  init();
	    		  p2ppeer.discoverNamenodes();
	    	  }  else if ("-advertisements".equals(cmd)) {
	    		  init();
	    		  p2ppeer.discoverAdvertisements();
	    	  } else if ("-node".equals(cmd)) {
	    		  if (args.length != 2) {
	    			  printHelp("");
	    			  return exitCode;
	    		  } else {
	    			  String peerid = args[i++];
	    			  if(peerid.startsWith("urn:jxta:cbid-")) {
	    				  init();
	    				  p2ppeer.discoverNode(peerid);
	    			  } else {
	    				  printHelp("");
		    			  return exitCode;
	    			  }
	    		  }
	    	  }
	      } catch (IOException ioe) {
	    	  System.err.println("Failed to connect to P2P network");
	          exitCode = -1;
	      }
	      	      
	      return exitCode;
	}
	
	private void close() {
		if (p2ppeer != null) p2ppeer.stop();	
	}
	
	public static void main(String argv[]) throws Exception {
		P2PShell shell = new P2PShell();
		
	    int res = 0;
	    try {
	    	LOG.debug("Starting shell with cmd : "+argv);
	        res = ToolRunner.run(shell, argv);
	    } finally {
	    	LOG.debug("Closing shell");
	    	shell.close();
	    }
	    
	    System.exit(res);
	}

}
