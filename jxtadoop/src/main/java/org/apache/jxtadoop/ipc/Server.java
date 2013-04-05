package org.apache.jxtadoop.ipc;


import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import java.nio.ByteBuffer;

import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;

import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import javax.security.auth.Subject;

import net.jxta.peer.PeerID;
import net.jxta.peergroup.PeerGroup;
import net.jxta.socket.JxtaServerSocket;
import net.jxta.socket.JxtaSocket;
import net.jxta.socket.JxtaSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.jxtadoop.conf.Configuration;
import org.apache.jxtadoop.security.SecurityUtil;
import org.apache.jxtadoop.io.Writable;
import org.apache.jxtadoop.io.WritableUtils;
import org.apache.jxtadoop.ipc.metrics.RpcMetrics;
import org.apache.jxtadoop.util.ReflectionUtils;
import org.apache.jxtadoop.util.StringUtils;
import org.apache.jxtadoop.security.authorize.AuthorizationException;

/** 
 * <b><font color="red">Class modified to use Jxta pipes</font></b><br><br>
 * An abstract IPC service.  IPC calls take a single {@link Writable} as a
 * parameter, and return a {@link Writable} as their value.  A service runs on
 * a port and is defined by a parameter class and a value class.
 * 
 * @see Client
 */
public abstract class Server {
  
  /**
   * The first four bytes of Hadoop RPC connections
   */
  public static final ByteBuffer HEADER = ByteBuffer.wrap("hrpc".getBytes());
  
  
  // 1 : Introduce ping and server does not throw away RPCs
  // 3 : Introduce the protocol into the RPC connection header
  public static final byte CURRENT_VERSION = 3;
  
  /**
   * How many calls/handler are allowed in the queue.
   */
  private static final int MAX_QUEUE_SIZE_PER_HANDLER = 100;
  /**
   * When the read or write buffer size is larger than this limit, i/o will be 
   * done in chunks of this size. Most RPC requests and responses would be
   * be smaller.
   */
  private static int NIO_BUFFER_LIMIT = 8*1024; //should not be more than 64KB.
  
  public static final Log LOG = LogFactory.getLog(Server.class);

  private static final ThreadLocal<Server> SERVER = new ThreadLocal<Server>();

  private static final Map<String, Class<?>> PROTOCOL_CACHE = 
    new ConcurrentHashMap<String, Class<?>>();
  
  static Class<?> getProtocolClass(String protocolName, Configuration conf) 
  throws ClassNotFoundException {
    Class<?> protocol = PROTOCOL_CACHE.get(protocolName);
    if (protocol == null) {
      protocol = conf.getClassByName(protocolName);
      PROTOCOL_CACHE.put(protocolName, protocol);
    }
    return protocol;
  }
  
  /** Returns the server instance called under or null.  May be called under
   * {@link #call(Writable, long)} implementations, and under {@link Writable}
   * methods of paramters and return values.  Permits applications to access
   * the server context.*/
  public static Server get() {
    return SERVER.get();
  }
 
  /** This is set to Call object before Handler invokes an RPC and reset
   * after the call returns.
   */
  private static final ThreadLocal<Call> CurCall = new ThreadLocal<Call>();
  
  /** Returns the remote side ip address when invoked inside an RPC 
   *  Returns null incase of an error.
   */
  public static InetAddress getRemoteIp() {
    Call call = CurCall.get();
    if (call != null) {
      return call.connection.socket.getInetAddress();
    }
    return null;
  }
  /** Returns remote address as a string when invoked inside an RPC.
   *  Returns null in case of an error.
   */
  public static String getRemoteAddress() {
    InetAddress addr = getRemoteIp();
    return (addr == null) ? null : addr.getHostAddress();
  }

  private String bindAddress; 
  private PeerGroup rpcpg;
  private SocketAddress p2pServerSockAddr;
  private JxtaSocketAddress jxtaServerSockAddr;
  
  private int port;                               // port we listen on
  private int handlerCount;                       // number of handler threads
  private Class<? extends Writable> paramClass;   // class of call parameters
  private int maxIdleTime;                        // the maximum idle time after 
                                                  // which a client may be disconnected
  private int thresholdIdleConnections;           // the number of idle connections
                                                  // after which we will start
                                                  // cleaning up idle 
                                                  // connections
  int maxConnectionsToNuke;                       // the max number of 
                                                  // connections to nuke
                                                  //during a cleanup
  
  protected RpcMetrics  rpcMetrics;
  
  private Configuration conf;

  private int maxQueueSize;

  volatile private boolean running = true;         // true while server runs
  private BlockingQueue<Call> callQueue; // queued calls

  private List<Connection> connectionList = 
    Collections.synchronizedList(new LinkedList<Connection>());
  
  //maintain a list
  //of client connections
  private Listener listener = null;
  private Responder responder = null;
  private int numConnections = 0;
  private Handler[] handlers = null;

  /**
   * A convenience method to bind to a given address and report 
   * better exceptions if the address is not a valid host.
   * @param socket the socket to bind
   * @param address the address to bind to
   * @param backlog the number of connections allowed in the queue
   * @throws BindException if the address can't be bound
   * @throws UnknownHostException if the address isn't a valid host name
   * @throws IOException other random errors from bind
   */
  public static void bind(ServerSocket socket, InetSocketAddress address, 
                          int backlog) throws IOException {
    try {
      socket.bind(address, backlog);
    } catch (BindException e) {
      BindException bindException = new BindException("Problem binding to " + address
                                                      + " : " + e.getMessage());
      bindException.initCause(e);
      throw bindException;
    } catch (SocketException e) {
      // If they try to bind to a different host's address, give a better
      // error message.
      if ("Unresolved address".equals(e.getMessage())) {
        throw new UnknownHostException("Invalid hostname for server: " + 
                                       address.getHostName());
      } else {
        throw e;
      }
    }
  }

  /** A call queued for handling. */
  private static class Call {
    private int id;                               // the client's call id
    private Writable param;                       // the parameter passed
    private Connection connection;                // connection to client
    private long timestamp;     // the time received when response is null
                                   // the time served when response is not null
    private ByteBuffer response;                      // the response for this call

    public Call(int id, Writable param, Connection connection) { 
      this.id = id;
      this.param = param;
      this.connection = connection;
      this.timestamp = System.currentTimeMillis();
      this.response = null;
    }
    
    @Override
    public String toString() {
      return param.toString() + " from " + connection.toString();
    }

    public void setResponse(ByteBuffer response) {
      this.response = response;
    }
  }

  /** Listens on the socket. Creates jobs for the handler threads*/
  private class Listener extends Thread {
    private ServerSocket acceptServer = null; //the accept server
    private JxtaServerSocket jxtAcceptServer = null; //same as above
    
    //private InetSocketAddress address; //the address we bind at
    private Random rand = new Random();
    private long lastCleanupRunTime = 0; //the last time when a cleanup connec-
                                         //-tion (for idle connections) ran
    private long cleanupInterval = 10000; //the minimum interval between 
                                          //two cleanup runs
    
    /*public Listener() throws IOException {
      address = new InetSocketAddress(bindAddress, port);
      
      // Create a new server socket
      acceptServer = new ServerSocket();

      // Bind the server socket to the local host and port
      acceptServer.bind(address);
      port = acceptServer.getLocalPort(); //Could be an ephemeral port

      this.setName("IPC Server listener on " + port);
      this.setDaemon(true);
    }*/
    
    public Listener( PeerGroup pg, JxtaSocketAddress jssa) throws IOException {
    	int soTimeout = Integer.parseInt(conf.get("hadoop.p2p.rpc.timeout"));
    	
		// LOG.debug("Net peergroup :"+jssa.getPeerGroupId());
		// LOG.debug("Peer id :"+jssa.getPeerId());
    	jxtAcceptServer = new JxtaServerSocket(pg, jssa.getPipeAdv(), Integer.parseInt(conf.get("hadoop.p2p.rpc.backlog")),soTimeout);
    	acceptServer = (ServerSocket)jxtAcceptServer;
		    	
        this.setName("IPC Server listener on " + port + " for namenode ..."+jssa.getPeerId().toString().substring(jssa.getPeerId().toString().length()-8));
        this.setDaemon(true);
    }
    
    /** cleanup connections from connectionList. Choose a random range
     * to scan and also have a limit on the number of the connections
     * that will be cleanedup per run. The criteria for cleanup is the time
     * for which the connection was idle. If 'force' is true then all 
     * connections will be looked at for the cleanup.
     */
    private void cleanupConnections(boolean force) {
      LOG.debug("Cleaning up RPC connections");
      
      if (force || numConnections > thresholdIdleConnections) {
        long currentTime = System.currentTimeMillis();
        if (!force && (currentTime - lastCleanupRunTime) < cleanupInterval) {
          return;
        }
        int start = 0;
        int end = numConnections - 1;
        
        if (!force) {
          start = rand.nextInt() % numConnections;
          end = rand.nextInt() % numConnections;
          int temp;
          if (end < start) {
            temp = start;
            start = end;
            end = temp;
          }
        }
        int i = start;
        int numNuked = 0;
        while (i <= end) {
          Connection c;
          synchronized (connectionList) {
          try {
              c = connectionList.get(i);
            } catch (Exception e) {return;}
          }
          if (c.timedOut(currentTime)) {
            if (LOG.isDebugEnabled())
              LOG.debug(getName() + ": disconnecting client " + c.getHostAddress());
            closeConnection(c);
            numNuked++;
            end--;
            c = null;
            if (!force && numNuked == maxConnectionsToNuke) break;
          }
          else i++;
        }
        lastCleanupRunTime = System.currentTimeMillis();
      }
    }

    @Override
    public void run() {
      LOG.debug("Methode : Listener run()");
      LOG.info(getName() + ": starting");
      SERVER.set(Server.this);
      
      while (running) {	        
    	 JxtaSocket s = null;
    	  
    	 try {
			s = (JxtaSocket) acceptServer.accept();
				
			if(s != null) {
				Connection c = new Connection(s, System.currentTimeMillis());
		        
				synchronized (connectionList) {
			          connectionList.add(numConnections, c);
			          numConnections++;
				}
			    
				if (LOG.isDebugEnabled())
			          LOG.debug("Server connection" +
			              "; # active connections: " + numConnections +
			              "; # queued calls: " + callQueue.size());
				
				Thread readthr = new Thread(new Reader(c),"Connection Reader Thread "+numConnections);
				readthr.start();
			}						
		}catch (OutOfMemoryError e) {
	          LOG.warn("Out of Memory in server select", e);
	          closeCurrentConnection(s, e);
	          cleanupConnections(true);
	          try { Thread.sleep(60000); } catch (Exception ie) {}    
		} catch (SocketTimeoutException e) {
				cleanupConnections(true); 
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			closeCurrentConnection(s, e);
        }
      }
      
      LOG.info("Stopping " + this.getName());

      synchronized (this) {
        try {
        	acceptServer.close();
        } catch (IOException e) { }
        
        acceptServer = null;
        
        synchronized(connectionList) {
        	while (!connectionList.isEmpty()) {
        		closeConnection(connectionList.remove(0));
        	}
        }
      }
    }
   
    private void closeCurrentConnection(Socket s, Throwable e) {
    	Connection c = null;
    	
    	if (s != null) {
    		synchronized(connectionList) {
	    		Iterator<Connection> itrcon = connectionList.iterator();
		    	 
		    	 while (itrcon.hasNext()) {
		    		 Connection cc = itrcon.next();
		    		 if (cc.getSocket().equals(s)) {
		    			 c = cc;
		    			 break;
		    		 }
		    	 }
		    	 
		        if (c != null) {
		          if (LOG.isDebugEnabled())
		            LOG.debug(getName() + ": disconnecting client ");
		          closeConnection(c);
		          c = null;
		        }
    		}
	      }
	    }
   
    JxtaSocketAddress getAddress() {
    	return (JxtaSocketAddress)jxtAcceptServer.getLocalSocketAddress();
    }
    
    synchronized void doStop() {
	      if (acceptServer != null ) {
	    	  try {
	    		  acceptServer.close();
	    	  } catch (IOException e) {
	    		  LOG.info(getName() + ":Exception in closing listener socket. " + e);
	    	  }
	      }
    }
  }

  // Sends responses of RPC back to clients.
  private class Responder extends Thread {
			final static int PURGE_INTERVAL = 900000;
			
		    Responder() throws IOException {
		      this.setName("IPC Server Responder");
		      this.setDaemon(true);
		    }

		    @Override
		    public void run() {
		      LOG.debug("Methode : responder - run()");
		      LOG.info(getName() + ": starting");
		      SERVER.set(Server.this);
		      
		      long lastPurgeTime = 0;

		      while (running) {
		    	  try {
						this.sleep(1000);
		    	  } catch (InterruptedException e1) {}
					
		          // Step 1 : Processing connection call queues
		    	  synchronized(connectionList) {
		        	try {
			        	for(int i=0; i < connectionList.size();i++) {
			        		Connection c = connectionList.get(i);
					        			
					       	if (c.responseQueue.size() > 0) {
					       		LOG.debug("Pending calls for connection "+c+" : "+c.responseQueue.size());
					       		processResponse(c.responseQueue, false);
					       	}
			        	}
		        	} catch (Exception e) {
		        		LOG.warn("Exception in Responder " + 
		        				StringUtils.stringifyException(e));
		        	}		        	
		        }
		        
		        // Step 2 : Purging call queues & old connections 
		        long now = System.currentTimeMillis();
		        if (now < lastPurgeTime + PURGE_INTERVAL) {
		            continue;
		        }
		        lastPurgeTime = now;
		        
		        synchronized(connectionList) {
		        	try {
		        		int purge =0;
		        		
			        	for(int i=0; i < connectionList.size();i++) {
			        		Connection c = connectionList.get(i);
			        		
			        		if (c.responseQueue.size() > 0) {
			        			LOG.debug("Response queue not empty for connection "+c);
			        			Call call;
			        			
				        		synchronized(c.responseQueue) {
				        			call = c.responseQueue.removeFirst();
				        	        
				        			if (now > call.timestamp + PURGE_INTERVAL) {
				        	            purge++;
				        				closeConnection(c);
				        	        }
				        		}
				        	} else if (now > c.lastContact + PURGE_INTERVAL) {
				        		purge++;
				        		closeConnection(c);
				        	}
			        	}
			        	
				        LOG.debug("Purged "+purge+" connections ");
				        
			        } catch (Exception e) {
			        	LOG.warn("Exception in Responder " + 
			        			StringUtils.stringifyException(e));
			        }
		        }
		      }
		      LOG.info("Stopping " + this.getName());
		    }

		    // Processes one response. Returns true if there are no more pending
		    // data for this channel.
		    //
		    private boolean processResponse(LinkedList<Call> responseQueue,
		                                    boolean inHandler) throws IOException {
		      // LOG.debug("Methode : responder - processResponse()");
		      boolean error = true;
		      boolean done = false;       // there is more data for this channel.
		      int numElements = 0;
		      Call call = null;
		      try {
		        synchronized (responseQueue) {
		          //
		          // If there are no items for this channel, then we are done
		          //
		          numElements = responseQueue.size();
		          if (numElements == 0) {
		            error = false;
		            return true;              // no more data for this channel.
		          }
		          //
		          // Extract the first call
		          //
		          call = responseQueue.removeFirst();
		          Socket remoteclient = call.connection.socket;
	  
		          //
		          // Send as much data as we can in the non-blocking fashion
		          //
		          BufferedOutputStream bos = new BufferedOutputStream(remoteclient.getOutputStream());
		          byte[] reponsemsg = call.response.array();
		          bos.write(reponsemsg,0,reponsemsg.length);
		          bos.flush();   
		          
		          call.connection.decRpcCount();
		          call.connection.setLastContact(System.currentTimeMillis());
		          
		          numElements = responseQueue.size();
		          if (numElements == 0) {    // last call fully processes.
		              done = true;             // no more data for this channel.
		          } else {
		        	  LOG.warn("More calls pending");
		              done = false;            // more calls pending to be sent.
		          }
		          
		          if (LOG.isDebugEnabled()) {
		              LOG.debug(getName() + ": responding to #" + call.id + "; Wrote " + reponsemsg.length + " bytes.");
		            }
		          		          
		          error = false;              // everything went off well
		        }
		      } finally {
		        if (error && call != null) {
		          LOG.warn(getName()+", call " + call + ": output error");
		          done = true;               // error. no more data for this channel.
		          closeConnection(call.connection);
		        }
		      }
		      return done;
		    }

		    //
		    // Enqueue a response from the application.
		    //
		    void doRespond(Call call) throws IOException {
		      // LOG.debug("Methode : responder - doRespond()");
		      synchronized (call.connection.responseQueue) {
		        call.connection.responseQueue.addLast(call);
		        if (call.connection.responseQueue.size() == 1) {
		          processResponse(call.connection.responseQueue, true);
		        }
		      }
		    }

	 }

  /** Reads calls from a connection and queues them for handling. */
  private class Connection {
    private boolean versionRead = false; //if initial signature and
                                         //version are read
    private boolean headerRead = false;  //if the connection header that
                                         //follows version is read.

    private BufferedInputStream bis;
    private ByteBuffer data, headerdata;
    private LinkedList<Call> responseQueue;
    private volatile int rpcCount = 0; // number of outstanding rpcs
    private long lastContact;
    private int dataLength = 0;
    private int headerLength = 0;
    private boolean connectionrunning = true;
    private Socket socket;
    private JxtaSocket jsocket;
    private SocketAddress hostAddress;
    
    ConnectionHeader header = new ConnectionHeader();
    Class<?> protocol;
    
    Subject user = null;

    // Fake 'call' for failed authorization response
    private final int AUTHROIZATION_FAILED_CALLID = -1;
    private final Call authFailedCall = 
      new Call(AUTHROIZATION_FAILED_CALLID, null, null);
    private ByteArrayOutputStream authFailedResponse = new ByteArrayOutputStream();
    
    public Connection(JxtaSocket jsock, long lastContact) {
    	this((Socket)jsock,lastContact);
    	this.jsocket = jsock;
    }
    
    public Connection(Socket sock, long lastContact) {
    	this.socket = sock;
    	this.lastContact = lastContact;
    	this.data = null;  
    	this.headerdata = null;
    	   	
    	hostAddress = socket.getRemoteSocketAddress();
    	
    	LOG.debug("Remote connection");
    	this.responseQueue = new LinkedList<Call>();
    }   

    @Override
    public String toString() {
      return getHostAddress(); 
    }
    
    public String getHostAddress() {
    	return this.hostAddress.toString();
    }

    public void setLastContact(long lastContact) {
      this.lastContact = lastContact;
    }

    public long getLastContact() {
      return lastContact;
    }

    public Socket getSocket() {
    	return socket;
    }
    
    /* Return true if the connection has no outstanding rpc */
    private boolean isIdle() {
      return rpcCount == 0;
    }
    
    /* Decrement the outstanding RPC count */
    private void decRpcCount() {
      rpcCount--;
    }
    
    /* Increment the outstanding RPC count */
    private void incRpcCount() {
      rpcCount++;
    }
    
    private boolean timedOut(long currentTime) {
      if (isIdle() && currentTime -  lastContact > maxIdleTime)
        return true;
      return false;
    }

    public int readAndProcess() throws IOException, InterruptedException {
    	ByteBuffer sockbb = ByteBuffer.allocate(NIO_BUFFER_LIMIT);
    	
    	bis = new BufferedInputStream(socket.getInputStream());
    	
    	while (connectionrunning) {
	        /* Read at most one RPC. If the header is not read completely yet
	         * then iterate until we read first RPC or until there is no data left.
	         */  
    		
    		//hrpc 03. 00. 00. 00. 88.%org.jxtadoop.ipc.VersionedProtocol 01. 0A.STRING_UGI 06.franck 0B. 06.franck 03.adm 07.dialout 05.cdrom 05.video 07.plugdev 07.lpadmin 05.admin 0A.sambashare 06.bacula 06.davfs2 00. 00. 00.c 00. 00. 00. 00. 00. 12.getProtocolVersion 00. 00. 00. 02. 00. 10.java.lang.String 00.%org.jxtadoop.rpc.RpcClientProtocol 00. 04.long 00. 00. 00. 00. 00. 00. 00. 01.
    		//hrpc 03. 00. 00. 00. 88.%org.jxtadoop.rpc.RpcClientProtocol 01. 0A.STRING_UGI 06.franck 0B. 06.franck 03.adm 07.dialout 05.cdrom 05.video 07.plugdev 07.lpadmin 05.admin 0A.sambashare 06.bacula 06.davfs2 00. 00. 00. 13. 00. 00. 00. 01. 00. 09.heartbeat 00. 00. 00. 00.

    		
	        int count = 0;
	        int pos = 0;
	        sockbb.clear();
	        	
	        try {
	        	while ((count = bis.read()) > -1) {
	        		sockbb.put(pos,(byte)count);
		        	pos++;
		 	       
		        	/*if(count >= 33 && count <=126)
		        		System.out.printf("%c", count);
		        	else 
		        		System.out.printf(" %02X.", count);*/
				   		   	
		        	if (bis.available() == 0) break;
		        }
	        } catch (SocketTimeoutException ste) { 
	        	continue;
	        }
	        //System.out.printf("\n");
	        
	        if ( count == -1 ) {
	        	Thread.sleep(1000);
	        	continue;
	        }
       	
	        byte[] h = new byte[4];
	        sockbb.get(h, 0, 4);
	        ByteBuffer bb = ByteBuffer.wrap(h);
	        if (HEADER.equals(bb)) {
	        	versionRead = false;
	        	headerRead = false;
	        	data = null;
	        	headerdata=null;
	        }
	        sockbb.rewind();
	        	        
	        if (!versionRead) {           	
	        	int version = sockbb.get(4);
	        	
	        	byte[] headerb = new byte[4];
	          
	        	sockbb.get(headerb, 0, 4);
	        	ByteBuffer headerbb = ByteBuffer.wrap(headerb);
	          	          	          	        
	        	if (!HEADER.equals(headerbb) || version != CURRENT_VERSION) {
	        		// Warning is ok since this is not supposed to happen.
	        		LOG.warn("Incorrect header or version mismatch from \n" + this.getHostAddress() + "\n got version "
	        				+ version + " expected version " + CURRENT_VERSION);
		            return -1;
	        	} 
	        	versionRead = true;
	        }
	        
	        if (!headerRead) {
	        	if (headerdata == null) {
	            	sockbb.position(5);
	            	headerLength = sockbb.getInt();
	            	dataLength = pos - 8 - headerLength;
	           
	              if (headerLength == Client.PING_CALL_ID) {
	                sockbb.clear();
	                return 0;  //ping message
	              }
	              
	              byte[] headerb = new byte[headerLength];
	              sockbb.get(headerb,0,headerLength);
	              headerdata = ByteBuffer.wrap(headerb);
	        
	              headerdata.position(headerdata.remaining());
	         
	              if (headerdata.remaining() == 0) {
	                  headerdata.flip();
	              }
	              
	              processHeader();
		          
		          try {
		        	  authorize(user, header);
		              
		              if (LOG.isDebugEnabled()) {
		                LOG.debug("Successfully authorized " + header);
		              }
		          } catch (AuthorizationException ae) {
		        	  authFailedCall.connection = this;
		        	  setupResponse(authFailedResponse, authFailedCall, 
		                            Status.FATAL, null, 
		                            ae.getClass().getName(), ae.getMessage());
		        	  responder.doRespond(authFailedCall);
		              
		              // Close this connection
		        	  return -1;
		          }
	        	}    
	        } else {
	        	sockbb.position(0);
	        	dataLength = sockbb.getInt();
	        }
	                
	        incRpcCount();  // Increment the rpc count               
	        
	        if (!headerRead) {
	        	headerRead = true;
	        	sockbb.rewind();
	        	sockbb.position(4+1+4+headerLength+4);
	        } else {
	        	sockbb.position(4);
	        }
	        
	        if (headerRead) {
	        	if (dataLength < 1)
	        		continue;
	        	LOG.debug("Data length : "+dataLength);
	        	byte[] datab = new byte[dataLength];
			    sockbb.get(datab,0,dataLength);
			    data = ByteBuffer.wrap(datab);
			          
		        processData();
		        data = null;
	        }
    	} 
    	
    	return -1;
    }

    /// Reads the connection header following version
    private void processHeader() throws IOException {
	      DataInputStream in =
		        new DataInputStream(new ByteArrayInputStream(headerdata.array()));
		      header.readFields(in);
		      try {
		        String protocolClassName = header.getProtocol();
		        if (protocolClassName != null) {
		          protocol = getProtocolClass(header.getProtocol(), conf);
		        }
		      } catch (ClassNotFoundException cnfe) {
		        throw new IOException("Unknown protocol: " + header.getProtocol());
		      }
		      
		      // TODO: Get the user name from the GSS API for Kerberbos-based security
		      // Create the user subject
		      user = SecurityUtil.getSubject(header.getUgi());
    }
    
    private void processData() throws  IOException, InterruptedException {
      DataInputStream dis =
        new DataInputStream(new ByteArrayInputStream(data.array()));
      int id = dis.readInt();                    // try to read an id
        
      if (LOG.isDebugEnabled())
        LOG.debug(" got #" + id);

      Writable param = ReflectionUtils.newInstance(paramClass, conf);           // read param
      param.readFields(dis);        
        
      Call call = new Call(id, param, this);
      callQueue.put(call);              // queue the call; maybe blocked here
    }

    private synchronized void close() throws IOException {
	    LOG.debug("Closing connection");  
    	data = null;
	    connectionrunning = false;
	       
	      try {
	    	  socket.shutdownOutput();
	    	  // socket.close();
	      } catch(Exception e) {
	    	  // e.printStackTrace();
	      }
	    }
  }

  /** Handles queued calls . */
  private class Handler extends Thread {
    public Handler(int instanceNumber) {
      this.setDaemon(true);
      this.setName("IPC Server handler "+ instanceNumber);
    }

    @Override
    public void run() {
      LOG.info(getName() + ": starting");
      SERVER.set(Server.this);
      ByteArrayOutputStream buf = new ByteArrayOutputStream(10240);
      while (running) {
        try {
          final Call call = callQueue.take(); // pop the queue; maybe blocked here

          if (LOG.isDebugEnabled())
            LOG.debug(getName() + ": has #" + call.id);
          
          String errorClass = null;
          String error = null;
          Writable value = null;

          CurCall.set(call);
          try {
            // Make the call as the user via Subject.doAs, thus associating
            // the call with the Subject
            value = 
              Subject.doAs(call.connection.user, 
                           new PrivilegedExceptionAction<Writable>() {
                              @Override
                              public Writable run() throws Exception {
                                // make the call
                                return call(call.connection.protocol, 
                                            call.param, call.timestamp);

                              }
                           }
                          );
              
          } catch (PrivilegedActionException pae) {
            Exception e = pae.getException();
            LOG.info(getName()+", call "+call+": error: " + e, e);
            errorClass = e.getClass().getName();
            error = StringUtils.stringifyException(e);
          } catch (Throwable e) {
            LOG.info(getName()+", call "+call+": error: " + e, e);
            errorClass = e.getClass().getName();
            error = StringUtils.stringifyException(e);
          }
          CurCall.set(null);

          setupResponse(buf, call, 
                        (error == null) ? Status.SUCCESS : Status.ERROR, 
                        value, errorClass, error);
          responder.doRespond(call);
        } catch (InterruptedException e) {
          if (running) {                          // unexpected -- log it
            LOG.info(getName() + " caught: " +
                     StringUtils.stringifyException(e));
          }
        } catch (Exception e) {
          LOG.info(getName() + " caught: " +
                   StringUtils.stringifyException(e));
        }
      }
      LOG.info(getName() + ": exiting");
    }

  }
  
  private class Reader implements Runnable {
		 Connection conn;
		 
		 public Reader(Connection c) {
			 this.conn = c;
		 }
		 
		@Override
		public void run() {
			LOG.debug("Starting the connection reader");
			int count = 0;
			
			conn.setLastContact(System.currentTimeMillis());
			
			try {
				count = conn.readAndProcess();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
			if (count < 0) {
				if (LOG.isDebugEnabled())
			         LOG.debug("Disconnecting client " + 
			                    conn.getHostAddress() + ". Number of active connections: "+
			                    numConnections);
				closeConnection(conn);
				conn = null;
			}else {
				conn.setLastContact(System.currentTimeMillis());
			}
		}
	}
	 	  
  protected Server(PeerGroup pg, JxtaSocketAddress jssa,
          Class<? extends Writable> paramClass, int handlerCount, 
          Configuration conf)
  		throws IOException 
  		{
	  		this(pg, jssa, paramClass, handlerCount,  conf, jssa.getPeerId().toString());
  		}
  
  /** Constructs a server listening on the named port and address.  Parameters passed must
   * be of the named class.  The <code>handlerCount</handlerCount> determines
   * the number of handler threads that will be used to process calls.
   * 
   */
  protected Server(PeerGroup pg, JxtaSocketAddress jssa, 
          Class<? extends Writable> paramClass, int handlerCount, 
          Configuration conf, String serverName) 
  			throws IOException {
		this.p2pServerSockAddr = (SocketAddress)jssa;
		this.jxtaServerSockAddr = jssa;
		this.rpcpg = pg;
		this.conf = conf;
		this.paramClass = paramClass;
		this.handlerCount = handlerCount;
		this.maxQueueSize = handlerCount * MAX_QUEUE_SIZE_PER_HANDLER;
		this.callQueue  = new LinkedBlockingQueue<Call>(maxQueueSize); 
		this.maxIdleTime = 2*conf.getInt("ipc.client.connection.maxidletime", 1000);
		this.maxConnectionsToNuke = conf.getInt("ipc.client.kill.max", 10);
		this.thresholdIdleConnections = conf.getInt("ipc.client.idlethreshold", 4000);
		
		// Start the listener here and let it bind to the port
		listener = new Listener(rpcpg, jxtaServerSockAddr);   
		this.rpcMetrics = new RpcMetrics(serverName,Integer.toString(this.port), this);
		
		// Create the responder here
		responder = new Responder();
  }
  
  
  private void closeConnection(Connection connection) {
    //synchronized (connectionList) {
      if (connectionList.remove(connection))
        numConnections--;
    //}
    try {
      connection.close();
    } catch (IOException e) {
    }
  }
  
  /**
   * Setup response for the IPC Call.
   * 
   * @param response buffer to serialize the response into
   * @param call {@link Call} to which we are setting up the response
   * @param status {@link Status} of the IPC call
   * @param rv return value for the IPC Call, if the call was successful
   * @param errorClass error class, if the the call failed
   * @param error error message, if the call failed
   * @throws IOException
   */
  private void setupResponse(ByteArrayOutputStream response, 
                             Call call, Status status, 
                             Writable rv, String errorClass, String error) 
  throws IOException {
    response.reset();
    DataOutputStream out = new DataOutputStream(response);
    out.writeInt(call.id);                // write call id
    out.writeInt(status.state);           // write status

    if (status == Status.SUCCESS) {
      rv.write(out);
    } else {
      WritableUtils.writeString(out, errorClass);
      WritableUtils.writeString(out, error);
    }
    call.setResponse(ByteBuffer.wrap(response.toByteArray()));
  }
  
  Configuration getConf() {
    return conf;
  }
  
  /** Starts the service.  Must be called before any calls will be handled. */
  public synchronized void start() throws IOException {
    responder.start();
    listener.start();
    handlers = new Handler[handlerCount];
    
    for (int i = 0; i < handlerCount; i++) {
      handlers[i] = new Handler(i);
      handlers[i].start();
    }
  }

  /** Stops the service.  No new calls will be handled after this is called. */
  public synchronized void stop() {
    LOG.info("Stopping server on " + port);
    running = false;
    if (handlers != null) {
      for (int i = 0; i < handlerCount; i++) {
        if (handlers[i] != null) {
          handlers[i].interrupt();
        }
      }
    }
    listener.interrupt();
    listener.doStop();
    responder.interrupt();
    notifyAll();
    if (this.rpcMetrics != null) {
      this.rpcMetrics.shutdown();
    }
  }

  /** Wait for the server to be stopped.
   * Does not wait for all subthreads to finish.
   *  See {@link #stop()}.
   */
  public synchronized void join() throws InterruptedException {
    while (running) {
      wait();
    }
  }

  /**
   * Return the socket (ip+port) on which the RPC server is listening to.
   * @return the socket (ip+port) on which the RPC server is listening to.
   */
  public synchronized JxtaSocketAddress getListenerAddress() {
    return listener.getAddress();
  }
  
  /** 
   * Called for each call. 
   * @deprecated Use {@link #call(Class, Writable, long)} instead
   */
  @Deprecated
  public Writable call(Writable param, long receiveTime) throws IOException {
    return call(null, param, receiveTime);
  }
  
  /** Called for each call. */
  public abstract Writable call(Class<?> protocol,
                               Writable param, long receiveTime)
  throws IOException;
  
  /**
   * Authorize the incoming client connection.
   * 
   * @param user client user
   * @param connection incoming connection
   * @throws AuthorizationException when the client isn't authorized to talk the protocol
   */
  public void authorize(Subject user, ConnectionHeader connection) 
  throws AuthorizationException {}
  
  /**
   * The number of open RPC conections
   * @return the number of open rpc connections
   */
  public int getNumOpenConnections() {
    return numConnections;
  }
  
  /**
   * The number of rpc calls in the queue.
   * @return The number of rpc calls in the queue.
   */
  public int getCallQueueLen() {
    return callQueue.size();
  }

public static PeerID getRemotePeerID() {
	Call call = CurCall.get();
    if (call != null) {
        JxtaSocketAddress jsa = (JxtaSocketAddress) call.connection.jsocket.getRemoteSocketAddress();
        return jsa.getPeerId(); 
      }
    return null;
}
  
  }

