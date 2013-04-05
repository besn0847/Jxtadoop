package org.apache.jxtadoop.hdfs.p2p;


import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

import javax.net.SocketFactory;
/** 
 * <b><font color="red">Class added to Hadoop to use Jxta pipes</font></b><br><br>
 * 
 * @author Franck Besnard <franck@besnard.mobi>
 * @version 1.0
 * @since November, 2011
 *
 */
public class P2PSocketFactory extends SocketFactory {
	  public P2PSocketFactory() {
	  }

	  /* @inheritDoc */
	  @Override
	  public Socket createSocket() throws IOException {
		  return new Socket();
	  }

	  /* @inheritDoc */
	  @Override
	  public Socket createSocket(InetAddress addr, int port) throws IOException {
		  throw new IOException("Operation not supported");
	  }

	  /* @inheritDoc */
	  @Override
	  public Socket createSocket(InetAddress addr, int port,
	      InetAddress localHostAddr, int localPort) throws IOException {
		  throw new IOException("Operation not supported");
	  }

	  /* @inheritDoc */
	  @Override
	  public Socket createSocket(String host, int port) throws IOException,
	      UnknownHostException {
		  throw new IOException("Operation not supported");
	  }

	  /* @inheritDoc */
	  @Override
	  public Socket createSocket(String host, int port,
	      InetAddress localHostAddr, int localPort) throws IOException,
	      UnknownHostException {
		  throw new IOException("Operation not supported");
	  }

	  /* @inheritDoc */
	  @Override
	  public boolean equals(Object obj) {
	    if (this == obj)
	      return true;
	    if (obj == null)
	      return false;
	    if (!(obj instanceof P2PSocketFactory))
	      return false;
	    return true;
	  }

	  /* @inheritDoc */
	  @Override
	  public int hashCode() {
	    return 88;
	  } 
}
