package org.apache.jxtadoop.hdfs.p2p;


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketAddress;

import net.jxta.peer.PeerID;
import net.jxta.socket.JxtaSocketAddress;
/**
 * <b><font color="red">Class added to Hadoop to use Jxta pipes</font></b><br><br>
 * 
 * @author Franck Besnard <franck@besnard.mobi>
 * @version 1.0
 * @since November, 2011
 *
 */
public class P2PNetUtils extends org.apache.jxtadoop.net.NetUtils {	
	public static void connect(Socket socket, SocketAddress endpoint, int timeout) throws IOException {
		if (socket == null || endpoint == null || timeout < 0) {
			throw new IllegalArgumentException("Illegal argument for connect()");
		}

		socket.connect(endpoint, timeout);
	}
	
	public static InputStream getInputStream(Socket socket) throws IOException {
		return socket.getInputStream();
	}
	
	public static OutputStream getOutputStream(Socket socket) throws IOException {
		return socket.getOutputStream();
	}
}

