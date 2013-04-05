package org.apache.jxtadoop.hdfs.protocol;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.jxtadoop.hdfs.p2p.P2PConstants;
import org.apache.jxtadoop.io.UTF8;
import org.apache.jxtadoop.io.WritableComparable;

/**
 * DatanodeID is composed of the data node 
 * name (hostname:portNumber) and the data storage ID, 
 * which it currently represents.
 * 
 */
public class DatanodeID implements WritableComparable<DatanodeID> {
  public static final DatanodeID[] EMPTY_ARRAY = {}; 

  // public String name;      /// hostname:portNumber
  public String id;
  public String storageID; /// unique per cluster storageID
  // protected int infoPort;     /// the port where the infoserver is running
  protected String infoPipe;
  // public int ipcPort;     /// the port where the ipc server is running
  public String ipcPipe;
  
  /** Equivalent to DatanodeID(""). */
  public DatanodeID() {this("");}

  /** Equivalent to DatanodeID(nodeName, "", -1, -1). */
  public DatanodeID(String nodeName) {this(nodeName, "", P2PConstants.RPCPIPEID, P2PConstants.INFOPIPEID);}

  /**
   * DatanodeID copy constructor
   * 
   * @param from
   */
  public DatanodeID(DatanodeID from) {
    this(from.getPeerId(),
        from.getStorageID(),
        from.getInfoPipe(),
        from.getIpcPipe());
  }
  
  /**
   * Create DatanodeID
   * @param nodeName (hostname:portNumber) 
   * @param storageID data storage ID
   * @param infoPort info server port 
   * @param ipcPort ipc server port
   */
  public DatanodeID(String nodeId, String storageID,
      String infoPipe, String ipcPipe) {
    this.id = nodeId;
    this.storageID = storageID;
    this.infoPipe = infoPipe;
    this.ipcPipe = ipcPipe;
  }
  
  /**
   * @return hostname:portNumber.
   */
  public String getPeerId() {
    return id;
  }
  
  /**
   * @return data storage ID.
   */
  public String getStorageID() {
    return this.storageID;
  }

  /**
   * @return infoPort (the port at which the HTTP server bound to)
   */
  public String getInfoPipe() {
    return infoPipe;
  }

  /**
   * @return ipcPort (the port at which the IPC server bound to)
   */
  public String getIpcPipe() {
    return ipcPipe;
  }

  /**
   * sets the data storage ID.
   */
  public void setStorageID(String storageID) {
    this.storageID = storageID;
  }

  /**
   * @return hostname and no :portNumber.
   */
  /*public String getHost() {
    int colon = name.indexOf(":");
    if (colon < 0) {
      return name;
    } else {
      return name.substring(0, colon);
    }
  }*/
  
  /*public int getPort() {
    int colon = name.indexOf(":");
    if (colon < 0) {
      return 50010; // default port.
    }
    return Integer.parseInt(name.substring(colon+1));
  }*/

  public boolean equals(Object to) {
    if (this == to) {
      return true;
    }
    if (!(to instanceof DatanodeID)) {
      return false;
    }
    return (id.equals(((DatanodeID)to).getPeerId()) &&
            storageID.equals(((DatanodeID)to).getStorageID()));
  }
  
  public int hashCode() {
	return id.hashCode()^ storageID.hashCode();
  }
  
  public String toString() {
	  return id;
  }
  
  /**
   * Update fields when a new registration request comes in.
   * Note that this does not update storageID.
   */
  public void updateRegInfo(DatanodeID nodeReg) {
	id = nodeReg.getPeerId();
	infoPipe = nodeReg.getInfoPipe();
  }
    
  /** Comparable.
   * Basis of compare is the String name (host:portNumber) only.
   * @param that
   * @return as specified by Comparable.
   */
  public int compareTo(DatanodeID that) {
    return id.compareTo(that.getPeerId());
  }

  /////////////////////////////////////////////////
  // Writable
  /////////////////////////////////////////////////
  /** {@inheritDoc} */
  public void write(DataOutput out) throws IOException {
    UTF8.writeString(out, id);
    UTF8.writeString(out, storageID);
    UTF8.writeString(out, infoPipe);
  }

  /** {@inheritDoc} */
  public void readFields(DataInput in) throws IOException {
    id = UTF8.readString(in);
    storageID = UTF8.readString(in);
    infoPipe = UTF8.readString(in);
  }
}
