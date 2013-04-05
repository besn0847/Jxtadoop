package org.apache.jxtadoop.hdfs.protocol;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

import org.apache.jxtadoop.io.Text;
import org.apache.jxtadoop.io.UTF8;
import org.apache.jxtadoop.io.Writable;
import org.apache.jxtadoop.io.WritableFactories;
import org.apache.jxtadoop.io.WritableFactory;
import org.apache.jxtadoop.io.WritableUtils;
import org.apache.jxtadoop.net.NetworkTopology;
import org.apache.jxtadoop.net.Node;
import org.apache.jxtadoop.net.NodeBase;
import org.apache.jxtadoop.util.StringUtils;

/** 
 * DatanodeInfo represents the status of a DataNode.
 * This object is used for communication in the
 * Datanode Protocol and the Client Protocol.
 */
public class DatanodeInfo extends DatanodeID implements Node {
  protected long capacity;
  protected long dfsUsed;
  protected long remaining;
  protected long lastUpdate;
  protected int xceiverCount;
  protected String location = NetworkTopology.DEFAULT_RACK;

  /** HostName as suplied by the datanode during registration as its 
   * name. Namenode uses datanode IP address as the name.
   */
  protected String hostId = null;
  
  // administrative states of a datanode
  public enum AdminStates {NORMAL, DECOMMISSION_INPROGRESS, DECOMMISSIONED; }
  protected AdminStates adminState;


  public DatanodeInfo() {
    super();
    adminState = null;
  }
  
  public DatanodeInfo(DatanodeInfo from) {
    super(from);
    this.capacity = from.getCapacity();
    this.dfsUsed = from.getDfsUsed();
    this.remaining = from.getRemaining();
    this.lastUpdate = from.getLastUpdate();
    this.xceiverCount = from.getXceiverCount();
    this.location = from.getNetworkLocation();
    this.adminState = from.adminState;
    this.hostId = from.hostId;
  }

  public DatanodeInfo(DatanodeID nodeID) {
    super(nodeID);
    this.capacity = 0L;
    this.dfsUsed = 0L;
    this.remaining = 0L;
    this.lastUpdate = 0L;
    this.xceiverCount = 0;
    this.adminState = null;    
  }
  
  protected DatanodeInfo(DatanodeID nodeID, String location, String hostId) {
    this(nodeID);
    this.location = location;
    this.hostId = hostId;
  }
  
  /** The raw capacity. */
  public long getCapacity() { return capacity; }
  
  /** The used space by the data node. */
  public long getDfsUsed() { return dfsUsed; }

  /** The used space by the data node. */
  public long getNonDfsUsed() { 
    long nonDFSUsed = capacity - dfsUsed - remaining;
    return nonDFSUsed < 0 ? 0 : nonDFSUsed;
  }

  /** The used space by the data node as percentage of present capacity */
  public float getDfsUsedPercent() { 
    if (capacity <= 0) {
      return 100;
    }

    return ((float)dfsUsed * 100.0f)/(float)capacity; 
  }

  /** The raw free space. */
  public long getRemaining() { return remaining; }

  /** The remaining space as percentage of configured capacity. */
  public float getRemainingPercent() { 
    if (capacity <= 0) {
      return 0;
    }

    return ((float)remaining * 100.0f)/(float)capacity; 
  }

  /** The time when this information was accurate. */
  public long getLastUpdate() { return lastUpdate; }

  /** number of active connections */
  public int getXceiverCount() { return xceiverCount; }

  /** Sets raw capacity. */
  public void setCapacity(long capacity) { 
    this.capacity = capacity; 
  }

  /** Sets raw free space. */
  public void setRemaining(long remaining) { 
    this.remaining = remaining; 
  }

  /** Sets time when this information was accurate. */
  public void setLastUpdate(long lastUpdate) { 
    this.lastUpdate = lastUpdate; 
  }

  /** Sets number of active connections */
  public void setXceiverCount(int xceiverCount) { 
    this.xceiverCount = xceiverCount; 
  }

  /** rack name **/
  public synchronized String getNetworkLocation() {return location;}
    
  /** Sets the rack name */
  public synchronized void setNetworkLocation(String location) {
    this.location = NodeBase.normalize(location);
  }
  
  public String getHostName() {
    return (hostId == null || hostId.length()==0) ? getPeerId() : hostId;
  }
  
  public void setHostName(String host) {
    hostId = host;
  }
  
  /** A formatted string for reporting the status of the DataNode. */
  public String getDatanodeReport() {
    StringBuffer buffer = new StringBuffer();
    long c = getCapacity();
    long r = getRemaining();
    long u = getDfsUsed();
    long nonDFSUsed = getNonDfsUsed();
    float usedPercent = getDfsUsedPercent();
    float remainingPercent = getRemainingPercent();

    buffer.append("Name: "+id+"\n");
    if (!NetworkTopology.DEFAULT_RACK.equals(location)) {
      buffer.append("Rack: "+location+"\n");
    }
    buffer.append("Decommission Status : ");
    if (isDecommissioned()) {
      buffer.append("Decommissioned\n");
    } else if (isDecommissionInProgress()) {
      buffer.append("Decommission in progress\n");
    } else {
      buffer.append("Normal\n");
    }
    buffer.append("Configured Capacity: "+c+" ("+StringUtils.byteDesc(c)+")"+"\n");
    buffer.append("DFS Used: "+u+" ("+StringUtils.byteDesc(u)+")"+"\n");
    buffer.append("Non DFS Used: "+nonDFSUsed+" ("+StringUtils.byteDesc(nonDFSUsed)+")"+"\n");
    buffer.append("DFS Remaining: " +r+ "("+StringUtils.byteDesc(r)+")"+"\n");
    buffer.append("DFS Used%: "+StringUtils.limitDecimalTo2(usedPercent)+"%\n");
    buffer.append("DFS Remaining%: "+StringUtils.limitDecimalTo2(remainingPercent)+"%\n");
    buffer.append("Last contact: "+new Date(lastUpdate)+"\n");
    return buffer.toString();
  }

  /** A formatted string for printing the status of the DataNode. */
  public String dumpDatanode() {
    StringBuffer buffer = new StringBuffer();
    long c = getCapacity();
    long r = getRemaining();
    long u = getDfsUsed();
    buffer.append(id);
    if (!NetworkTopology.DEFAULT_RACK.equals(location)) {
      buffer.append(" "+location);
    }
    if (isDecommissioned()) {
      buffer.append(" DD");
    } else if (isDecommissionInProgress()) {
      buffer.append(" DP");
    } else {
      buffer.append(" IN");
    }
    buffer.append(" " + c + "(" + StringUtils.byteDesc(c)+")");
    buffer.append(" " + u + "(" + StringUtils.byteDesc(u)+")");
    buffer.append(" " + StringUtils.limitDecimalTo2(((1.0*u)/c)*100)+"%");
    buffer.append(" " + r + "(" + StringUtils.byteDesc(r)+")");
    buffer.append(" " + new Date(lastUpdate));
    return buffer.toString();
  }

  /**
   * Start decommissioning a node.
   * old state.
   */
  public void startDecommission() {
    adminState = AdminStates.DECOMMISSION_INPROGRESS;
  }

  /**
   * Stop decommissioning a node.
   * old state.
   */
  public void stopDecommission() {
    adminState = null;
  }

  /**
   * Returns true if the node is in the process of being decommissioned
   */
  public boolean isDecommissionInProgress() {
    if (adminState == AdminStates.DECOMMISSION_INPROGRESS) {
      return true;
    }
    return false;
  }

  /**
   * Returns true if the node has been decommissioned.
   */
  public boolean isDecommissioned() {
    if (adminState == AdminStates.DECOMMISSIONED) {
      return true;
    }
    return false;
  }

  /**
   * Sets the admin state to indicate that decommision is complete.
   */
  public void setDecommissioned() {
    adminState = AdminStates.DECOMMISSIONED;
  }

  /**
   * Retrieves the admin state of this node.
   */
  AdminStates getAdminState() {
    if (adminState == null) {
      return AdminStates.NORMAL;
    }
    return adminState;
  }

  /**
   * Sets the admin state of this node.
   */
  protected void setAdminState(AdminStates newState) {
    if (newState == AdminStates.NORMAL) {
      adminState = null;
    }
    else {
      adminState = newState;
    }
  }

  private int level; //which level of the tree the node resides
  private Node parent; //its parent

  /** Return this node's parent */
  public Node getParent() { return parent; }
  public void setParent(Node parent) {this.parent = parent;}
   
  /** Return this node's level in the tree.
   * E.g. the root of a tree returns 0 and its children return 1
   */
  public int getLevel() { return level; }
  public void setLevel(int level) {this.level = level;}

  /////////////////////////////////////////////////
  // Writable
  /////////////////////////////////////////////////
  static {                                      // register a ctor
    WritableFactories.setFactory
      (DatanodeInfo.class,
       new WritableFactory() {
         public Writable newInstance() { return new DatanodeInfo(); }
       });
  }

  /** {@inheritDoc} */
  public void write(DataOutput out) throws IOException {
    super.write(out);

    //TODO: move it to DatanodeID once DatanodeID is not stored in FSImage
    UTF8.writeString(out, ipcPipe);

    out.writeLong(capacity);
    out.writeLong(dfsUsed);
    out.writeLong(remaining);
    out.writeLong(lastUpdate);
    out.writeInt(xceiverCount);
    Text.writeString(out, location);
    Text.writeString(out, hostId == null? "": hostId);
    WritableUtils.writeEnum(out, getAdminState());
  }

  /** {@inheritDoc} */
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);

    //TODO: move it to DatanodeID once DatanodeID is not stored in FSImage
    //this.ipcPort = in.readShort() & 0x0000ffff;
    this.ipcPipe = UTF8.readString(in);
    
    this.capacity = in.readLong();
    this.dfsUsed = in.readLong();
    this.remaining = in.readLong();
    this.lastUpdate = in.readLong();
    this.xceiverCount = in.readInt();
    this.location = Text.readString(in);
    this.hostId = Text.readString(in);
    setAdminState(WritableUtils.readEnum(in, AdminStates.class));
  }

	@Override
	public String getName() {
		return hostId;
	}
}

