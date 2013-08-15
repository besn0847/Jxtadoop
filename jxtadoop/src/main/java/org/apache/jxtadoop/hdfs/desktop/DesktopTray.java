package org.apache.jxtadoop.hdfs.desktop;

import java.awt.AWTException;
import java.awt.FlowLayout;
import java.awt.Frame;
import java.awt.Image;
import java.awt.MenuItem;
import java.awt.PopupMenu;
import java.awt.SystemTray;
import java.awt.TextArea;
import java.awt.Toolkit;
import java.awt.TrayIcon;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.jxtadoop.hdfs.server.datanode.DataNode;
import org.apache.jxtadoop.hdfs.server.datanode.FSDatasetInterface;
import org.apache.jxtadoop.util.StringUtils;

/**
 * Used to manage the SystemTray in the Desktop if applicable.
 * Allow quit for DataNode
 */
public class DesktopTray extends Frame implements Runnable {
	 /**
	 * 
	 */
	private static final long serialVersionUID = 2762054010389622504L;

	public static final Log LOG = LogFactory.getLog(DesktopTray.class);
	
	 private TrayIcon trayIcon;
	  private SystemTray tray;
	  private TextArea dnInfoArea;
	  
	  private DataNode datanode;
	  private FSDatasetInterface dndata;
	  private int dnStatus = 0; // 0 - Init; 1 - Running; 2 - Starting 
	  private Thread desktopTrayThread;
	  
	  private Image startIcon, pendingIcon;
	  private boolean shouldRun = false;
	  private int trayHeight,trayWidth;
	  
	  private Class<?> dnClass;
	  private String[] dnArgs;
	  private org.apache.commons.logging.Log dnLog;
	 
	  public DesktopTray() {
		  this(null);
	  }
	  
	 public DesktopTray(DataNode d) {
		  this.datanode = d;
		  
		  startIcon = Toolkit.getDefaultToolkit().getImage(getClass().getResource("/icons/jx_started.png"));
		  pendingIcon = Toolkit.getDefaultToolkit().getImage(getClass().getResource("/icons/jx_stopped.png"));
		  
		  this.trayIcon = null;
		  tray = SystemTray.getSystemTray();
	  }
	  
	  public void init(Class<?> clazz, String[] args,
              final org.apache.commons.logging.Log l) throws SecurityException {
		    LOG.info("Initiliazing the Desktop Tray to control Datanode");
		    
		    this.dnClass = clazz;
		    this.dnArgs = args;
		    this.dnLog = l;
		    
		    
			trayHeight = (int)tray.getTrayIconSize().getHeight();
			trayWidth = (int)tray.getTrayIconSize().getWidth();
			
			final Image scaledPending = pendingIcon.getScaledInstance(trayWidth, trayHeight, 0);
	         
	         ActionListener infoListener = new ActionListener() {
					public void actionPerformed(ActionEvent e) {
						if (datanode != null)
							showDNInfoArea(datanode);
					}
	         };    
	         
	         ActionListener quitListener = new ActionListener() {
					public void actionPerformed(ActionEvent e) {
						if (datanode != null)
							datanode.shutdown();
						shouldRun = false;
						System.exit(0);
					}
	         };    
	         
	         PopupMenu popup = new PopupMenu();
	         
	         MenuItem infoItem = new MenuItem("Info");
	         infoItem.addActionListener(infoListener);
	         popup.add(infoItem);
	         
	         MenuItem quitItem = new MenuItem("Shutdown");
	         quitItem.addActionListener(quitListener);
	         popup.add(quitItem);
	         
	     	trayIcon = new TrayIcon(scaledPending, "Datanode Tray", popup);
	    	trayIcon.addActionListener(quitListener);
	    	
	    	this.shouldRun = true;
	    	this.desktopTrayThread = new Thread(this, "The Desktop tray Thread");
	    	this.desktopTrayThread.start();
	  }
	  
	  private void setDesktopIconStart() {
			if (trayIcon != null) 
				trayIcon.setImage(startIcon.getScaledInstance(trayWidth, trayHeight, 0));
	  }
	  
	  private void setDesktopIconPending() {
			if (trayIcon != null) 
				trayIcon.setImage(pendingIcon.getScaledInstance(trayWidth, trayHeight, 0));
	  }
	  
	  public void setDatanode(DataNode d) {
		  this.datanode = d;
	  }
	  
	  private void showDNInfoArea(DataNode d) {
		   if (dnInfoArea == null)  dnInfoArea=new TextArea("",20,50,TextArea.SCROLLBARS_NONE);
		   
		   String info = "";
		   
			  try {
				  long capacity = dndata.getCapacity();
				  long dfsused = dndata.getDfsUsed();
				  long remaining = dndata.getRemaining();
				  long presentCapacity = dfsused + remaining;
				  String storage = dndata.getStorageInfo();
				  
				  info += "ID : "+datanode.machineName;
				  info += "\n";
				  info += "\nStorage info : "+storage;
				  info += "\n";
				  info += "\nConfigured Capacity: " + capacity + " (" + StringUtils.byteDesc(capacity) + ")";
				  info += "\nPresent Capacity: " + presentCapacity + " (" + StringUtils.byteDesc(presentCapacity) + ")";
				  info += "\nDFS Remaining: " + remaining + " (" + StringUtils.byteDesc(remaining) + ")";
				  info += "\nDFS Used: " + dfsused + " (" + StringUtils.byteDesc(dfsused) + ")";
				  info += "\nDFS Used%: " + StringUtils.limitDecimalTo2(((1.0 * dfsused) / presentCapacity) * 100) + "%";
			  } catch (IOException e) {}
		   
		   dnInfoArea.setText(info);
		   dnInfoArea.setEditable(false);
		   
		   WindowAdapter wa = new WindowAdapter(){
			   public void windowClosing(WindowEvent e){
				   setVisible(false);
			   }
		   	};
		   
		   setName("Local Datanode Information");
		   add(dnInfoArea);
		   setLayout(new FlowLayout());
		   pack();
		   setResizable(true);
		   setVisible(true);
		   addWindowListener(wa);
	   }
	  
	  @Override
	  public void run() {
		  String msg = "";
			  try {
				tray.add(trayIcon);
			  } catch (AWTException e) {
				LOG.error("Failed to add Desktop tray : "+e.getMessage());
			  }
			  
			  StringUtils.startupShutdownMessage(dnClass, dnArgs, dnLog);
			  try {
					this.datanode = DataNode.createDataNode(dnArgs, null);
				} catch (IOException e) {
					LOG.error("Failed to create DataNode instance  : "+e.getMessage());
				}  	
	          
	            if (datanode != null) {	        
	            	synchronized(datanode) {
	            		dndata = datanode.data;
	            	
			            try  {
						  while(shouldRun) {
							  Thread.sleep(1000);
							  
							  if(datanode !=null) {
								  msg = "dnStatus : "+this.dnStatus+";\t connected : "+(datanode.isConnectedToNN());
								  LOG.debug(msg);
								  if(this.dnStatus == 0 && DataNode.isDatanodeUp(datanode)) {
									  	this.setDesktopIconStart();
									  	this.dnStatus = 1;
								  }  else if (this.dnStatus == 1 && !datanode.isConnectedToNN()) {
									  	this.setDesktopIconPending();
									  	this.dnStatus = 2;
								  } else if  (this.dnStatus == 2 && datanode.isConnectedToNN()) {
									  this.setDesktopIconStart();
									  this.dnStatus = 1;
								  }
							  }
						  }  
			            } catch (InterruptedException ie) {}
	            	}
	   }  
	  }
}
