package org.apache.jxtadoop.fs;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.jxtadoop.conf.Configuration;

public class DiskUsage implements FileFilter {
	private String  dirPath;
    private File dir;
    private long dirSize = 0;
	private AtomicLong used = new AtomicLong();
	private volatile boolean shouldRun = true;
	private long refreshInterval;
	@SuppressWarnings("unused")
	private IOException duException = null;
	private Thread refreshUsed;
	
	public static final Log LOG = LogFactory.getLog(DiskUsage.class);
	
	public DiskUsage(File path, long interval) throws IOException {
		this.refreshInterval=interval;
		this.dirPath = path.getCanonicalPath();
		this.dir = new File(this.dirPath);
		run();
	}
	
	public DiskUsage(File path, Configuration conf) throws IOException {
	    this(path, 600000L);
	  }
	
	public DiskUsage(File path) throws IOException {
	    this(path, 600000L);
	  }
	
	 public String getDirPath() {
		   return dirPath;
	 }
	  	
	  public void decDfsUsed(long value) {
		    used.addAndGet(-value);
		  }
	  
	  public void incDfsUsed(long value) {
		    used.addAndGet(value);
		  }
	  
	  public boolean accept(File file) {
		  if ( file.isFile())
			  dirSize += file.length();
		  else
			  file.listFiles(this);
		  return false;
	  	}
		
	  public long getUsed() throws IOException {
		  return dirSize;
	  }
	  
	  public void start() {
		      refreshUsed = new Thread(new DURefreshThread(),"refreshUsed-"+dirPath);
		      refreshUsed.setDaemon(true);
		      refreshUsed.start();
	  }
	  
	  protected void run() throws IOException {
		  this.dirSize = 0;
		  this.accept(dir);
	  }
	  
	  public void shutdown() {
		    this.shouldRun = false;
		    
		    if(this.refreshUsed != null) {
		      this.refreshUsed.interrupt();
		    }
		  }
	  
	  public String toString() {
		    return
		      "du -sk " + dirPath +"\n" +
		      dirSize  / 1024+ "\t" + dirPath;
		  }
	  
	  class DURefreshThread implements Runnable {
		    
		    public void run() {
		      
		      while(shouldRun) {
		        try {
		          Thread.sleep(refreshInterval);
		          
		          try {
		            DiskUsage.this.run();
		          } catch (IOException e) {
		            synchronized (DiskUsage.this) {
		              duException = e;
		            }
		            
		            LOG.warn("Could not get disk usage information", e);
		          }
		        } catch (InterruptedException e) {
		        }
		      }
		    }
		  }
	  
	  public static void main(String[] args) throws Exception {
		    String path = ".";
		    
		    if (args.length > 0) {
		      path = args[0];
		    }
		    
		    DiskUsage du = new DiskUsage(new File(path),15000L);	
		    du.start();
		    
		    System.out.println(du.toString());
	  }
}
