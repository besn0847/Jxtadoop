package org.apache.jxtadoop.fs;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.jxtadoop.conf.Configuration;

public class DiskFree {
	private File dffile; // Default file to be analyzed
	private Path dfpath; // File path
	private FileStore dfstore; // File store on which the file resides
	
	public static final Log LOG = LogFactory.getLog(DiskFree.class);
    
    public DiskFree(File file, long interval) throws IOException {
		if(file.isDirectory())
			this.dffile = file.getCanonicalFile(); // Initialize the mountpoint to the file
		else 
			this.dffile = file.getParentFile().getCanonicalFile();
			
		this.dfpath = this.dffile.toPath(); 
		this.dfstore=Files.getFileStore(this.dfpath);
        for (FileStore store : FileSystems.getDefault().getFileStores()) {
        	if(store.equals(this.dfstore))
                this.dfstore=store;
        }
    }
    
	public DiskFree(File path, Configuration conf) throws IOException {
		this(path, 0L);
	  }
    
    public DiskFree(File path) throws IOException {
	    this(path, 0L);
	  }
    
    public String getDirPath() {
    	return this.dfpath.toString();
      }
    
    public String getFilesystem() throws IOException { 
    	return this.dfstore.name();
      }
    
    public long getCapacity() throws IOException { 
    	return this.dfstore.getTotalSpace();
      }
    
    public long getUsed() throws IOException { 
    	return (this.dfstore.getTotalSpace() - this.dfstore.getUsableSpace());
      }
    
    public long getAvailable() throws IOException { 
    	return this.dfstore.getUsableSpace();
      }
    
    public int getPercentUsed() throws IOException {
    	return (int)(this.getUsed() *100 / this.getCapacity());
      }
    
    public String getMount() throws IOException {
		boolean found = false;
		File mountpoint = null;
		File iterator = this.dffile; 
		FileStore store = null;
		FileStore root = Files.getFileStore((new File(this.getRoot()).toPath()));
		
		while(!found) { 	
			store = Files.getFileStore(iterator.toPath());
			
			if(store.equals(root)) {
				found= true;
				if(mountpoint == null)
					return this.getRoot();
				else
					return mountpoint.getCanonicalPath();
			}
		
			mountpoint = iterator;
			iterator = new File(mountpoint.getCanonicalFile()+"/..");
		}
		
		return "error"; 
      }
    
	protected String getRoot() {
		return this.dfpath.getRoot().toString();
	}

    public String toString() {
        try {
			return
				      "df -k " + this.getDirPath() +"\n" +
				      this.getFilesystem() + "\t" +
				      this.getCapacity() / 1024 + "\t" +
				      this.getUsed() / 1024 + "\t" +
				      this.getAvailable() / 1024 + "\t" +
				      this.getPercentUsed() + "%\t" +
				      this.getMount();
		} catch (IOException e) {
			LOG.error(e.getMessage());
			return "error";
		}
    }
    
	public static void main(String[] args) throws Exception {
		 String path = ".";
		    
		    if (args.length > 0) {
		      path = args[0];
		    }
		    
		    DiskFree df = new DiskFree(new File(path),5000L);	
		    
		    System.out.println(df.toString());
	}
}
