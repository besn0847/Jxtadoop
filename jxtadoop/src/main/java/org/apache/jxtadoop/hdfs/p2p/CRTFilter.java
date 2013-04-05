package org.apache.jxtadoop.hdfs.p2p;

import java.io.File;
import java.io.FilenameFilter;
/**
 * This class implements the logic to filter the certificate file in the cert/ directory.
 * 
 * @author Franck Besnard <franck@besnard.mobi>
 * @version 1.0
 * @since November, 2011
 * 
 */
public class CRTFilter  implements FilenameFilter  {
	/**
	 * Filter all the files with .crt extension.  
	 */
	@Override
	public boolean accept(File arg0, String arg1) {
		return (arg1.endsWith(".crt"));
	}
}
