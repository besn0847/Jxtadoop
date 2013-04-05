package org.apache.jxtadoop.hdfs.p2p;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
/**
 * Useful utils to manage files and byte streams.
 * 
 * @author Franck Besnard <franck@besnard.mobi>
 * @version 1.0
 * @since November, 2011
 *
 */
public class FileSystemUtils {
	/**
	 * Returns the file extension.
	 * 
	 * @param fileName The file name to extract the extension from.
	 * @return The file extension
	 */
	public static String getFilenameWithoutExtension(final String fileName)
	{
		final int extensionIndex = fileName.lastIndexOf(".");
		
		if (extensionIndex == -1)
		{
			return fileName;
		}
		
		return fileName.substring(0, extensionIndex);
	}
	/**
	 * Return the file as a byte stream.
	 * 
	 * @param The file to read the byte stream from 
	 * @return The byte stream
	 * @throws IOException The byte stream cannot be read
	 */
	public static ByteArrayOutputStream getBytesFromFile(File f) throws IOException {
		FileInputStream cfis;
		
		cfis = new FileInputStream(f);
			
		int n;
		byte[] buffer = new byte[16];
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
			
		while ((n = cfis.read(buffer)) != -1){
			baos.write(buffer,0,n);
		}
		
		cfis.close();
		
		return baos;
	}
}
