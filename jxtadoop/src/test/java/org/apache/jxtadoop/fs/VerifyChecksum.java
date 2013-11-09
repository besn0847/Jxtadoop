package org.apache.jxtadoop.fs;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.jxtadoop.conf.Configuration;
import org.apache.jxtadoop.conf.Configured;
import org.apache.jxtadoop.hdfs.p2p.P2PConstants;
import org.apache.jxtadoop.io.DataOutputBuffer;
import org.apache.jxtadoop.io.MD5Hash;
import org.apache.jxtadoop.util.DataChecksum;

public class VerifyChecksum  extends Configured {
	Configuration c;
	String file;
	
	private int bytes_per_crc = 512;
	long l_LocalBlockSize = P2PConstants.DEFAULT_BLOCK_SIZE;
	
	public VerifyChecksum(Configuration c, String file) throws IOException {
		super(c);
		this.c = c;
		this.file = file;
	}
	
	public MD5MD5CRC32FileChecksum getLocalFilesystemHDFSStyleChecksum(
			String strPath, int bytesPerCRC, long lBlockSize)
			throws IOException {

		Path srcPath = new Path(strPath);
		FileSystem srcFs = srcPath.getFileSystem(getConf());
		long lFileSize = 0;
		int iBlockCount = 0;
		DataOutputBuffer md5outDataBuffer = new DataOutputBuffer();
		DataChecksum chksm = DataChecksum.newDataChecksum(
				DataChecksum.CHECKSUM_CRC32, 512);
		InputStream in = null;
		MD5MD5CRC32FileChecksum returnChecksum = null;
		long crc_per_block = lBlockSize / bytesPerCRC;

		if (null == srcFs) {
			System.out.println("srcFs is null! ");
		} else {

			// FileStatus f_stats = srcFs.getFileStatus( srcPath );
			lFileSize = srcFs.getFileStatus(srcPath).getLen();
			iBlockCount = (int) Math.ceil((double) lFileSize
					/ (double) lBlockSize);

			// System.out.println( "Debug > getLen == " + f_stats.getLen() +
			// " bytes" );
			// System.out.println( "Debug > iBlockCount == " + iBlockCount );

		}

		if (srcFs.getFileStatus(srcPath).isDir()) {

			throw new IOException("Cannot compute local hdfs hash, " + srcPath
					+ " is a directory! ");

		}

		try {

			in = srcFs.open(srcPath);
			long lTotalBytesRead = 0;

			for (int x = 0; x < iBlockCount; x++) {

				ByteArrayOutputStream ar_CRC_Bytes = new ByteArrayOutputStream();

				byte crc[] = new byte[4];
				byte buf[] = new byte[512];

				try {

					int bytesRead = 0;

					while ((bytesRead = in.read(buf)) > 0) {

						lTotalBytesRead += bytesRead;

						chksm.reset();
						chksm.update(buf, 0, bytesRead);
						chksm.writeValue(crc, 0, true);
						ar_CRC_Bytes.write(crc);

						if (lTotalBytesRead >= (x + 1) * lBlockSize) {

							break;
						}

					} // while

					DataInputStream inputStream = new DataInputStream(
							new ByteArrayInputStream(ar_CRC_Bytes.toByteArray()));

					// this actually computes one ---- run on the server
					// (DataXceiver) side
					final MD5Hash md5_dataxceiver = MD5Hash.digest(inputStream);
					md5_dataxceiver.write(md5outDataBuffer);

				} catch (IOException e) {

					e.printStackTrace();

				} catch (Exception e) {

					e.printStackTrace();

				}

			} // for

			// this is in 0.19.0 style with the extra padding bug
			final MD5Hash md5_of_md5 = MD5Hash.digest(md5outDataBuffer
					.getData());
			returnChecksum = new MD5MD5CRC32FileChecksum(bytesPerCRC,
					crc_per_block, md5_of_md5);

		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {

			e.printStackTrace();

		} finally {
			in.close();
		} // try

		return returnChecksum;

	}
	
	public void run() throws IOException {
		MD5MD5CRC32FileChecksum checksum = getLocalFilesystemHDFSStyleChecksum(file,bytes_per_crc,l_LocalBlockSize);
		if (checksum != null) {
			System.err.println(file+" :"+checksum.toString().substring(checksum.toString().indexOf(':')).replace(':', ' '));
		} else {
			System.err.println("Failed to compute checksum");
		}
	}
	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException  {
		Configuration.addDefaultResource("core-default.xml");
		Configuration.addDefaultResource("hdfs-default.xml");
		Configuration.addDefaultResource("hdfs-p2p.xml");
	
		
		VerifyChecksum vc = new VerifyChecksum(new Configuration(),args[0]);
		vc.run();
	}

}
