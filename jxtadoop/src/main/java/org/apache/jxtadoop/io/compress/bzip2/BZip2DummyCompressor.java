package org.apache.jxtadoop.io.compress.bzip2;

import java.io.IOException;

import org.apache.jxtadoop.io.compress.Compressor;

/**
 * This is a dummy compressor for BZip2.
 */
public class BZip2DummyCompressor implements Compressor {

  public int compress(byte[] b, int off, int len) throws IOException {
    throw new UnsupportedOperationException();
  }

 public void end() {
    throw new UnsupportedOperationException();
  }

  public void finish() {
    throw new UnsupportedOperationException();
  }

  public boolean finished() {
    throw new UnsupportedOperationException();
  }

  public long getBytesRead() {
    throw new UnsupportedOperationException();
  }

 public long getBytesWritten() {
    throw new UnsupportedOperationException();
  }

  public boolean needsInput() {
    throw new UnsupportedOperationException();
  }

  public void reset() {
    // do nothing
  }

  public void setDictionary(byte[] b, int off, int len) {
    throw new UnsupportedOperationException();
  }

 public void setInput(byte[] b, int off, int len) {
    throw new UnsupportedOperationException();
  }

}
