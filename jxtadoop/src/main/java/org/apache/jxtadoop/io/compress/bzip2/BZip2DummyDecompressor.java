package org.apache.jxtadoop.io.compress.bzip2;

import java.io.IOException;

import org.apache.jxtadoop.io.compress.Decompressor;

/**
 * This is a dummy decompressor for BZip2.
 */
public class BZip2DummyDecompressor implements Decompressor {

  public int decompress(byte[] b, int off, int len) throws IOException {
    throw new UnsupportedOperationException();
  }

  public void end() {
    throw new UnsupportedOperationException();
  }

  public boolean finished() {
    throw new UnsupportedOperationException();
  }

  public boolean needsDictionary() {
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
