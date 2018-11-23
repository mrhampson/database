package com.mrhampson.database;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * @author Marshall Hampson
 */
public class ByteBufferUtils {
  public static String fromASCIIBytes(ByteBuffer byteString) {
      Objects.requireNonNull(byteString);
      int currentPosition = byteString.position();
      int limit = byteString.limit();
      StringBuilder sb = new StringBuilder();
      for (int i = currentPosition; i < limit; i++) {
          sb.append((char)byteString.get());
      }
      return sb.toString();
  }
}
