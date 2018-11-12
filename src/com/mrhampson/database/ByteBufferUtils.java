/*
 * ByteHelpers.java
 * Created on Nov 10, 2018, 10:50 PM
 *
 * Copyright 2008-2018 LiveAction, Incorporated. All Rights Reserved.
 * 3500 W Bayshore Road, Palo Alto, California 94303, U.S.A.
 *
 * This software is the confidential and proprietary information
 * of LiveAction ("Confidential Information").
 * You shall not disclose such Confidential Information and shall use
 * it only in accordance with the terms of the license agreement
 * you entered into with LiveAction.
 */
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
