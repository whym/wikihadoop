/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wikimedia.wikihadoop;

import java.io.*;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.fs.Seekable;


public class ByteMatcher {
  private final InputStream in;
  private final Seekable pos;
  private long lastPos;
  private long bytes;
  private final long end;
  public ByteMatcher(InputStream in, Seekable pos, long end) throws IOException {
    this.in = in;
    this.pos = pos;
    this.bytes = 0;
    this.end = end;
    this.lastPos = pos.getPos();
  }
  public long getReadBytes() { return this.bytes; }
  public long getPos() throws IOException { return this.pos.getPos(); }
  public boolean finished() throws IOException { return this.pos.getPos() >= this.end; }
  public long getLastPos() { return this.lastPos; }

  public void skip(long len) throws IOException {
    this.in.skip(len);
    this.bytes += len;
  }

  boolean readUntilMatch(String textPat, DataOutputBuffer outBufOrNull) throws IOException {
    byte[] match = textPat.getBytes("UTF-8");
    int i = 0;
    while (true) {
      int b = this.in.read();
      // end of file:
      if (b == -1) {
        //System.err.println("eof 1");
        return false;
      }
      ++this.bytes;    //! TODO: count up later in batch
      // save to buffer:
      if (outBufOrNull != null)
        outBufOrNull.write(b);
      
      // check if we're matching:
      if (b == match[i]) {
        i++;
        if (i >= match.length)
          return true;
      } else {
        i = 0;
        //pos.proceed(i + 1);
      }
      // see if we've passed the stop point:
      if (outBufOrNull == null && i == 0 && this.pos.getPos() >= end) {
        System.err.println("eof 2");
        return false;
      }
    }
  }
}
