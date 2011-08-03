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
import java.util.*;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.compress.*;

/** A InputFormat implementation that splits a Wikimedia Dump File into page fragments, and emits them as input records.
 *  A key contains a pair of continued revisions and the page element that the revisions belong to in the following format.
 *  &lt;page&gt;
 *    &lt;title&gt;....&lt;/title&gt;
 *    &lt;id&gt;....&lt;/id&gt;
 *    &lt;revision&gt;
 *      ....
 *    &lt;/revision&gt;
 *    &lt;revision&gt;
 *      ....
 *    &lt;/revision&gt;
 *  &lt;/page&gt;
 */
public class StreamWikiDumpInputFormat extends KeyValueTextInputFormat {

  private CompressionCodecFactory compressionCodecs = null;
   
  public void configure(JobConf conf) {
    this.compressionCodecs = new CompressionCodecFactory(conf);
  }
  
  protected boolean isSplitable(FileSystem fs, Path file) {
    final CompressionCodec codec = compressionCodecs.getCodec(file);
    if (null == codec) {
      return true;
    }
    return codec instanceof SplittableCompressionCodec;
  }

  /** 
   * Generate the list of files and make them into FileSplits.
   * @param job the job context
   * @throws IOException
   */
  @Override public InputSplit[] getSplits(JobConf job, int n) throws IOException {
    System.err.println("StreamWikiDumpInputFormat.getSplits job=" + job + " n=" + n);
    InputSplit[] oldSplits = super.getSplits(job, n);
    List<InputSplit> splits = new ArrayList<InputSplit>();
    for ( InputSplit x: oldSplits ) {
      splits.add(x);//!
    }

    InputSplit[] array = new InputSplit[splits.size()];
    splits.toArray(array);
    return array;
  }

  public RecordReader<Text, Text> getRecordReader(final InputSplit genericSplit,
                                                  JobConf job, Reporter reporter) throws IOException {
    // handling non-standard record reader (likely StreamXmlRecordReader) 
    FileSplit split = (FileSplit) genericSplit;
    LOG.info("getRecordReader start.....split=" + split);
    reporter.setStatus(split.toString());

    // Open the file and seek to the start of the split
    FileSystem fs = split.getPath().getFileSystem(job);
    return new MyRecordReader(split, reporter, job, fs);
  }

  private class MyRecordReader implements RecordReader<Text,Text> {
    
    public MyRecordReader(FileSplit split, Reporter reporter,
                          JobConf job, FileSystem fs) throws IOException {
      this.revisionBeginPattern = "<revision";
      this.revisionEndPattern   = "</revision>";
      this.pageHeader   = new DataOutputBuffer();
      this.prevRevision = new DataOutputBuffer();
      this.pageFooter = getBuffer("\n</page>\n".getBytes("UTF-8"));
      this.revHeader  = getBuffer(this.revisionBeginPattern.getBytes("UTF-8"));
      this.firstDummyRevision = getBuffer(" beginningofpage=\"true\"></revision>\n".getBytes("UTF-8"));
      this.bufInRev = new DataOutputBuffer();
      this.bufBeforeRev = new DataOutputBuffer();
      this.split = split;
      this.fs = fs;
      SeekableInputStream in = SeekableInputStream.getInstance(split, fs, compressionCodecs);
      SplitCompressionInputStream sin = in.getSplitCompressionInputStream();
      if ( sin == null ) {
        this.start = split.getStart();
        this.end   = split.getStart() + split.getLength();
      } else {
        this.start = sin.getAdjustedStart();
        this.end   = sin.getAdjustedEnd();
      }
      this.reporter = reporter;

      allWrite(this.prevRevision, this.firstDummyRevision);
      this.currentPageNum = -1;
      this.pageBytes = getPageBytes(this.split, this.fs, compressionCodecs, this.reporter);

      this.istream = SeekableInputStream.getInstance(this.split, this.fs, compressionCodecs);
      this.matcher = new ByteMatcher(this.istream, this.istream, this.end);
      this.seekNextRecordBoundary();
      this.reporter.incrCounter(WikiDumpCounters.WRITTEN_REVISIONS, 0);
      this.reporter.incrCounter(WikiDumpCounters.WRITTEN_PAGES, 0);
    }
    
    @Override public Text createKey() {
      return new Text();
    }
    
    @Override public Text createValue() {
      return new Text();
    }
    
    @Override public void close() throws IOException {
      this.istream.close();
    }
    
    @Override public float getProgress() throws IOException {
      float rate = 0.0f;
      if (this.end == this.start) {
        rate = 1.0f;
      } else {
        rate = ((float)(this.getPosition() - this.start)) / ((float)(this.end - this.start));
      }
      return rate;
    }
    
    @Override public long getPos() throws IOException {
      return this.istream.getPos();
    }
    
    public long getPosition() throws IOException {
      return this.istream.getPos();
    }
    
    public synchronized long getReadBytes() throws IOException {
        return this.matcher.getReadBytes();
      }
    
    @Override synchronized public boolean next(Text key, Text value) throws IOException {
      //System.err.println("StreamWikiDumpInputFormat: split=" + split + " start=" + this.start + " end=" + this.end + " pos=" + this.getPosition() + " read=" + this.getReadBytes() + " pageBytes=" + pageBytes);//!
      LOG.info("StreamWikiDumpInputFormat: split=" + split + " start=" + this.start + " end=" + this.end + " pos=" + this.getPosition() + " pageBytes=" + pageBytes);
      
      //System.err.println("0.2 check pos="+this.getPosition() + " end="+this.end);//!
      if (this.currentPageNum >= this.pageBytes.size() / 2  ||  this.getReadBytes() >= this.tailPageEnd()) {
        return false;
      }

      //System.err.println("2 move to rev from: " + this.getReadBytes());//!
      if (!readUntilMatch(this.revisionBeginPattern, this.bufBeforeRev)  ||  this.getReadBytes() >= this.tailPageEnd()) { // move to the beginning of the next revision
        return false;
      }
      //System.err.println("2.1 move to rev to: " + this.getReadBytes());//!
        
      //System.err.println("4.5 check if exceed: " + this.getReadBytes() + " " + nextPageBegin() + " " + prevPageEnd());//!
      if ( this.getReadBytes() >= this.nextPageBegin() ) {
        // int off = (int)(this.nextPageBegin() - this.prevPageEnd());
        int off = findIndex(pageBeginPattern.getBytes("UTF-8"), this.bufBeforeRev);
        if ( off >= 0 ) {
          offsetWrite(this.pageHeader, off, this.bufBeforeRev);
          allWrite(this.prevRevision, this.firstDummyRevision);
          this.currentPageNum++;
          reporter.incrCounter(WikiDumpCounters.WRITTEN_PAGES, 1);
          //System.err.println("4.6 exceed");//!
        }
      }
      
      //System.err.println("4 read rev from: " + this.getReadBytes());//!
      if (!readUntilMatch(this.revisionEndPattern, this.bufInRev)) { // store the revision
        //System.err.println("no revision end" + this.getReadBytes() + " " + this.end);//!
          LOG.info("no revision end");
          return false;
        }
      //System.err.println("4.1 read rev to: " + this.getReadBytes());//!
        
      //System.err.println("5 read rev pos " + this.getReadBytes());//!
      byte[] record = writeInSequence(new DataOutputBuffer[]{ this.pageHeader,
                                                              this.prevRevision,
                                                              this.revHeader,
                                                              this.bufInRev,
                                                              this.pageFooter});
      key.set(record);
      //System.out.print(key.toString());//!
      value.set("");
      this.reporter.setStatus("StreamWikiDumpInputFormat: write new record pos=" + this.istream.getPos() + " bytes=" + this.getReadBytes());
      System.err.println("StreamWikiDumpInputFormat: write new record pos=" + this.istream.getPos() + " bytes=" + this.getReadBytes() + " next=" + this.nextPageBegin() + " prev=" + this.prevPageEnd());//!
      reporter.incrCounter(WikiDumpCounters.WRITTEN_REVISIONS, 1);
      
      allWrite(this.prevRevision, this.bufInRev);
      
      return true;
    }
    
    public synchronized void seekNextRecordBoundary() throws IOException {
      if ( this.getReadBytes() < this.nextPageBegin() ) {
        long len = this.nextPageBegin() - this.getReadBytes();
        this.matcher.skip(len);
      }
    }
    private synchronized boolean readUntilMatch(String textPat, DataOutputBuffer outBufOrNull) throws IOException {
      if ( outBufOrNull != null )
        outBufOrNull.reset();
      return this.matcher.readUntilMatch(textPat, outBufOrNull);
    }
    private long tailPageEnd() {
      if ( this.pageBytes.size() > 0 ) {
        return this.pageBytes.get(this.pageBytes.size() - 1);
      } else {
        return 0;
      }
    }
    private long nextPageBegin() {
      if ( (this.currentPageNum + 1) * 2 < this.pageBytes.size() - 1 ) {
        return this.pageBytes.get((this.currentPageNum + 1) * 2);
      } else {
        return this.end;
      }
    }
    private long prevPageEnd() {
      if ( this.currentPageNum == 0 ) {
        if ( this.pageBytes.size() > 0 ) {
          return this.pageBytes.get(0);
        } else {
          return this.start;
        }
      } else if ( this.currentPageNum * 2 - 1 <= this.pageBytes.size() - 1 ) {
        return this.pageBytes.get(this.currentPageNum * 2 - 1);
      } else {
        return this.end;
      }
    }  
  
    private int currentPageNum;
    private final long start;
    private final long end;
    private final List<Long> pageBytes;
    private final SeekableInputStream  istream;
    private final String revisionBeginPattern;
    private final String revisionEndPattern;
    private final DataOutputBuffer pageHeader;
    private final DataOutputBuffer revHeader;
    private final DataOutputBuffer prevRevision;
    private final DataOutputBuffer pageFooter;
    private final DataOutputBuffer firstDummyRevision;
    private final DataOutputBuffer bufInRev;
    private final DataOutputBuffer bufBeforeRev;
    private final FileSystem fs;
    private final FileSplit split;
    private final Reporter reporter;
    private final ByteMatcher matcher;
  }

  private static byte[] writeInSequence(DataOutputBuffer[] array) {
    int size = 0;
    for (DataOutputBuffer buf: array) {
      size += buf.getLength();
    }
    byte[] dest = new byte[size];
    int n = 0;
    for (DataOutputBuffer buf: array) {
      System.arraycopy(buf.getData(), 0, dest, n, buf.getLength());
      n += buf.getLength();
    }
    return dest;
  }
  
  private static void copy(InputStream in, OutputStream out) throws IOException {
    byte[] buffer = new byte[1024 * 1024];
    int len = 0;
    while ( (len = in.read(buffer)) > 0 ) {
      out.write(buffer, 0, len);
    }
    out.flush();
  }

  private static DataOutputBuffer getBuffer(byte[] bytes) throws IOException {
    DataOutputBuffer ret = new DataOutputBuffer(bytes.length);
    ret.write(bytes);
    return ret;
  }

  private static List<Long> getPageBytes(FileSplit split, FileSystem fs, CompressionCodecFactory compressionCodecs, Reporter reporter) throws IOException {
    SeekableInputStream in = null;
    try {
      in = SeekableInputStream.getInstance(split, fs, compressionCodecs);
      long start = split.getStart();
      long end   = start + split.getLength();
      SplitCompressionInputStream cin = in.getSplitCompressionInputStream();
      if ( cin != null ) {
        start = cin.getAdjustedStart();
        end   = cin.getAdjustedEnd();
      }
      ByteMatcher matcher = new ByteMatcher(in, in, end);
      List<Long> ret = new ArrayList<Long>();
      while ( true ) {
        if ( matcher.finished() || !matcher.readUntilMatch(pageBeginPattern, null) ) {
          break;
        }
        ret.add(matcher.getReadBytes() - pageBeginPattern.getBytes("UTF-8").length);
        if ( matcher.finished() || !matcher.readUntilMatch(pageEndPattern, null) ) {
          System.err.println("could not find "+pageEndPattern+", page over a split?  pos=" + matcher.getPos() + " bytes=" + matcher.getReadBytes());
          ret.remove(ret.size() - 1);
          break;
        }
        ret.add(matcher.getReadBytes() - pageEndPattern.getBytes("UTF-8").length);
        reporter.setStatus(String.format("StreamWikiDumpInputFormat: find page %6d start=%d pos=%d end=%d bytes=%d", ret.size(), start, matcher.getPos(), end, matcher.getReadBytes()));
        reporter.incrCounter(WikiDumpCounters.FOUND_PAGES, 1);
      }
      //System.err.println("getPageBytes " + ret);//!
      return ret;
    } finally {
      if ( in != null ) {
        in.close();
      }
    }
  }

  private static void offsetWrite(DataOutputBuffer to, int fromOffset, DataOutputBuffer from) throws IOException {
    if ( from.getLength() <= fromOffset || fromOffset < 0 ) {
      throw new IllegalArgumentException(String.format("invalid offset: offset=%d length=%d", fromOffset, from.getLength()));
    }
    byte[] bytes = new byte[from.getLength() - fromOffset];
    System.arraycopy(from.getData(), fromOffset, bytes, 0, bytes.length);
    to.reset();
    to.write(bytes);
  }
  private static void allWrite(DataOutputBuffer to, DataOutputBuffer from) throws IOException {
    offsetWrite(to, 0, from);
  }

  private static int findIndex(byte[] match, DataOutputBuffer from_) throws IOException {
    // TODO: faster string pattern match (KMP etc)
    int m = 0;
    int i;
    byte[] from = from_.getData();
    for ( i = 0; i < from_.getLength(); ++i ) {
      if ( from[i] == match[m] ) {
        ++m;
      } else {
        m = 0;
      }
      if ( m == match.length ) {
        return i - m + 1;
      }
    }
    // throw new IllegalArgumentException("pattern not found: " + new String(match) + " in " + new String(from));
    System.err.println("pattern not found: " + new String(match) + " in " + new String(from));//!
    return -1;
  }

  private static enum WikiDumpCounters {
    FOUND_PAGES, WRITTEN_REVISIONS, WRITTEN_PAGES
  }

  private static final String pageBeginPattern = "<page>";
  private static final String pageEndPattern   = "</page>";
}
