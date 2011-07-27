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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.streaming.StreamBaseRecordReader;
import org.apache.hadoop.streaming.StreamUtil;

import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.streaming.StreamUtil;

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
  // public List<InputSplit> getSplits(JobContext job) throws IOException {
  //   long minSize = Math.max(getFormatMinSplitSize(), getMinSplitSize(job));
  //   long maxSize = getMaxSplitSize(job);

  //   // generate splits
  //   List<InputSplit> splits = new ArrayList<InputSplit>();
  //   List<FileStatus> files = listStatus(job);
  //   for (FileStatus file: files) {
  //     Path path = file.getPath();
  //     long length = file.getLen();
  //     if (length != 0) {
  //       FileSystem fs = path.getFileSystem(job.getConfiguration());
  //       BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0, length);
  //       if (isSplitable(job, path)) {
  //         long blockSize = file.getBlockSize();
  //         long splitSize = computeSplitSize(blockSize, minSize, maxSize);

  //         long bytesRemaining = length;
  //         while (((double) bytesRemaining)/splitSize > SPLIT_SLOP) {
  //           int blkIndex = getBlockIndex(blkLocations, length-bytesRemaining);
  //           splits.add(makeSplit(path, length-bytesRemaining, splitSize,
  //                                    blkLocations[blkIndex].getHosts()));
  //           bytesRemaining -= splitSize;
  //         }

  //         if (bytesRemaining != 0) {
  //           splits.add(makeSplit(path, length-bytesRemaining, bytesRemaining,
  //                      blkLocations[blkLocations.length-1].getHosts()));
  //         }
  //       } else { // not splitable
  //         splits.add(makeSplit(path, 0, length, blkLocations[0].getHosts()));
  //       }
  //     } else { 
  //       //Create empty hosts array for zero length files
  //       splits.add(makeSplit(path, 0, length, new String[0]));
  //     }
  //   }
  //   // Save the number of input files for metrics/loadgen
  //   job.getConfiguration().setLong(NUM_INPUT_FILES, files.size());
  //   LOG.debug("Total # of splits: " + splits.size());
  //   return splits;
  // }

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
      FSDataInputStream in = fs.open(split.getPath());
      this.revisionBeginPattern = "<revision";
      this.revisionEndPattern   = "</revision>";
      this.pageHeader   = new DataOutputBuffer();
      this.prevRevision = new DataOutputBuffer();
      this.pageFooter = getBuffer("\n</page>\n".getBytes());
      this.revHeader  = getBuffer(this.revisionBeginPattern.getBytes());
      this.firstDummyRevision = getBuffer(" beginningofpage=\"true\"></revision>\n".getBytes());
      this.bufInRev = new DataOutputBuffer();
      this.bufBeforeRev = new DataOutputBuffer();
      
      final org.apache.hadoop.fs.Path file = split.getPath();
      CompressionCodec codec = compressionCodecs.getCodec(file);

      
      if (codec != null) {
        Decompressor decompressor = CodecPool.getDecompressor(codec);
        if (codec instanceof SplittableCompressionCodec) {
          SplittableCompressionCodec scodec = (SplittableCompressionCodec)codec;
          this.pageBytes = getPageBytes(fs, split.getPath(), 
                                        scodec,
                                        decompressor,
                                        split.getStart(), split.getStart() + split.getLength());
          final SplitCompressionInputStream cIn = scodec.createInputStream
            (in, decompressor, split.getStart(), split.getStart() + split.getLength(),
             SplittableCompressionCodec.READ_MODE.BYBLOCK);
          this.start = cIn.getAdjustedStart();
          this.end   = cIn.getAdjustedEnd();
          this.in = cIn;
          this.filePosition = cIn;
          this.fileBytes = new BytesRead();
        } else {
          this.start = split.getStart();
          this.end   = split.getStart() + split.getLength();
          this.in = codec.createInputStream(in, decompressor);
          this.filePosition = in;
          this.fileBytes  = new BytesRead();
          this.pageBytes = getPageBytes(fs, split.getPath(), this.start, this.end);
        }
      } else {
        this.start = split.getStart();
        this.end   = split.getStart() + split.getLength();
        this.in = in;
        this.filePosition = in;
        this.fileBytes    = new BytesRead();
        this.filePosition.seek(this.start);
        this.pageBytes = getPageBytes(fs, split.getPath(), this.start, this.end);
      }
      //System.err.println("StreamWikiDumpInputFormat: start=" + this.start + " end=" + this.end + " pos=" + this.filePosition.getPos() + " pageBytes=" + pageBytes);
      System.err.println("StreamWikiDumpInputFormat: split=" + split + " start=" + this.start + " end=" + this.end + " pos=" + this.filePosition.getPos() + " pageBytes=" + pageBytes);//!
      LOG.info("StreamWikiDumpInputFormat: split=" + split + " start=" + this.start + " end=" + this.end + " pos=" + this.filePosition.getPos() + " pageBytes=" + pageBytes);
      this.pageHeader.reset();
      init();
    }

    public void init() throws IOException {
      this.prevRevision.reset();
      this.prevRevision.write(this.firstDummyRevision.getData(), 0, this.firstDummyRevision.getLength());
      this.currentPageNum = 0;
      this.seekNextRecordBoundary();
    }
  
    public Text createKey() {
      return new Text();
    }
    
    public Text createValue() {
      return new Text();
    }

    public synchronized void close() throws IOException {
        this.in.close();
      }

    public float getProgress() throws IOException {
      if (this.end == this.start) {
        return 1.0f;
      } else {
        return ((float)(this.getPos() - this.start)) / ((float)(this.end - this.start));
      }
    }

    public synchronized long getPos() throws IOException {
      return this.filePosition.getPos();
    }

    public synchronized long getReadBytes() throws IOException {
      return this.fileBytes.getPos();
    }

    public synchronized boolean next(Text key, Text value) throws IOException {
      if (this.getPos() >= this.end) {
        return false;
      }

      //System.err.println("1 page end check");//!
    
      //System.err.println("2 move to rev from: " + this.getReadBytes());//!
      this.bufBeforeRev.reset();
      if (!readUntilMatch(this.revisionBeginPattern, this.bufBeforeRev)) { // move to the beginning of the next revision
        return false;
      }
      System.err.println("2.1 move to rev to: " + this.getReadBytes());//!

      System.err.println("4.5 check if exceed: " + this.getReadBytes() + " " + nextPageBegin() + " " + prevPageEnd());//!
      if ( this.getReadBytes() >= this.nextPageBegin() ) {
        this.pageHeader.reset();
        int off = (int)(this.nextPageBegin() - this.prevPageEnd());
        offsetWrite(this.pageHeader, off, this.bufBeforeRev);
        this.prevRevision.reset();
        this.prevRevision.write(this.firstDummyRevision.getData(), 0, this.firstDummyRevision.getLength());
        this.currentPageNum++;
        //System.err.println("4.6 exceed");//!
      }

      //System.err.println("4 read rev from: " + this.getReadBytes());//!
      this.bufInRev.reset();
      if (this.getPos() >= this.end || !readUntilMatch(this.revisionEndPattern, this.bufInRev)) { // store the revision
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
      value.set("");

      this.prevRevision.reset();
      this.prevRevision.write(this.bufInRev.getData(), 0, this.bufInRev.getLength());

      return true;
    }

    public void seekNextRecordBoundary() throws IOException {
      if ( this.getReadBytes() < this.nextPageBegin() ) {
        long len = this.nextPageBegin() - this.getReadBytes();
        this.fileBytes.proceed(len);
        this.in.skip(len);
      }
    }
    private boolean readUntilMatch(String textPat, DataOutputBuffer outBufOrNull) throws IOException {
      //System.err.println("read " + textPat + " pos=" + this.filePosition.getPos() + " bytes=" + this.fileBytes.getPos());
      return readUntilMatch_(this.in, this.filePosition, this.fileBytes, this.end, textPat, outBufOrNull);
    }
    private long nextPageBegin() {
      if ( this.currentPageNum * 2 < this.pageBytes.size() - 1 ) {
        return this.pageBytes.get(this.currentPageNum * 2);
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
      } else if ( this.currentPageNum * 2 - 1 < this.pageBytes.size() - 1 ) {
        return this.pageBytes.get(this.currentPageNum * 2 - 1);
      } else {
        return this.end;
      }
    }

    private int currentPageNum;
    private final long start;
    private final long end;
    private final List<Long> pageBytes;
    private final BytesRead fileBytes;
    private final Seekable  filePosition;
    private final InputStream in;
    private final String revisionBeginPattern;
    private final String revisionEndPattern;
    private final DataOutputBuffer pageHeader;
    private final DataOutputBuffer revHeader;
    private final DataOutputBuffer prevRevision;
    private final DataOutputBuffer pageFooter;
    private final DataOutputBuffer firstDummyRevision;
    private final DataOutputBuffer bufInRev;
    private final DataOutputBuffer bufBeforeRev;
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

  private static boolean readUntilMatch_(InputStream in, Seekable pos, BytesRead bytes, long end, String textPat, DataOutputBuffer outBufOrNull) throws IOException {
    byte[] match = textPat.getBytes("UTF-8");
    int i = 0;
    while (true) {
      int b = in.read();
      bytes.proceed(1);
      // end of file:
      if (b == -1)
        return false;
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
      if (outBufOrNull == null && i == 0 && pos.getPos() >= end)
        return false;
    }
  }

  private static List<Long> getPageBytes(InputStream in, Seekable pos, BytesRead bytes, long start, long end) throws IOException {
    List<Long> ret = new ArrayList<Long>();
    while ( true ) {
      if ( pos.getPos() >= end  || !readUntilMatch_(in, pos, bytes, end, pageBeginPattern, null) ) {
        break;
      }
      ret.add(bytes.getPos() - pageBeginPattern.length());
      if ( pos.getPos() >= end  || !readUntilMatch_(in, pos, bytes, end, pageEndPattern, null) ) {
        System.err.println("could not find "+pageEndPattern+", page over a split?  pos=" + pos + " bytes=" + bytes);
        ret.remove(ret.size() - 1);
        break;
      }
      ret.add(bytes.getPos() - pageEndPattern.length());
    }
    return ret;
  }

  private static List<Long> getPageBytes(FileSystem fs, Path path, long start, long end) throws IOException {
    FSDataInputStream in = fs.open(path);
    return getPageBytes(in, in, new BytesRead(), start, end);
  }  
  private static List<Long> getPageBytes(FileSystem fs, Path path, SplittableCompressionCodec codec, Decompressor decompressor, long start, long end) throws IOException {
    SplitCompressionInputStream in = codec.createInputStream
      (fs.open(path), decompressor, start, end,
       SplittableCompressionCodec.READ_MODE.BYBLOCK);
    return getPageBytes(in, in, new BytesRead(), in.getAdjustedStart(), in.getAdjustedEnd());
  }
  private static byte[] offset(byte[] bytes, int offset) {
    byte[] ret = new byte[bytes.length - offset];
    System.arraycopy(bytes, offset, ret, 0, ret.length);
    return ret;
  }
  private static void offsetWrite(DataOutputBuffer to, int fromOffset, DataOutputBuffer from) throws IOException {
    byte[] bytes = new byte[from.getLength() - fromOffset];
    System.arraycopy(from.getData(), fromOffset, bytes, 0, bytes.length);
    to.write(bytes);
  }

  private static class BytesRead {
    long p = 0;
    public long getPos() {
      return p;
    }
    public void reset() {
      this.p = 0;
    }
    public void proceed(long n) {
      this.p += n;
    }
  }
  private static final String pageBeginPattern = "<page>";
  private static final String pageEndPattern   = "</page>";
}
