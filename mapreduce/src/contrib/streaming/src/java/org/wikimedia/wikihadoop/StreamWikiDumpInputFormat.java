/**
 * Copyright 2011 Yusuke Matsubara
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.compress.*;

/** A InputFormat implementation that splits a Wikimedia Dump File into page fragments, and emits them as input records.
 * The record reader embedded in this input format converts a page into a sequence of page-like elements, each of which contains two consecutive revisions.  Output is given as keys with empty values.
 *
 * For example,  Given the following input containing two pages and four revisions,
 * <pre><code>
 *  &lt;page&gt;
 *    &lt;title&gt;ABC&lt;/title&gt;
 *    &lt;id&gt;123&lt;/id&gt;
 *    &lt;revision&gt;
 *      &lt;id&gt;100&lt;/id&gt;
 *      ....
 *    &lt;/revision&gt;
 *    &lt;revision&gt;
 *      &lt;id&gt;200&lt;/id&gt;
 *      ....
 *    &lt;/revision&gt;
 *    &lt;revision&gt;
 *      &lt;id&gt;300&lt;/id&gt;
 *      ....
 *    &lt;/revision&gt;
 *  &lt;/page&gt;
 *  &lt;page&gt;
 *    &lt;title&gt;DEF&lt;/title&gt;
 *    &lt;id&gt;456&lt;/id&gt;
 *    &lt;revision&gt;
 *      &lt;id&gt;400&lt;/id&gt;
 *      ....
 *    &lt;/revision&gt;
 *  &lt;/page&gt;
 * </code></pre>
 * it will produce four keys like this:
 * <pre><code>
 *  &lt;page&gt;
 *    &lt;title&gt;ABC&lt;/title&gt;
 *    &lt;id&gt;123&lt;/id&gt;
 *    &lt;revision&gt;&lt;revision beginningofpage="true"&gt;&lt;text xml:space="preserve"&gt;&lt;/text&gt;&lt;/revision&gt;&lt;revision&gt;
 *      &lt;id&gt;100&lt;/id&gt;
 *      ....
 *    &lt;/revision&gt;
 *  &lt;/page&gt;
 * </code></pre>
 * <pre><code>
 *  &lt;page&gt;
 *    &lt;title&gt;ABC&lt;/title&gt;
 *    &lt;id&gt;123&lt;/id&gt;
 *    &lt;revision&gt;
 *      &lt;id&gt;100&lt;/id&gt;
 *      ....
 *    &lt;/revision&gt;
 *    &lt;revision&gt;
 *      &lt;id&gt;200&lt;/id&gt;
 *      ....
 *    &lt;/revision&gt;
 *  &lt;/page&gt;
 * </code></pre>
 * <pre><code>
 *  &lt;page&gt;
 *    &lt;title&gt;ABC&lt;/title&gt;
 *    &lt;id&gt;123&lt;/id&gt;
 *    &lt;revision&gt;
 *      &lt;id&gt;200&lt;/id&gt;
 *      ....
 *    &lt;/revision&gt;
 *    &lt;revision&gt;
 *      &lt;id&gt;300&lt;/id&gt;
 *      ....
 *    &lt;/revision&gt;
 *  &lt;/page&gt;
 * </code></pre>
 * <pre><code>
 *  &lt;page&gt;
 *    &lt;title&gt;DEF&lt;/title&gt;
 *    &lt;id&gt;456&lt;/id&gt;
 *    &lt;revision&gt;&lt;revision beginningofpage="true"&gt;&lt;text xml:space="preserve"&gt;&lt;/text&gt;&lt;/revision&gt;&lt;revision&gt;
 *      &lt;id&gt;400&lt;/id&gt;
 *      ....
 *    &lt;/revision&gt;
 *  &lt;/page&gt;
 * </code></pre>
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
  @Override public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    LOG.info("StreamWikiDumpInputFormat.getSplits job=" + job + " n=" + numSplits);
    InputSplit[] oldSplits = super.getSplits(job, numSplits);
    List<InputSplit> splits = new ArrayList<InputSplit>();
    FileStatus[] files = listStatus(job);
    // Save the number of input files for metrics/loadgen
    job.setLong(NUM_INPUT_FILES, files.length);
    long totalSize = 0;                           // compute total size
    for (FileStatus file: files) {                // check we have valid files
      if (file.isDirectory()) {
        throw new IOException("Not a file: "+ file.getPath());
      }
      totalSize += file.getLen();
    }
    long minSize = job.getLong(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.SPLIT_MINSIZE, 1);
    long goalSize = totalSize / (numSplits == 0 ? 1 : numSplits);
    for (FileStatus file: files) {
      if (file.isDirectory()) {
        throw new IOException("Not a file: "+ file.getPath());
      }
      long blockSize = file.getBlockSize();
      long splitSize = computeSplitSize(goalSize, minSize, blockSize);
      LOG.info(String.format("goalsize=%d splitsize=%d blocksize=%d", goalSize, splitSize, blockSize));
      //System.err.println(String.format("goalsize=%d splitsize=%d blocksize=%d", goalSize, splitSize, blockSize));
      for (InputSplit x: getSplits(job, file, pageBeginPattern, splitSize) ) 
        splits.add(x);
    }
    return splits.toArray(new InputSplit[splits.size()]);
  }


  private FileSplit makeSplit(Path path, long start, long size, NetworkTopology clusterMap, BlockLocation[] blkLocations) throws IOException {
    return makeSplit(path, start, size,
                     getSplitHosts(blkLocations, start, size, clusterMap));
  }

  public List<InputSplit> getSplits(JobConf job, FileStatus file, String pattern, long splitSize) throws IOException {
    NetworkTopology clusterMap = new NetworkTopology();
    List<InputSplit> splits = new ArrayList<InputSplit>();
    Path path = file.getPath();
    long length = file.getLen();
    FileSystem fs = file.getPath().getFileSystem(job);
    BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0, length);
    if ((length != 0) && isSplitable(fs, path)) { 
      
      long bytesRemaining = length;
      SeekableInputStream in = SeekableInputStream.getInstance
        (path, 0, length, fs, this.compressionCodecs);
      SplitCompressionInputStream is = in.getSplitCompressionInputStream();
      long start = 0;
      long start_offset = 0;
      if ( is != null ) {
        start = is.getAdjustedStart();
        length = is.getAdjustedEnd();
        is.close();
        in = null;
      }
      LOG.info("locations=" + Arrays.asList(blkLocations));
      ByteMatcher matcher = null;
      FileSplit split = null;
      long lastSplitSize = splitSize;
      Set<Long> processedPageEnds = new HashSet<Long>();
      double factor = 2.0;
      while (((double) bytesRemaining)/lastSplitSize > factor  &&  bytesRemaining > 0) {
        if (matcher == null) {
          long st = Math.min(start + lastSplitSize, length - 1);
          split = makeSplit(path,
                            st,
                            Math.min(lastSplitSize, length - st),
                            clusterMap, blkLocations);
          System.err.println("split move to: " + split);
          lastSplitSize = splitSize;
          if ( in != null )
            in.close();
          if ( split.getLength() <= 1 ) {
            break;
          }
          in = SeekableInputStream.getInstance(split,
                                               fs, this.compressionCodecs);
          SplitCompressionInputStream cin = in.getSplitCompressionInputStream();
          matcher = new ByteMatcher(in);
        }
        // read until the next page end in the look-ahead split
        if ( matcher.readUntilMatch(pageEndPattern, null, split.getStart() + split.getLength())
             &&  matcher.getPos() > matcher.getLastUnmatchPos()
             &&  !processedPageEnds.contains(matcher.getPos()) ) {
          splits.add(makeSplit(path, start, matcher.getPos() - start, clusterMap, blkLocations));
          System.err.println(path + ": #" + splits.size() + " " + pageEndPattern + " found: pos=" + matcher.getPos() + " last=" + matcher.getLastUnmatchPos() + " read=" + matcher.getReadBytes() + " current=" + start + " remaining=" + bytesRemaining + " split=" + split);
          processedPageEnds.add(matcher.getPos());
          long newstart = Math.max(matcher.getLastUnmatchPos(), start);
          bytesRemaining -= newstart - start;
          start = newstart;
          matcher = null;
        } else {
          if (matcher.getPos() >= length)
            break;
          System.err.println(path + ": #" + splits.size() + " " + pageEndPattern + " not found: pos=" + matcher.getPos() + " last=" + matcher.getLastUnmatchPos() + " read=" + matcher.getReadBytes() + " current=" + start + " remaining=" + bytesRemaining + " split=" + split);
          split = makeSplit(path,
                            split.getStart(),
                            Math.min((long)(split.getLength() * factor), length - split.getStart()),
                            clusterMap, blkLocations);
          lastSplitSize = split.getLength();
        }
      }
      
      if (bytesRemaining > 0) {
        System.err.println(pageEndPattern + " remaining: pos=" + (length-bytesRemaining) + " end=" + length);
        splits.add(makeSplit(path, length-bytesRemaining, bytesRemaining, 
                             blkLocations[blkLocations.length-1].getHosts()));
      }
      if ( in != null )
        in.close();
    } else if (length != 0) {
      splits.add(makeSplit(path, 0, length, clusterMap, blkLocations));
    } else { 
      //Create empty hosts array for zero length files
      splits.add(makeSplit(path, 0, length, new String[0]));
    }
    return splits;
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
      this.firstDummyRevision = getBuffer(" beginningofpage=\"true\"><text xml:space=\"preserve\"></text></revision>\n".getBytes("UTF-8"));
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
        this.end   = sin.getAdjustedEnd() + 1;
      }
      this.reporter = reporter;

      allWrite(this.prevRevision, this.firstDummyRevision);
      this.currentPageNum = -1;
      this.pageBytes = getPageBytes(this.split, this.fs, compressionCodecs, this.reporter);

      this.istream = SeekableInputStream.getInstance(this.split, this.fs, compressionCodecs);
      this.matcher = new ByteMatcher(this.istream, this.istream);
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
        rate = ((float)(this.getPos() - this.start)) / ((float)(this.end - this.start));
      }
      return rate;
    }
    
    @Override public long getPos() throws IOException {
      return this.matcher.getPos();
    }
    
    public synchronized long getReadBytes() throws IOException {
        return this.matcher.getReadBytes();
      }
    
    @Override synchronized public boolean next(Text key, Text value) throws IOException {
      //LOG.info("StreamWikiDumpInputFormat: split=" + split + " start=" + this.start + " end=" + this.end + " pos=" + this.getPos());

        if ( this.nextPageBegin() < 0 ) {
          return false;
        }
      
      //System.err.println("0.2 check pos="+this.getPos() + " end="+this.end);//!
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
        } else {
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
      this.reporter.setStatus("StreamWikiDumpInputFormat: write new record pos=" + this.getPos() + " bytes=" + this.getReadBytes() + " next=" + this.nextPageBegin() + " prev=" + this.prevPageEnd());
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
      return this.matcher.readUntilMatch(textPat, outBufOrNull, this.end);
    }
    private long tailPageEnd() {
      if ( this.pageBytes.size() > 0 ) {
        return this.pageBytes.get(this.pageBytes.size() - 1);
      } else {
        return 0;
      }
    }
    private long nextPageBegin() {
      if ( (this.currentPageNum + 1) * 2 < this.pageBytes.size() ) {
        return this.pageBytes.get((this.currentPageNum + 1) * 2);
      } else {
        throw new IllegalArgumentException();
      }
    }
    private long prevPageEnd() {
      if ( this.currentPageNum == 0 ) {
        if ( this.pageBytes.size() > 0 ) {
          return this.pageBytes.get(0);
        } else {
          return 0;
        }
      } else if ( this.currentPageNum * 2 - 1 <= this.pageBytes.size() - 1 ) {
        return this.pageBytes.get(this.currentPageNum * 2 - 1);
      } else {
        return this.pageBytes.get(this.pageBytes.size() - 1);
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
        end   = cin.getAdjustedEnd() + 1;
      }
      ByteMatcher matcher = new ByteMatcher(in, in);
      List<Long> ret = new ArrayList<Long>();
      while ( true ) {
        if ( matcher.getPos() >= end || !matcher.readUntilMatch(pageBeginPattern, null, end) ) {
          break;
        }
        ret.add(matcher.getReadBytes() - pageBeginPattern.getBytes("UTF-8").length);
        if ( matcher.getPos() >= end || !matcher.readUntilMatch(pageEndPattern, null, end) ) {
          System.err.println("could not find "+pageEndPattern+", page over a split?  pos=" + matcher.getPos() + " bytes=" + matcher.getReadBytes());
          //ret.add(end);
          break;
        }
        ret.add(matcher.getReadBytes() - pageEndPattern.getBytes("UTF-8").length);
        String report = String.format("StreamWikiDumpInputFormat: find page %6d start=%d pos=%d end=%d bytes=%d", ret.size(), start, matcher.getPos(), end, matcher.getReadBytes());
        reporter.setStatus(report);
        reporter.incrCounter(WikiDumpCounters.FOUND_PAGES, 1);
        LOG.info(report);
      }
      if ( ret.size() % 2 == 0 ) {
        ret.add(matcher.getReadBytes());
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
    System.err.println("pattern not found: " + new String(match) + " in " + new String(from, 0, from_.getLength()));//!
    return -1;
  }

  private static enum WikiDumpCounters {
    FOUND_PAGES, WRITTEN_REVISIONS, WRITTEN_PAGES
  }

  private static final String pageBeginPattern = "<page>";
  private static final String pageEndPattern   = "</page>";
}
