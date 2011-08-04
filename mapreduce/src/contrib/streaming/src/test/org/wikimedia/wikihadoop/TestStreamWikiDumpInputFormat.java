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

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.io.compress.*;

import org.junit.Test;
import static org.junit.Assert.*;

public class TestStreamWikiDumpInputFormat {

  private static Configuration conf = new Configuration();

  @Test
  public void testFormatWithOneSplitUncompressed() throws IOException {
    JobConf job = new JobConf(conf);
    FileSystem fs = FileSystem.getLocal(conf);
    Path dir = new Path(System.getProperty("test.build.data", ".") + "/mapred");
    Path txtFile = new Path(dir, "auto.txt");

    fs.delete(dir, true);

    StreamWikiDumpInputFormat.setInputPaths(job, dir);

    Writer txtWriter = new OutputStreamWriter(fs.create(txtFile));
    try {
      txtWriter.write("<tree><page><header/><revision>first</revision><revision>second</revision><revision>third</revision><revision>n</revision><revision>n+1</revision></page>\n"
                      + "<page><longlongheader/><revision>e</revision></page>\n"
                      + "<page><long-long-long-header/><revision>f</revision></page></tree>\n");
    } finally {
      txtWriter.flush();
      txtWriter.close();
    }

    StreamWikiDumpInputFormat format = new StreamWikiDumpInputFormat();
    format.configure(job);
    List<String> found = collect(format, job, 1);

    assertEquals(Arrays.asList(new String[]{
          "<page><header/><revision beginningofpage=\"true\"></revision>\n<revision>first</revision>\n</page>\n",
          "<page><header/><revision>first</revision><revision>second</revision>\n</page>\n",
          "<page><header/><revision>second</revision><revision>third</revision>\n</page>\n",
          "<page><header/><revision>third</revision><revision>n</revision>\n</page>\n",
          "<page><header/><revision>n</revision><revision>n+1</revision>\n</page>\n",
          "<page><longlongheader/><revision beginningofpage=\"true\"></revision>\n<revision>e</revision>\n</page>\n",
          "<page><long-long-long-header/><revision beginningofpage=\"true\"></revision>\n<revision>f</revision>\n</page>\n",
        }), found);
  }

  @Test
    public void testFormatWithOneSplitUncompressedFragmentsAndSpaces() throws IOException {
    JobConf job = new JobConf(conf);
    FileSystem fs = FileSystem.getLocal(conf);
    Path dir = new Path(System.getProperty("test.build.data", ".") + "/mapred");
    Path txtFile = new Path(dir, "auto.txt");

    fs.delete(dir, true);

    StreamWikiDumpInputFormat.setInputPaths(job, dir);

    Writer txtWriter = new OutputStreamWriter(fs.create(txtFile));
    try {
      txtWriter.write("foo-bar-foo-bar-<revision>foo-bar-foo-bar</revision>    </page> <page> <revision>1</revision></page>     <page>     <header/> <revision>first</revision><revision>second</revision><revision>third</revision><revision>n</revision><revision>n+1</revision>    </page>\n" + "  <page>  <longlongheader/><revision>e</revision>    </page>     <page><revision>1</revision>foo-bar-foo-bar-foo-bar-foo-bar-foo-bar-foo-bar-foo-bar-foo-bar-foo-bar-foo-bar-foo-bar-foo-bar-foo-bar-foo-bar-foo-bar-foo-bar-foo-bar-foo-bar-foo-bar-foo-bar-foo-bar-foo-bar-foo-bar-foo-bar-foo-bar-foo-bar-foo-bar-foo-bar-foo-bar-foo-bar-foo-bar-foo-bar-foo-bar-foo-bar-foo-bar-foo-bar-foo-bar-foo-bar-foo-bar-foo-bar-foo-bar-foo-bar-foo-bar-foo-bar-<revision>2</revision>\n");
    } finally {
      txtWriter.flush();
      txtWriter.close();
    }

    StreamWikiDumpInputFormat format = new StreamWikiDumpInputFormat();
    format.configure(job);
    List<String> found = collect(format, job, 1);
    assertEquals(Arrays.asList(new String[]{
          "<page> <revision beginningofpage=\"true\"></revision>\n<revision>1</revision>\n</page>\n",
          "<page>     <header/> <revision beginningofpage=\"true\"></revision>\n<revision>first</revision>\n</page>\n",
          "<page>     <header/> <revision>first</revision><revision>second</revision>\n</page>\n",
          "<page>     <header/> <revision>second</revision><revision>third</revision>\n</page>\n",
          "<page>     <header/> <revision>third</revision><revision>n</revision>\n</page>\n",
          "<page>     <header/> <revision>n</revision><revision>n+1</revision>\n</page>\n",
          "<page>  <longlongheader/><revision beginningofpage=\"true\"></revision>\n<revision>e</revision>\n</page>\n",
        }), found);
  }

  private static byte[] bzip2(byte[] bytes) throws IOException {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    CompressionOutputStream c = new BZip2Codec().createOutputStream(os);
    c.write(bytes);
    c.finish();
    c.flush();
    return os.toByteArray();
  }

  @Test
  public void testFormatWithOneSplitCompressed() throws IOException {
    JobConf job = new JobConf(conf);
    FileSystem fs = FileSystem.getLocal(conf);
    Path dir = new Path(System.getProperty("test.build.data", ".") + "/mapred");
    Path txtFile = new Path(dir, "auto.bz2");

    fs.delete(dir, true);

    StreamWikiDumpInputFormat.setInputPaths(job, dir);

    OutputStream writer = fs.create(txtFile);
    try {
      writer.write(bzip2(("<tree><page><header/><revision>first</revision><revision>second</revision><revision>third</revision><revision>n</revision><revision>n+1</revision></page>\n" + "<page><longlongheader/><revision>e</revision></page></tree>\n").getBytes()));
    } finally {
      writer.flush();
      writer.close();
    }

    StreamWikiDumpInputFormat format = new StreamWikiDumpInputFormat();
    format.configure(job);
    List<String> found = collect(format, job, 1);
    assertEquals(Arrays.asList(new String[]{
          "<page><header/><revision beginningofpage=\"true\"></revision>\n<revision>first</revision>\n</page>\n",
          "<page><header/><revision>first</revision><revision>second</revision>\n</page>\n",
          "<page><header/><revision>second</revision><revision>third</revision>\n</page>\n",
          "<page><header/><revision>third</revision><revision>n</revision>\n</page>\n",
          "<page><header/><revision>n</revision><revision>n+1</revision>\n</page>\n",
          "<page><longlongheader/><revision beginningofpage=\"true\"></revision>\n<revision>e</revision>\n</page>\n",
        }), found);
  }

  @Test
  public void testSplitUncompressed() throws IOException {
    JobConf job = new JobConf(conf);
    FileSystem fs = FileSystem.getLocal(conf);
    Path dir = new Path(System.getProperty("test.build.data", ".") + "/mapred");
    Path txtFile = new Path(dir, "auto.txt");

    fs.delete(dir, true);

    StreamWikiDumpInputFormat.setInputPaths(job, dir);

    Writer txtWriter = new OutputStreamWriter(fs.create(txtFile));
    try {
      txtWriter.write("<page><revision>AB</revision>          \n");
      txtWriter.write("<revision>C</revision>           <revisio");
      txtWriter.write("n>DER</revision></page> <page><revision>");
      txtWriter.write("long-long-long-long-long-long-long-long-");
      txtWriter.write("long-long-long-long-long-long-long-long-");
      txtWriter.write("long-long-long-revision. </revision></pa");
      txtWriter.write("ge>\n");
    } finally {
      txtWriter.flush();
      txtWriter.close();
    }

    StreamWikiDumpInputFormat format = new StreamWikiDumpInputFormat();
    format.configure(job);

    for ( Integer len: new Integer[]{20, 40, 50} ) {
      long size = 0;
      long n = 0;
      for ( InputSplit is: format.getSplits(job, fs.getFileStatus(txtFile), "</page>", len) ) {
        FileSplit split = (FileSplit)is;
        size += split.getLength();
        String str = new String(read(split, job));
        str = str.trim();
        if ( str.length() == 0 ) continue;
        if ( str.indexOf("<page>") >= 0 && !(str.indexOf("</page>") >= 0) ) {
          assertTrue("page fragment in \"" + str + "\"", false);
        }
        assertTrue("no </page> in \"" + str + "\"", str.indexOf("</page>") >= 0);
        ++n;
      }
      assertTrue("total size is too small: expected: " + fs.getFileStatus(txtFile).getLen() + ", found: " + size, fs.getFileStatus(txtFile).getLen() <= size);
      assertTrue("number of splits is too large: expected: " + size/len + ", found: " + n, size/len >= n);
    }
  }

  @Test
  public void testSplitCompressed() throws IOException {
    JobConf job = new JobConf(conf);
    FileSystem fs = FileSystem.getLocal(conf);
    Path dir = new Path(System.getProperty("test.build.data", ".") + "/mapred");
    Path txtFile = new Path(dir, "auto.bz2");

    fs.delete(dir, true);

    StreamWikiDumpInputFormat.setInputPaths(job, dir);

    OutputStream writer = fs.create(txtFile);

    try {
      writer.write(bzip2(("<page><revision>AB</revision>          \n"
                             + "<revision>C</revision>           <revisio"
                             + "n>DER</revision></page> <page><revision>"
                             + "long-long-long-long-long-long-long-long-"
                             + "long-long-long-long-long-long-long-long-"
                             + "long-long-long-revision. </revision></pa"
                             + "ge>\n").getBytes("UTF-8")));
    } finally {
      writer.flush();
      writer.close();
    }

    StreamWikiDumpInputFormat format = new StreamWikiDumpInputFormat();
    format.configure(job);

    for ( Integer len: new Integer[]{10, 20, 40} ) {
      long size = 0;
      for ( InputSplit is: format.getSplits(job, fs.getFileStatus(txtFile), "</page>", len) ) {
        FileSplit split = (FileSplit)is;
        size += split.getLength();
        String str = new String(read(split, job));
        str = str.trim();
        if ( str.length() == 0 ) continue;
        assertTrue("no </page> in \"" + str + "\"", str.indexOf("</page>") >= 0);
        assertTrue("no <page> in \"" + str + "\"", str.indexOf("<page>") >= 0);
      }
      assertTrue("total size is too small: expected: " + fs.getFileStatus(txtFile).getLen() + ", found: " + size, fs.getFileStatus(txtFile).getLen() <= size);
    }
  }

  @Test
  public void testFormatWithCompressed() throws IOException {
    JobConf job = new JobConf(conf);
    FileSystem fs = FileSystem.getLocal(conf);
    Path dir = new Path(System.getProperty("test.build.data", ".") + "/mapred");
    Path txtFile = new Path(dir, "auto.bz2");

    fs.delete(dir, true);

    StreamWikiDumpInputFormat.setInputPaths(job, dir);

    OutputStream writer = fs.create(txtFile);
    try {
      writer.write(bzip2(("<tree><page><header/><revision>first</revision>bugbug<revision>second</revision><revision>third</revision><revision>n</revision><revision>n+1</revision></page>\n"
                          + "<page><longlongheader/><revision>e</revision></page></tree>\n").getBytes()));
    } finally {
      writer.flush();
      writer.close();
    }

    StreamWikiDumpInputFormat format = new StreamWikiDumpInputFormat();
    format.configure(job);
    for ( Integer n: new Integer[]{1,2,3,4,5} ) {
      List<String> found = collect(format, job, n);
      assertEquals(Arrays.asList(new String[]{
            "<page><header/><revision beginningofpage=\"true\"></revision>\n<revision>first</revision>\n</page>\n",
            "<page><header/><revision>first</revision><revision>second</revision>\n</page>\n",
            "<page><header/><revision>second</revision><revision>third</revision>\n</page>\n",
            "<page><header/><revision>third</revision><revision>n</revision>\n</page>\n",
            "<page><header/><revision>n</revision><revision>n+1</revision>\n</page>\n",
            "<page><longlongheader/><revision beginningofpage=\"true\"></revision>\n<revision>e</revision>\n</page>\n",
          }), found);
    }
  }

  private static List<String> collect(FileInputFormat<Text,Text> format, JobConf job, int n) throws IOException {
    List<String> found = new ArrayList<String>();
    for (InputSplit split : format.getSplits(job, n)) {
      RecordReader<Text,Text> reader = format.getRecordReader(split, job, Reporter.NULL);
      Text key = reader.createKey();
      Text value = reader.createValue();
      try {
        while (reader.next(key, value)) {
          found.add(key.toString());
        }
      } finally {
        reader.close();
      }
    }
    return found;
  }

  private static String read(FileSplit split, JobConf job) throws IOException {
    StringBuffer buff = new StringBuffer();
    SeekableInputStream is = SeekableInputStream.getInstance(split, FileSystem.getLocal(conf), new CompressionCodecFactory(job));
    byte[] buf = new byte[2048];
    int len = 0;
    while ( (len=is.read(buf)) >= 0 ) {
      buff.append(new String(buf, 0, len));
    }
    return buff.toString();
  }

  private static String snip(String str, int snip) {
    if ( str.length() > snip ) {
      str = str.substring(0, snip/2) + " ... " + str.substring(str.length() - snip/2, str.length());
    }
    return str.replace("\n", "\\n");
  }

  public static void main(String[] args) throws IOException {
    Configuration conf = new Configuration();
    JobConf job = new JobConf(conf);
    FileSystem fs = FileSystem.getLocal(conf);
    Path dir = new Path(System.getProperty("test.build.data", ".") + "/mapred");
    Path txtFile = new Path(dir, args[0]);

    fs.delete(dir, true);

    StreamWikiDumpInputFormat.setInputPaths(job, dir);

    OutputStream os = fs.create(txtFile);
    
    InputStream is = new FileInputStream(args[0]);
    byte[] buff = new byte[1024];
    int le = 0;
    while ( (le = is.read(buff)) > 0 ) {
      os.write(buff, 0, le);
    }
    os.close();

    StreamWikiDumpInputFormat format = new StreamWikiDumpInputFormat();
    format.configure(job);
    Text key = new Text();
    Text value = new Text();
    int len = 400;
    if ( System.getProperty("sniplen") != null ) {
      len = Integer.parseInt(System.getProperty("sniplen"));
    }
    int num  = 1;
    if ( System.getProperty("splitnum") != null ) {
      num = Integer.parseInt(System.getProperty("splitnum"));
    }

    for (InputSplit split : format.getSplits(job, num)) {
      System.err.println(split);
      RecordReader reader = format.getRecordReader(split, job, Reporter.NULL);
      try {
        while (reader.next(key, value)) {
          System.err.println("key: (" + key.toString().length() + ") " + snip(key.toString(), len));
          System.out.println(key.toString());
        }
      } finally {
        reader.close();
      }
    }
  }

}
