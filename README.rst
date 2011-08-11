
=====================
WikiHadoop
=====================
--------------------------------------------------------------------------------------------
Stream-based InputFormat for processing the compressed XML dumps of Wikipedia with Hadoop
--------------------------------------------------------------------------------------------

 :Homepage: http://github.com/whym/wikihadoop
 :Date: 2011-08-05
 :Version: 0.0.1

Overview
==============================

Wikipedia XML dumps with complete edit histories [#]_ have been difficult to process because of its exceptional size and structure.  While a "page" is a common processing unit, one Wikipedia page may contain more than gigabytes of text when the edit history is very long.

This software provides an ``InputFormat`` for `Hadoop Streaming`_ Interface that processes Wikipedia bzip2 XML dumps in a streaming manner.  Using this ``InputFormat``, the content of every page is fed to a mapper via standard input and output without using too much memory.

.. _Hadoop Common: http://github.com/apache/hadoop-common
.. _Hadoop Streaming: http://hadoop.apache.org/common/docs/current/streaming.html
.. _Apache Hadoop: http://hadoop.apache.org
.. _Apache Ant: http://ant.apache.org
.. _WikiHadoop: http://github.com/whym/wikihadoop

.. [#] For example, pages-meta-history1.xml.bz2, pages-meta-history2.xml.bz2, etc, provided at http://dumps.wikimedia.org/enwiki/20110803/ has sizes of more than several gigabytes in compressed forms.

How to use
==============================

1. Download WikiHadoop_ extract the source tree.  Confirm there is a directory called ``mapreduce``.

2. Download `Hadoop Common`_ and extract the source tree.  Confirm there is a directory called ``mapreduce``.

3. Move to the top directory of the source tree of your copy of Hadoop Common.

4. Merge the ``mapreduce`` directory of your copy of WikiHadoop into that of Hadoop Common. ::
    
      rsync -r ../wikihadoop/mapreduce/ mapreduce/      

5. Move to the directory called ``mapreduce/src/contrib/streaming`` under the source tree of Hadoop Common. ::
    
      cd mapreduce/src/contrib/streaming

6. Run Ant to build a jar file. [#]_ ::
    
      ant jar

   If it does not compile, try using the branch of Hadoop 0.21. Run ``git checkout branch-0.21`` and return to 3.

7. Find the jar file at ``mapreduce/build/contrib/streaming/hadoop-${version}-streaming.jar`` under the Hadoop common source tree.

8. Use it as ``hadoop-streaming.jar`` in the manner explained at `Hadoop Streaming`_.  Specify WikiHadoop as the input format with an option ``-inputformat org.wikimedia.wikihadoop.StreamWikiDumpInputFormat``.

.. [#] You can run tests here.  To run tests, run ``ant compile-test``, add ``:../../../build/classes:../../../build/classes/:../../../build/contrib/streaming/classes:../../../build/contrib/streaming/test:../../../build/ivy/lib/Hadoop-Common/common/guava*.jar`` to the ``CLASSPATH`` environmental variable and run ``java org.junit.runner.JUnitCore org.wikimedia.wikihadoop.TestStreamWikiDumpInputFormat``

Input & Output format
=============================
Input can be Wikipedia XML dumps either as compressed in bzip2 (this is what you can directly get from the distribution site) or uncompressed.

Output is in the following format.

Requirements
==============================
Following softwares are required.

- `Apache Hadoop`_ 0.21 (possibly also 0.20 and higher)
- `Apache Ant`_

Sample command line usage
==============================

To process an English Wikipedia dump with the default mapper: ::

   hadoop jar hadoop-$\{version\}-streaming.jar -input /enwiki-20110722-pages-meta-history27.xml.bz2 -output /usr/hadoop/out -inputformat org.wikimedia.wikihadoop.StreamWikiDumpInputFormat

Mechanism
==============================

Splitting
----------------

Parsing
----------------
WikiHadoop's parser can be seen as a SAX parser that is tuned for Wikipedia dump XMLs.  By limiting its flexibility, it is supposed to achieve higher efficiency.  Instead of extracting all occurrence of elements and attributes, it only looks for beginnings and endings of ``page`` elements and ``revision`` elements.

Known problems
==============================
- The default size of minimum split tends to be too small.  Try changing it to a larger value by setting ``mapreduce.input.fileinputformat.split.minsize`` to, for example, 500000000.
- Timeout when pages are too long.  Try setting ``mapreduce.task.timeout`` longer than 6000000. Before it starts parsing the data and reporting the progress, WikiHadoop can take more than 600 minutes to preprocess XML dumps.
- Missing revisions.

.. Local variables:
.. mode: rst
.. End:
