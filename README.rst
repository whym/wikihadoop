
=====================
WikiHadoop
=====================
--------------------------------------------------------------------------------------------
Stream-based InputFormat for processing the compressed XML dumps of Wikipedia with Hadoop
--------------------------------------------------------------------------------------------

 :Homepage: http://github.com/whym/wikihadoop
 :Date: 2011-08-15
 :Version: 0.1

Overview
==============================

Wikipedia XML dumps with complete edit histories [#]_ have been difficult to process because of their exceptional size and structure.  While a "page" is a common processing unit, one Wikipedia page may contain more than gigabytes of text when the edit history is very long.

This software provides an ``InputFormat`` for `Hadoop Streaming`_ Interface that processes Wikipedia bzip2 XML dumps in a streaming manner.  Using this ``InputFormat``, the content of every page is fed to a mapper via standard input and output without using too much memory.  Thanks to Hadoop Streaming, mappers can be implemented in any language.

See the `wiki page`__ for a more detailed introduction and tutorial.

__ https://github.com/whym/wikihadoop/wiki
.. _Hadoop Common: http://github.com/apache/hadoop-common
.. _Hadoop Streaming: http://hadoop.apache.org/common/docs/current/streaming.html
.. _Apache Hadoop: http://hadoop.apache.org
.. _Apache Ant: http://ant.apache.org
.. _WikiHadoop: http://github.com/whym/wikihadoop

.. [#] For example, one dump file such as pages-meta-history1.xml.bz2, pages-meta-history6.xml.bz2, etc, provided at http://dumps.wikimedia.org/enwiki/20110803/ is more than 30 gigabytes in compressed forms, and more than 700 gigabytes when decompressed.

How to use
==============================

1. Download WikiHadoop_ and extract the source tree.  Confirm there is a directory called ``mapreduce``.

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

The record reader embedded in this input format converts a page into a sequence of page-like elements, each of which contains two consecutive revisions. Output is given as key-value style records where  a key is a page-like element and a value is always empty.  For example, Given the following input containing two pages and four revisions, ::

  <page>
    <title>ABC</title>
    <id>123</id>
    <revision>
      <id>100</id>
      ....
    </revision>
    <revision>
      <id>200</id>
      ....
    </revision>
    <revision>
      <id>300</id>
      ....
    </revision>
  </page>
  <page>
    <title>DEF</title>
    <id>456</id>
    <revision>
      <id>400</id>
      ....
    </revision>
  </page>
 
it will produce four keys formatted in page-like elements as follows ::

  <page>
    <title>ABC</title>
    <id>123</id>
    <revision><revision beginningofpage="true"><text xml:space="preserve"></text></revision><revision>
      <id>100</id>
      ....
    </revision>
  </page>
 
::

  <page>
    <title>ABC</title>
    <id>123</id>
    <revision>
      <id>100</id>
      ....
    </revision>
    <revision>
      <id>200</id>
      ....
    </revision>
  </page>
 
::

  <page>
    <title>ABC</title>
    <id>123</id>
    <revision>
      <id>200</id>
      ....
    </revision>
    <revision>
      <id>300</id>
      ....
    </revision>
  </page>
 
::

  <page>
    <title>DEF</title>
    <id>456</id>
    <revision><revision beginningofpage="true"><text xml:space="preserve"></text></revision><revision>
      <id>400</id>
      ....
    </revision>
  </page>

This result will provide a mapper with all information about the revision including the title and page ID.  We recommend to use our differ_ to get diffs.

.. _differ: http://svn.wikimedia.org/svnroot/mediawiki/trunk/tools/wsor/diffs/

Requirements
==============================
Following softwares are required.

- `Apache Hadoop`_ 0.21 (it possibly works also with 0.22 or higher)
- `Apache Ant`_

Sample command line usage
==============================

To process an English Wikipedia dump with Hadoop's default mapper: ::

   hadoop jar hadoop-$\{version\}-streaming.jar -input /enwiki-20110722-pages-meta-history27.xml.bz2 -output /usr/hadoop/out -inputformat org.wikimedia.wikihadoop.StreamWikiDumpInputFormat

Configuration variables
==============================
Following parameters can be configured as similarly as other parameters described in `Hadoop Streaming`_.

``org.wikimedia.wikihadoop.excludePagesWith=REGEX``
        Used to exclude pages with the headers that match to this.
        For example, to exclude all namespaces except for the main article space, use ``-D org.wikimedia.wikihadoop.excludePagesWith="<title>(Media|Special|Talk|User|User talk|Wikipedia|Wikipedia talk|File|File talk|MediaWiki|MediaWiki talk|Template|Template talk|Help|Help talk|Category|Category talk|Portal|Portal talk|Book|Book talk):"``.
        When unspecified, WikiHadoop sends all pages to mappers.
        
        Ignoring pages irrelevant to the task is a good idea, if you want to speed up the process.

``org.wikimedia.wikihadoop.previousRevision=true or false``
        When set ``false``, WikiHadoop writes one revision in one page-like element without attaching the previous revision.
        The default behaviour is to write two consecutive revisions in one page-like element, 

Mechanism
==============================

Splitting
----------------
Input dump files are split into smaller splits with the sizes close to the value of ``mapreduce.input.fileinputformat.split.minsize``.  When non-compressed input is used, each split exactly ends with a page end.  When bzip2 (or other splittable compression) input is used, each split is modified so that every page is contained at least one of the splits.

Parsing
----------------
WikiHadoop's parser can be seen as a SAX parser that is tuned for Wikipedia dump XMLs.  However, by limiting its flexibility, it is supposed to achieve higher efficiency.  Instead of extracting all occurrence of elements and attributes, it only looks for beginnings and endings of ``page`` elements and ``revision`` elements.

Known problems
==============================
- Hadoop map tasks with ``StreamWikiDumpInputFormat`` may take a long time to finish preprocessing before starting reporting the progress.
- Some revision pairs may be emitted twice when bzip2 input is used. (Issue #1)
- The default size of minimum split tends to be too small.Try changing it to a larger value by setting ``mapreduce.input.fileinputformat.split.minsize``.  The optimal value seems to be around (size of the input dump file) / (number of processors) / 5.  For example, it will be 500000000 for English Wikipedia dumps when processing with 12 processors.
- Timeout may happen when pages are too long.  Try setting ``mapreduce.task.timeout`` longer than 6000000. Before it starts parsing the data and reporting the progress, WikiHadoop can take more than 6000 seconds to preprocess XML dumps.
- Although very small in number, some revisions can be missing in the results.  We have seen 184 out of 17,971,932 revisions are missing. (Issue #2)

.. Local variables:
.. mode: rst
.. End:
