
=====================
WikiHadoop
=====================
--------------------------------------------------------------------------------------------
Stream-based InputFormat for processing the compressed XML dumps of Wikipedia with Hadoop
--------------------------------------------------------------------------------------------

 :Homepage: http://github.com/whym/wikihadoop
 :Date: 2012-04-16
 :Version: 0.2

Overview
==============================

Wikipedia XML dumps with complete edit histories [#]_ have been
difficult to process because of their exceptional size and structure.
While a "page" is a common processing unit, one Wikipedia page may
contain more than gigabytes of text when the edit history is very
long.

This software provides an ``InputFormat`` for `Hadoop Streaming`_
Interface that processes Wikipedia bzip2 XML dumps in a streaming
manner.  Using this ``InputFormat``, the content of every page is fed
to a mapper via standard input and output without using too much
memory.  Thanks to Hadoop Streaming, mappers can be implemented in any
language.

See the `wiki page`__ for a more detailed introduction and tutorial.

__ https://github.com/whym/wikihadoop/wiki
.. _Hadoop Streaming: http://hadoop.apache.org/common/docs/current/streaming.html
.. _Apache Hadoop: http://hadoop.apache.org
.. _Apache Maven: http://maven.apache.org
.. _WikiHadoop: http://github.com/whym/wikihadoop

.. [#] For example, one dump file such as pages-meta-history1.xml.bz2,
       pages-meta-history6.xml.bz2, etc, provided at
       http://dumps.wikimedia.org/enwiki/20110803/ is more than 30
       gigabytes in compressed forms, and more than 700 gigabytes
       when decompressed.

How to use
==============================
Essentially WikiHadoop is an input format for ``Hadoop Streaming``.  Once you have ``StreamWikiDumpInputFormat`` in the class path, you can give it into the ``-inputformat`` option.

To get the input format class working with Hadoop Streaming, proceed with the following procedures:

1. Install `Apache Hadoop`_.  Version 0.21 and 0.22 is the one we tested.

   - By default it builds with 0.21.  For Hadoop 0.22, change the version number in ``pom.xml`` at the lines containing ``<version>0.21.0-SNAPSHOT</version>``.
   - See also Requirements_.

2. Download our jar file.  Alternatively, you can build the class and/or the jar by yourself (see `How to build`_).

   - From our `download page`_ you can download the latest jar file or
     the tarball containing the default mapper for creating diffs.

3. Find the jar file of Hadoop Streaming ``hadoop-streaming.jar`` in your copy of Hadoop.  It is probably found at ``mapred/contrib/streaming/hadoop-0.21.0-streaming.jar``.

4. Run a `Hadoop Streaming`_ command with the jar file and our input format specified.

   -  A command will look like this: ::
      
       hadoop jar hadoop-streaming.jar -libjars wikihadoop.jar -inputformat org.wikimedia.wikihadoop.StreamWikiDumpInputFormat
     
      See `Sample command line usage`_, `Configuration variables`_ and the official documentation of `Hadoop Streaming`_ for more details.

   We recommend to use our differ_ as the mapper when creating text
   diffs between consecutive revisions.  The differ
   ``revision_differ.py`` is included in the tarball under ``diffs``, or
   can be downloaded from the MediaWiki SVN repository by ``svn
   checkout
   http://svn.wikimedia.org/svnroot/mediawiki/trunk/tools/wsor/diffs``.
   See its `Differ's readme file`_ for more details and other requirements.

.. _Differ's readme file: http://svn.wikimedia.org/svnroot/mediawiki/trunk/tools/wsor/diffs/README.txt
.. _StreamWikiDumpInputFormat: https://github.com/whym/wikihadoop/blob/master/mapreduce/src/contrib/streaming/src/java/org/wikimedia/wikihadoop/StreamWikiDumpInputFormat.java
.. _download page: https://github.com/whym/wikihadoop/downloads

How to build
==============================

2. Download WikiHadoop_ and extract the source tree.
   
   We provide both our git repository and a tarball package.
   
   - Use ``git clone https://whym@github.com/whym/wikihadoop.git`` to
     access to the latest source,

3. Add the repository URL ``https://repository.apache.org/content/groups/public/`` and ``>https://repository.cloudera.com/artifactory/libs-release-local`` to ~/.m2/settings.xml [#]_. Run Maven to build a jar file. ::
    
      mvn package

4. Find the resulting jar file at ``target/wikihadoop-0.2.jar``.

.. [#] You will need to have setting.xml like this:
       ::
       
        <settings>
          <profiles>
              <profile>
                <id>my-profile</id>
                <activation>
                  <activeByDefault>true</activeByDefault>
                </activation>
                <repositories>
                  <!-- after other <repository> elements  -->
                  <repository>
                    <id>apache-public</id>
                    <url>https://repository.apache.org/content/groups/public/</url>
                    <snapshots>
                      <enabled>true</enabled>
                    </snapshots>
                    <releases>
                      <enabled>true</enabled>
                    </releases>
                  </repository>
                  <repository>
                    <id>cloudera-libs-release</id>
                    <url>https://repository.cloudera.com/artifactory/libs-release-local</url>
                    <snapshots>
                      <enabled>true</enabled>
                    </snapshots>
                    <releases>
                      <enabled>true</enabled>
                    </releases>
                  </repository>
                </repositories>
              </profile>
            </profiles>
        </settings>


Input & Output format
=============================

Input can be Wikipedia XML dumps either as compressed in bzip2 (this
is what you can directly get from the distribution site) or
uncompressed.

The record reader embedded in this input format converts a page into a
sequence of page-like elements, each of which contains two consecutive
revisions. Output is given as key-value style records where a key is a
page-like element and a value is always empty.  For example, Given the
following input containing two pages and four revisions, ::

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

Notice that before This result will provide a mapper with all information about the revision including the title and page ID.  We recommend to use our differ_ to get diffs.

.. _differ: http://svn.wikimedia.org/svnroot/mediawiki/trunk/tools/wsor/diffs/

Requirements
==============================
Following softwares are required.

- `Apache Hadoop`_
  
  - Versions 0.21 and 0.22 are supported.
  - `Cloudera's`_ cdh3u1 is also supported at the `cdh3u1 branch`_, thanks to François Kawla).
  
- `Apache Maven`_

See also `Supported Versions of Hadoop`_ for more information.


.. _Cloudera's: https://ccp.cloudera.com/display/SUPPORT/Downloads
.. _cdh3u1 branch: https://github.com/whym/wikihadoop/tree/cdh3u1
.. _Supported Versions of Hadoop: https://github.com/whym/wikihadoop/wiki/Supported-Versions-of-Hadoop.

Sample command line usage
==============================

- To process an English Wikipedia dump with Hadoop's default mapper: ::
  
    hadoop jar hadoop-streaming.jar -libjars wikihadoop.jar -D mapreduce.input.fileinputformat.split.minsize=300000000 -D mapreduce.task.timeout=6000000 -input /enwiki-20110722-pages-meta-history27.xml.bz2 -output /usr/hadoop/out -inputformat org.wikimedia.wikihadoop.StreamWikiDumpInputFormat

Configuration variables
==============================
Following parameters can be configured as similarly as other parameters described in `Hadoop Streaming`_.

``org.wikimedia.wikihadoop.excludePagesWith=REGEX``
        Used to exclude pages with the headers that match to this.
        For example, to exclude all namespaces except for the main article space, use ``-D org.wikimedia.wikihadoop.excludePagesWith="<title>(Media|Special|Talk|User|User talk|Wikipedia|Wikipedia talk|File|File talk|MediaWiki|MediaWiki talk|Template|Template talk|Help|Help talk|Category|Category talk|Portal|Portal talk|Book|Book talk):"``.
        When unspecified, WikiHadoop sends all pages to mappers.
        
        Ignoring pages irrelevant to the task is a good idea, if you want to speed up the process.

``org.wikimedia.wikihadoop.previousRevision=true or false``
        When set ``false``, WikiHadoop writes only one revision in one page-like element without attaching the previous revision.
        The default behaviour (``true``) is to write two consecutive revisions in one page-like element, 

``mapreduce.input.fileinputformat.split.minsize=BYTES``
        This variables specified the minimum size of a split sent to
        input readers.
        
        The default size tends to be too small.  Try changing it to a
        larger value by setting.  The optimal value seems to be around
        (size of the input dump file) / (number of processors) / 5.
        For example, it will be 500000000 for English Wikipedia dumps
        when processing with 12 processors.

``mapreduce.task.timeout=MSECS``
        Timeout may happen when pages are too long.  Try setting
        longer than 6000000. Before it starts
        parsing the data and reporting the progress, WikiHadoop can take
        more than 6000 seconds to preprocess XML dumps.

Mechanism
==============================

Splitting
----------------
Input dump files are split into smaller splits with the sizes close to
the value of ``mapreduce.input.fileinputformat.split.minsize``.  When
non-compressed input is used, each split exactly ends with a page end.
When bzip2 (or other splittable compression) input is used, each split
is modified so that every page is contained at least one of the
splits.

Parsing
----------------

WikiHadoop's parser can be seen as a SAX parser that is tuned for
Wikipedia dump XMLs.  By limiting its flexibility, it is supposed to
achieve higher efficiency.  Instead of extracting all occurrence of
elements and attributes, it only looks for beginnings and endings of
``page`` elements and ``revision`` elements.

Known problems
==============================
- Hadoop map tasks with ``StreamWikiDumpInputFormat`` may take a long
  time to finish preprocessing before starting reporting the progress.
- Some revision pairs may be emitted twice when bzip2 input is
  used. (`Issue #1`_)

.. _Issue #1: https://github.com/whym/wikihadoop/issues/1

.. Local variables:
.. mode: rst
.. End:
