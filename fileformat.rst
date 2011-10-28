==Location==
The diffdb can be downloaded from [http://dumps.wikimedia.org/other/diffdb/ dumps.wikimedia.org].

==Fields==
<pre>
hadoop21@beta:~/wikihadoop/diffs$ /usr/lib/hadoop-beta/bin/hdfs dfs -cat /usr/hadoop/out-10-bzip2/part-00000 | head -n 3
133350337	11406585	0	'National security and homeland security presidential directive'	1180070193	u'Begin'	False	308437	u'Badagnani'	0:1:u"The '''[[National Security and Homeland Security Presidential Directive]]''' (NSPD-51/HSPD-20), signed by President [[George W. Bush]] on May 9, 2007, is a [[Presidential Directive]] giving the [[President of the United States]] near-total control over the United States in the event of a catastrophic event, without the oversight of [[United States Congress|Congress]].\n\nThe signing of this Directive was generally unnoticed by the U.S. media as well as the U.S. Congress. It is unclear how the National Security and Homeland Security Presidential Directive will reconcile with the [[National Emergencies Act]], signed in 1976, which gives Congress oversight during such emergencies.\n\n==External links==\n*[http://www.whitehouse.gov/news/releases/2007/05/20070509-12.html National Security and Homeland Security Presidential Directive], from White House site\n\n==See also==\n*[[National Emergencies Act]]\n*[[George W. Bush]]\n\n{{US-stub}}"
133350707	11406585	0	'National security and homeland security presidential directive'	1180070344	None	False	308437	u'Badagnani'	906:1:u'National Security Directive]]\n*[['
133350794	11406585	0	'National security and homeland security presidential directive'	1180070386	None	False	308437	u'Badagnani'	613:-1:u'signed'	613:1:u'a U.S. federal law passed'
</pre>

Each row represents a revision from a XML dump of the English Wikipedia.  There *should* be a row for every revision that wasn't deleted when that dump was produced; however at this time, some cleanup will need to be done to remove duplicates and fill in missing revision diffs.
* <code>rev_id</code>: The identifier of the revision being described PRIMARY KEY
* <code>page_id</code>: The identifier of the page being revised
* <code>namespace</code>: The identifier of the namespace of the page
* <code>title</code>: The title of the page being revised
* <code>timestamp</code>: The time the revision took place as a Unix epoch timestamp in seconds
* <code>comment</code>: The edit summary left by the editor
* <code>minor</code>: Minor status of the edit (boolean)
* <code>user_id</code>: The identifier of the editor who saved the revision
* <code>user_text</code>: The username of the editor who saved the revision
* diffs - Tab separated, diff operations.  Each diff operation has three parts (separated by colons):
** <code>position</code>: The position in the article text at which the operation took place
** <code>action</code>: Did the operation add or remove some text?  ("1" for add, "-1" for remove)
** <code>content</code>: The text operated on.  For added text, this is the content to add.  For removed text, this is the content that was removed.

Each row can have 0-many diff operations.  Values in the result set have been encoded using python's <code>repr()</code> function and can be reproduced in python with the <code>eval()</code> function.

==Reproduction==
# Install [http://hadoop.apache.org Hadoop], [https://github.com/whym/wikihadoop WikiHadoop] and the [http://svn.wikimedia.org/svnroot/mediawiki/trunk/tools/wsor/diffs/ differ].

# Log in to the Hadoop master node.
# Download the Wikipedia dump files compressed in bz2 from [http://dumps.wikimedia.org/enwiki/ the dump distribution site].  Make sure to choose the dumps with full edit histories (pages-meta-historyN.xml.bz2).
#* For the 20110405 dumps (this is the source of the dataset being generated): [http://download.wikimedia.org/enwiki/20110526/enwiki-20110526-pages-meta-history1.xml.bz2] [http://download.wikimedia.org/enwiki/20110526/enwiki-20110526-pages-meta-history2.xml.bz2] [http://download.wikimedia.org/enwiki/20110526/enwiki-20110526-pages-meta-history3.xml.bz2][http://download.wikimedia.org/enwiki/20110526/enwiki-20110526-pages-meta-history4.xml.bz2][http://download.wikimedia.org/enwiki/20110526/enwiki-20110526-pages-meta-history5.xml.bz2][http://download.wikimedia.org/enwiki/20110526/enwiki-20110526-pages-meta-history6.xml.bz2][http://download.wikimedia.org/enwiki/20110526/enwiki-20110526-pages-meta-history7.xml.bz2][http://download.wikimedia.org/enwiki/20110526/enwiki-20110526-pages-meta-history8.xml.bz2][http://download.wikimedia.org/enwiki/20110526/enwiki-20110526-pages-meta-history9.xml.bz2][http://download.wikimedia.org/enwiki/20110526/enwiki-20110526-pages-meta-history10.xml.bz2][http://download.wikimedia.org/enwiki/20110526/enwiki-20110526-pages-meta-history11.xml.bz2][http://download.wikimedia.org/enwiki/20110526/enwiki-20110526-pages-meta-history12.xml.bz2][http://download.wikimedia.org/enwiki/20110526/enwiki-20110526-pages-meta-history13.xml.bz2][http://download.wikimedia.org/enwiki/20110526/enwiki-20110526-pages-meta-history14.xml.bz2][http://download.wikimedia.org/enwiki/20110526/enwiki-20110526-pages-meta-history15.xml.bz2]
# Copy the dump files in to HDFS using <code>/usr/lib/hadoop-beta/bin/hdfs dfs -copyFromLocal enwiki*.xml</code>
# Launch a Hadoop job for each dump file using the command below. 
#* <code><pre style="overflow:auto;">screen -S j01diffs /usr/lib/hadoop-beta/bin/hadoop jar hadoop-0.22-streaming.jar -Dmapreduce.task.timeout=0 -Dmapred.reduce.tasks=0 -Dmapreduce.input.fileinputformat.split.minsize=290000000 -D mapreduce.map.output.compress=true -input /enwiki-20110405-pages-meta-history1.xml.bz2 -output /usr/hadoop/out-01 -mapper ~/wikihadoop/diffs/revision_differ.py -inputformat org.wikimedia.wikihadoop.StreamWikiDumpInputFormat</pre></code>
#* With 3 nodes and 24 cores in total, one dump file of EN wiki approximately takes 20-24 hours to process.
# If you want to extract the dataset as an ordinary file, accumulate the dataset rows into one file (diffs.tsv.gz) using <code>/usr/lib/hadoop-beta/bin/hdfs dfs -cat /usr/hadoop/out-*/part-* > diffs.tsv</code>.
#* There are some duplicates in the results [https://github.com/whym/wikihadoop/issues/1]. If you want to exclude those duplicates, use <code>/usr/lib/hadoop-beta/bin/hdfs dfs -cat /usr/hadoop/out-*/part-* | sort -n -k2 -k1 -u -T ~/tmp/ > diffs.tsv</code> instead.  Note that <code>~/tmp</code> needs to be a directory large enough to contain all the results shown with <code>/usr/lib/hadoop-beta/bin/hdfs dfs -du /usr/hadoop/out-*/part-*</code>.
#* This may take several hours~one day depending on the size.  It will be more than 400 GB for EN wiki.

==Notes==
The dataset being generated is incomplete in two ways.
* Missing entries for less than 0.003% revisions (estimated). [https://github.com/whym/wikihadoop/issues/2]
* Duplicated entries for less than 0.02% revisions (estimated).  [https://github.com/whym/wikihadoop/issues/1]
