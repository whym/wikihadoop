#!/usr/local/bin/pypy
################################################################################
# Revision Differ
#
# This script was written to be a streaming mapper for wikihadoop 
# (see https://github.com/whym/wikihadoop).  By default, this script runs under
# pypy (much faster), but it can also be run under CPython 2.7+.
#
# Required to run this script are
#  - diff_match_patch.py (provided)
#  - xml_simulator.py (provided)
#  - wikimedia-utilities (https://bitbucket.org/halfak/wikimedia-utilities)
#
# Author: Aaron Halfaker (aaron.halfaker@gmail.com)
# 
# This software licensed as GPLv2(http://www.gnu.org/licenses/gpl-2.0.html). and 
# is provided WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or 
# implied.
#
################################################################################
import logging, traceback, sys, re
from StringIO import StringIO

from diff_match_patch import diff_match_patch

from xml_simulator import RecordingFileWrapper
from wmf.dump.iterator import Iterator
import wmf

def tokenize(content):
	return re.findall(
		r"[\w]+" +   #Word
		r"|\[\[" +   #Opening internal link
		r"|\]\]" +   #Closing internal link
		r"|\{\{" +   #Opening template
		r"|\}\}" +   #Closing template
		r"|\{\{\{" + #Opening template var
		r"|\}\}\}" + #Closing template var
		r"|\n+" +    #Line breaks
		r"| +" +     #Spaces
		r"|&\w+;" +  #HTML escape sequence
		r"|'''" +    #Bold
		r"|''" +     #Italics
		r"|=+" +     #Header
		r"|\{\|" +   #Opening table
		r"|\|\}" +   #Closing table
		r"|\|\-" +   #Table row
		r"|.",       #Misc character
		content
	)

def hashTokens(tokens, hash2Token=[], token2Hash={}):
	hashBuffer = StringIO()
	for t in tokens:
		if t in token2Hash:
			hashBuffer.write(unichr(token2Hash[t]+1))
		else:
			hashId = len(hash2Token)
			hash2Token.append(t)
			token2Hash[t] = hashId
			hashBuffer.write(unichr(hashId+1))
		
	return (hashBuffer.getvalue(), hash2Token, token2Hash)

def unhash(hashes, hash2Token, sep=''):
	return sep.join(hash2Token[ord(h)-1] for h in hashes)

def simpleDiff(content1, content2, tokenize=tokenize, sep='', report=[-1,0,1]):
	hashes1, h2t, t2h = hashTokens(tokenize(content1))
	hashes2, h2t, t2h = hashTokens(tokenize(content2), h2t, t2h)
	
	report = set(report)
	
	dmp = diff_match_patch()
	
	diffs = dmp.diff_main(hashes1, hashes2, checklines=False)
	
	position = 0
	for (ar,hashes) in diffs:
		content = unhash(hashes,h2t,sep=sep)
		if ar in report:
			yield position, ar, content
		
		if ar != -1: position += len(content)


metaXML = """
<mediawiki xmlns="http://www.mediawiki.org/xml/export-0.5/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.mediawiki.org/xml/export-0.5/ http://www.mediawiki.org/xml/export-0.5.xsd" version="0.5" xml:lang="en">
<siteinfo>
<sitename>Wikipedia</sitename>
<base>http://en.wikipedia.org/wiki/Main_Page</base>
<generator>MediaWiki 1.17wmf1</generator>
<case>first-letter</case>
<namespaces>
<namespace key="-2" case="first-letter">Media</namespace>
<namespace key="-1" case="first-letter">Special</namespace>
<namespace key="0" case="first-letter" />
<namespace key="1" case="first-letter">Talk</namespace>
<namespace key="2" case="first-letter">User</namespace>
<namespace key="3" case="first-letter">User talk</namespace>
<namespace key="4" case="first-letter">Wikipedia</namespace>
<namespace key="5" case="first-letter">Wikipedia talk</namespace>
<namespace key="6" case="first-letter">File</namespace>
<namespace key="7" case="first-letter">File talk</namespace>
<namespace key="8" case="first-letter">MediaWiki</namespace>
<namespace key="9" case="first-letter">MediaWiki talk</namespace>
<namespace key="10" case="first-letter">Template</namespace>
<namespace key="11" case="first-letter">Template talk</namespace>
<namespace key="12" case="first-letter">Help</namespace>
<namespace key="13" case="first-letter">Help talk</namespace>
<namespace key="14" case="first-letter">Category</namespace>
<namespace key="15" case="first-letter">Category talk</namespace>
<namespace key="100" case="first-letter">Portal</namespace>
<namespace key="101" case="first-letter">Portal talk</namespace>
<namespace key="108" case="first-letter">Book</namespace>
<namespace key="109" case="first-letter">Book talk</namespace>
</namespaces>
</siteinfo>
"""


xmlSim = RecordingFileWrapper(sys.stdin, pre=metaXML, post='</mediawiki>')

try:
	dump = Iterator(xmlSim)
except Exception as e:
	sys.stderr.write(str(e) + xmlSim.getHistory())
	sys.exit(1)


for page in dump.readPages():
	sys.stderr.write('Processing: %s - %s\n' % (page.getId(), page.getTitle().encode('UTF-8')))
	try:
		lastRev = None
		currRevId = None
		for revision in page.readRevisions():
			currRevId = revision.getId()
			if lastRev == None:
				lastRev = revision
			else:
				namespace, title = wmf.normalizeTitle(page.getTitle(), namespaces=dump.namespaces)
				nsId = dump.namespaces[namespace]
				if revision.getContributor() != None:
					userId = revision.getContributor().getId()
					userName = revision.getContributor().getUsername()
				else:
					userId = None
					userName = None

				row = [
					repr(revision.getId()),
					repr(page.getId()),
					repr(nsId),
					repr(title),
					repr(revision.getTimestamp()),
					repr(revision.getComment()),
					repr(revision.getMinor()),
					repr(userId),
					repr(userName)
				]
				try:
					for d in simpleDiff(lastRev.getText(), revision.getText(), report=[-1,1]):
						row.append(":".join(repr(v) for v in d))
					
					print("\t".join(row))
					sys.stderr.write('reporter:counter:SkippingTaskCounters,MapProcessedRecords,1\n')
				except Exception as e:
					row.extend(["diff_fail", str(e).encode('string-escape')])
					print("\t".join(row))
					raise e
				
				
	except Exception as e:
		sys.stderr.write('%s - while processing revId=%s\n' % (e, currRevId))
		traceback.print_exc(file=sys.stderr)
