from StringIO import StringIO
from diff_match_patch import diff_match_patch
import re

revs = [
	{'rev_id': 1, 'content':'Foo derp 263254'},
	{'rev_id': 2, 'content':'Foo derp 26354'}
]

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
        		

def main():
	
	lastRev = {'content':''}
	content = ''
	for rev in revs:
		buff = StringIO()
		oldPos = 0
		lastPos = 0
		for pos, ar, c in simpleDiff(lastRev['content'], rev['content'], report=[-1,1]):
			equal = content[oldPos:oldPos+pos-lastPos]
			buff.write(equal)
			lastPos += len(equal)
			oldPos += len(equal)
			
			if ar == 1:
				buff.write(c)
				lastPos += len(c)
			elif ar == -1:
				oldPos += len(c)
				
			
			print("%s, %s, %r" % (pos, ar, c))
		
		buff.write(content[oldPos:])
		
			
		content = buff.getvalue()
		print("Rev: id=%s\n\t%r\n\t%r" % (rev['rev_id'], rev['content'], content))
		lastRev = rev
	
	content1 = open("content.2.txt", "r").read()
	hashes1, h2t, t2h = hashTokens(tokenize(content))
	print(len(hashes1))
	
	content = open("content.txt", "r").read()
	hashes2, h2t, t2h = hashTokens(tokenize(content), h2t, t2h)
	print(len(hashes2))
			
				
		
if __name__ == "__main__": main()

