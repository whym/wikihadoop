import sys
from StringIO import StringIO
from collections import deque

class FileWrapper:
	
	def __init__(self, fp, pre='', post=''):
		self.fp     = fp
		self.pre    = StringIO(pre)
		self.post   = StringIO(post)
		self.closed = False
		self.mode   = "r"
		
	def read(self, bytes=sys.maxint):
		bytes = int(bytes)
		if self.closed: raise ValueError("I/O operation on closed file")
		
		preBytes = self.pre.read(bytes)
		if len(preBytes) < bytes: 
			fpBytes = self.fp.read(bytes-len(preBytes))
		else:
			fpBytes = ''
		
		if len(preBytes) + len(fpBytes) < bytes:
			postBytes = self.post.read(bytes-(len(preBytes) + len(fpBytes)))
		else:
			postBytes = ''
		
		return preBytes + fpBytes + postBytes
	
	def readline(self):
		if self.closed: raise ValueError("I/O operation on closed file")
		
		output = self.pre.readline()
		if len(output) == 0 or output[-1] != "\n": 
			output += self.fp.readline()
		if len(output) == 0 or output[-1] != "\n": 
			output += self.post.readline()
		
		return output
	
	def readlines(self): raise NotImplementedError()
	
	def __iter__(self):
		
		line = self.readline()
		while line != '':
			yield line
			line = self.readline()
		
		
	def seek(self): raise NotImplementedError()
	def write(self): raise NotImplementedError()
	def writelines(self): raise NotImplementedError()
	def tell(self):
		return self.pre.tell() + self.fp.tell() + self.post.tell()
		
		
	def close(self):
		self.closed = True
		self.fp.close()

class RecordingFileWrapper(FileWrapper):
	
	def __init__(self, fp, pre='', post='', record=10000):
		self.history = deque(maxlen=record)
		FileWrapper.__init__(self, fp, pre=pre, post=post)
	
	def read(self, bytes=sys.maxint):
		outBytes = FileWrapper.read(self, bytes)
		self.history.extend(outBytes)
		return outBytes
	
	def readline(self):
		outBytes = FileWrapper.readline(self)
		self.history.extend(outBytes)
		return outBytes
	
	def getHistory(self):
		return ''.join(self.history)
