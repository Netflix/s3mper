#!/usr/bin/python2.7
import sys
import os.path
import time
import platform
import getpass
import traceback
import logging

from datetime import datetime

from urlparse import urlparse
from urlparse import urlunparse

logger = logging.getLogger('com.netflix.bdp.s3mper')

from boto.dynamodb2.table import Table
from boto.s3.key import Key
from boto import sqs
from boto.sqs.message import RawMessage

class S3mper:
    """ S3mper is a metastore library used to provide a layer of consistency on 
        top of S3 by using dynamodb to record what files should be in the S3
        listing.
        
        See go/s3mper for more information.
    """
    
    def __init__(self, disabled=False, fail_on_error=False, table_name='ConsistentListingMetastoreTest'):
        self.disabled = disabled
        self.disable_alerts = False
        self.fail_on_error = fail_on_error;
        
        if self.disabled:
            logger.warning('S3mper Explicitly Disabled')
            
            return
        
        self.db = Table(table_name)
        
    def add(self, paths):
        """ Adds a list of Paths to the file metastore and returns True on success. 
        
            Example:
            s.add([path1, path2]) -> True
        """
        if self.disabled:
            return
        
        epoch = self.__time_now()
        
        paths = self.__as_paths(paths)
        
        with self.db.batch_write() as batch:
            for path in paths:
                batch.put_item(data={ 'path':path.parent().normalize(), 'file':path.filename(), 'epoch':epoch })
    
    def list(self, path, include_delete_marked=False):
        """ Lists the given directory in the metastore.  The passed in path must be a directory.
        
            Example: 
            s.list(path) -> []
        """
        if self.disabled:
            return
        
        if isinstance(path, basestring):
            path = Path(path) 
        
        listing = self.db.query(path__eq=path.normalize(), consistent=True)
        
        paths = []
        
        for e in listing:
            if  ( not include_delete_marked ) and 'deleted' in e:
                continue
            
            paths.append(Path('s3n:'+e['path']+"/"+e['file']))
            
        return paths
    
    def checked_listing(self, s3_listing, path):
        """ Checks the s3_listing against the metastore listing.  All attempts
            are made to use the boto generator for listing if a check isn't
            necessary, but if a check must be made the whole listing for both
            the metastore and s3 listing need to be pulled into memory.
        """
        if self.disabled:
            return s3_listing
        
        expected = set( [p.url for p in self.list(path)] ) 
        
        if not expected:
            return s3_listing
        
        #This isn't ideal since we are sucking in the whole listing
        #to perform the check, but if we check on-the-fly, processing
        #could be partially complete before inconsistency is detected  
        s3_listing = list(s3_listing())
        
        for p in s3_listing:
            expected.discard(p if not isinstance(p, Key) else 's3://%s/%s' % (p.bucket, p.name))
        
        if not expected:
            return s3_listing
        else:
            logger.error("Failed consistency check.  Missing file count %d. Missing paths: %s" % (len(expected), expected))
            self.__send_alert(expected)
            
            if self.fail_on_error:
                raise S3ConsistencyException(expected)
        
    def delete(self, paths, delete_marker=False):
        """ Deletes the provided paths from the metastore.  
        
            Completly removing files from the metastore can cause problems 
            because the s3 listing may show the files even though the data may 
            not be available.  This will cause MR jobs to fail.  The delete marker
            can be used to hide files from the listing.
        
            Example:
            s.delete([path1, path2]) -> True
        """
        if(self.disabled):
            return
        
        paths = self.__as_paths(paths)
        
        if delete_marker:
            for path in paths:
                item = self.db.get_item(path=path.parent().normalize(), file=path.filename())
                item['deleted'] = "true"
        else:
            with self.db.batch_write() as batch:
                for path in paths:
                    batch.delete_item(path=path.parent().normalize(), file=path.filename())
        
    def __send_alert(self, paths, detail={}):
        if self.disable_alerts:
            return
        
        try:
            body = { 
               "truncated":    detail.get('truncated', False),
               "paths":        paths if len(paths) <= 10 else paths[0:9],
               "recovered":    detail.get('recovered', False),
               "missingFiles": len(paths),
               "stackTrace":   traceback.extract_stack(),
               "timestamp":    "%s"%datetime.utcnow(),
               "queryId":      detail.get('',None),
               "taskId":       detail.get('',None),
               "hostname":     platform.node(),
               "username":     getpass.getuser(),
               "queryType":    "DSE Platform Lib",
               "jobId":        detail.get('jobId',None),
               "attemptId":    detail.get('attemptId',None),
               "email":        detail.get('email',None),
               "dataovenId":   detail.get('dataovenId',None),
               "logFile":      detail.get('logFile',None),
               "inputFile":    detail.get('inputFile',None),
               "genieId":      detail.get('genieId',None),
               "epoch":        self.__time_now()
            }

            message = RawMessage()
            message.set_body(body)

            conn = sqs.connect_to_region("us-east-1")
            queue = conn.get_queue('s3mper-alert-queue')    
                 
            queue.write(message)     
            
        except Exception as e:
            print e    
        
    def __as_paths(self, paths):
        if isinstance(paths, basestring):
            return [ Path(paths) ]
        elif isinstance(paths, Path):
            return [paths]
        else:
            return paths
        
    def __time_now(self):
        """ Returns current time in milliseconds. """
        return int(time.time())

class Path:
    """ The Path class represents something similar to the hadoop Path class
        and allows for normalization of file paths for s3mper.
    """
    
    def __init__(self, url):
        """ The url of the object needs to be normalized to remove any confusion
            metastore based on how the path was provided.
            
            Example s3://this///is/not/./..//noramlized -> s3://this/is/normalized
        """
        p = urlparse(url)
        normalized_path = os.path.normpath(p.path)
        
        self.url = urlunparse((p.scheme, p.netloc, normalized_path, '', '', ''))   
        
    def filename(self):
        """ Returns just the base file name of the path.
        
            Path('s3://netflix/foo.txt').filename() -> 'foo.txt' 
        
        """
        return os.path.basename(urlparse(self.url).path)    
        
    def parent(self):
        """ Returns the parent directory as a Path object 
        
            Path('s3://netflix/foo/bar').parent() -> Path('s3://netflix/foo')
        """
        parse = urlparse(self.url)
        
        parent = os.path.dirname(parse.path)
        
        return Path(urlunparse((parse.scheme, parse.netloc, parent, '', '', '')))
    
    def normalize(self):
        """ Normalizes the path and removes the scheme protion so that s3 and s3n
            schemes don't conflict with each other.
            
            returns a String
        """
        p = urlparse(self.url)
        result = '//%s%s' % (p.netloc, p.path)
        
        if result.endswith('/'):
            result = result[:-1]
        
        return result
        
class S3ConsistencyException(Exception):
    def __init__(self, paths):
        self.paths = paths
        
    def __str__(self):
        return '%s' % self.paths
        
        

def main():
    if sys.argv[0] == 'list':
        path = Path(sys.argv[1])
        
        s = S3mper();
        listing = s.list(path)
        
        for p in listing:
            print p.url

if __name__ == "__main__":
    main()
