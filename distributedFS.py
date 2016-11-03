#!/usr/bin/env python
from __future__ import print_function, absolute_import, division

import logging

from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

if not hasattr(__builtins__, 'bytes'):
    bytes = str

class Memory(LoggingMixIn, Operations):
    'Example memory filesystem. Supports multiple level of files.'

    def __init__(self,MetaServerPort,DataServerPort):
        self.files = {}
        self.data = defaultdict(bytes)
        self.fd = 0
        self.BLKSIZE = 512
        self.MetaServerPort = MetaServerPort
        self.DataServerPort = DataServerPort
        print(self.MetaServerPort)
        print(self.DataServerPort)
        now = time()
        self.files['/'] = dict(st_mode=(S_IFDIR | 0o755), st_ctime=now,
                               st_mtime=now, st_atime=now, st_nlink=2)
	self.data['/'] = []

    def chmod(self, path, mode):
        self.files[path]['st_mode'] &= 0o770000
        self.files[path]['st_mode'] |= mode
        return 0

    def chown(self, path, uid, gid):
        self.files[path]['st_uid'] = uid
        self.files[path]['st_gid'] = gid

    def create(self, path, mode):
        self.files[path] = dict(st_mode=(S_IFREG | mode), st_nlink=1,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time())
        self.data[path] = []
        pathSplit = path.split('/')
        if len(pathSplit) == 2:
            self.data['/'].append(pathSplit[1])
        else:
            localPath = []
            num = 1
            while num < (len(pathSplit) - 1):
                localPath.append('/')
                localPath.append(pathSplit[num])
                num += 1
            localPath = ''.join(localPath)
            self.data[localPath].append(pathSplit[len(pathSplit) - 1])
        self.fd += 1    #ToDo - Handle fd
        return self.fd

    def getattr(self, path, fh=None):
        if path not in self.files:
            raise FuseOSError(ENOENT)

        return self.files[path]

    def getxattr(self, path, name, position=0):
        attrs = self.files[path].get('attrs', {})

        try:
            return attrs[name]
        except KeyError:
            return ''       # Should return ENOATTR

    def listxattr(self, path):
        attrs = self.files[path].get('attrs', {})
        return attrs.keys()

    def mkdir(self, path, mode):
        self.files[path] = dict(st_mode=(S_IFDIR | mode), st_nlink=2,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time())

        self.data[path] = []
        pathSplit = path.split('/')
        if len(pathSplit) == 2:
            self.data['/'].append(pathSplit[1])
            self.files['/']['st_nlink'] += 1
        else:
            localPath = []
            num = 1
            while num < (len(pathSplit) - 1):
                localPath.append('/')
                localPath.append(pathSplit[num])
                num += 1
            localPath = ''.join(localPath)
            self.data[localPath].append(pathSplit[len(pathSplit) - 1])
            self.files[localPath]['st_nlink'] += 1


    def open(self, path, flags):
        self.fd += 1
        return self.fd

    def read(self, path, size, offset, fh):
        Data = ''.join(self.data[path])
        return Data[offset:offset + size]

    def readdir(self, path, fh):
        dirlist = ['.', '..']
        return  dirlist + self.data[path]

    def readlink(self, path):
        Data = ''.join(self.data[path])
        return Data

    def removexattr(self, path, name):
        attrs = self.files[path].get('attrs', {})

        try:
            del attrs[name]
        except KeyError:
            pass        # Should return ENOATTR

    def rename(self, old, new):
        self.files[new] = self.files.pop(old)
        self.data[new] = self.data.pop(old)
        oldpathSplit = old.split('/')
        newpathSplit = new.split('/')
        # Check if renaming is done in same parent direcctory
        if len(oldpathSplit) == len(newpathSplit):
            if len(oldpathSplit) == 2:
                self.data['/'].remove(oldpathSplit[1])
                self.data['/'].append(newpathSplit[1])
            else:
                localPath = []
                num = 1
                while num < (len(oldpathSplit) - 1):
                    localPath.append('/')
                    localPath.append(oldpathSplit[num])
                    num += 1
                localPath = ''.join(localPath)
                self.data[localPath].remove(oldpathSplit[len(oldpathSplit) - 1])
                self.data[localPath].append(newpathSplit[len(newpathSplit) - 1])
            # Replace if it is directory
            if self.files[new]['st_nlink'] != 1:
                for x in self.files:
                    oldx = x
                    localX = x.split('/')
                    if len(localX) >= len(oldpathSplit):
                        if oldpathSplit[len(oldpathSplit) - 1] == localX[len(oldpathSplit) - 1]:
                            localX[len(oldpathSplit) - 1] = newpathSplit[len(newpathSplit) - 1]
                            localPath = []
                            num = 1
                            while num < len(localX):
                                localPath.append('/')
                                localPath.append(localX[num])
                                num += 1
                            newx = ''.join(localPath)
                            self.files[newx] = self.files.pop(oldx)
                            self.data[newx] = self.data.pop(oldx)
        else:
            if len(oldpathSplit) == 2:
                self.data['/'].remove(oldpathSplit[1])
            else:
                oldParentPath = []
                num = 1
                while num < (len(oldpathSplit) - 1):
                    oldParentPath.append('/')
                    oldParentPath.append(oldpathSplit[num])
                    num += 1
                oldParentPath = ''.join(oldParentPath)
                self.data[oldParentPath].remove(oldpathSplit[len(oldpathSplit) - 1])
            if len(newpathSplit) == 2:
                self.data['/'].append(newpathSplit[1])
            else:
                newParentPath = []
                num = 1
                while num < (len(newpathSplit) - 1):
                    newParentPath.append('/')
                    newParentPath.append(newpathSplit[num])
                    num += 1
                newParentPath = ''.join(newParentPath)
                self.data[newParentPath].append(newpathSplit[len(newpathSplit) - 1])
            # If it is a directory
            if self.files[new]['st_nlink'] != 1:
                for x in self.files:
                    if new not in x:
                        oldx = x;
                        newx = x.replace(old,new,1)
                        self.files[newx] = self.files.pop(oldx)
                        self.data[newx] = self.data.pop(oldx)


    def rmdir(self, path):
        # Todo - Free RAM
        self.files.pop(path)
        self.data.pop(path)
        pathSplit = path.split('/')
        if len(pathSplit) == 2:
            self.data['/'].remove(pathSplit[1])
            self.files['/']['st_nlink'] -= 1
        else:
            localPath = []
            num = 1
            while num < (len(pathSplit) - 1):
                localPath.append('/')
                localPath.append(pathSplit[num])
                num += 1
            localPath = ''.join(localPath)
            self.data[localPath].remove(pathSplit[len(pathSplit) - 1])
            self.files[localPath]['st_nlink'] -= 1

    def setxattr(self, path, name, value, options, position=0):
        # Ignore options
        attrs = self.files[path].setdefault('attrs', {})
        attrs[name] = value

    def statfs(self, path):
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    def symlink(self, target, source):
        self.files[target] = dict(st_mode=(S_IFLNK | 0o777), st_nlink=1,
                                  st_size=len(source))

        newDataInBlocks = []
        for i in range(0,len(source),self.BLKSIZE):
            newDataInBlocks.append(source[i : i + self.BLKSIZE])
        self.data[target] = newDataInBlocks

    def truncate(self, path, length, fh=None):
        newDataInBlocks = []
        oldData = ''.join(self.data[path])
        newData = oldData[:length].ljust(length,'\x00')
        for i in range(0,len(newData),self.BLKSIZE):
            newDataInBlocks.append(newData[i : i + self.BLKSIZE])
        self.data[path] = newDataInBlocks
        self.files[path]['st_size'] = length

    def unlink(self, path):
        self.files.pop(path)

    def utimens(self, path, times=None):
        now = time()
        atime, mtime = times if times else (now, now)
        self.files[path]['st_atime'] = atime
        self.files[path]['st_mtime'] = mtime

    def write(self, path, data, offset, fh):
        newDataInBlocks = []
        oldData = ''.join(self.data[path])
        newData = oldData[:offset].ljust(offset,'\x00') + data + oldData[offset + len(data):]
        for i in range(0,len(newData),self.BLKSIZE):
            newDataInBlocks.append(newData[i : i + self.BLKSIZE])
        self.data[path] = newDataInBlocks
        self.files[path]['st_size'] = len(newData)
        return len(data)


if __name__ == '__main__':
    if len(argv) < 4:
        print('usage: %s <mountpoint> <metaserver port> <dataserver port1> ...' % argv[0])
        exit(1)

    MetaServerPort = int(argv[2])
    DataServerPort = []
    for i in range(3,len(argv)):
        DataServerPort.append(int(argv[i]))

    logging.basicConfig(level=logging.DEBUG)
    fuse = FUSE(Memory(MetaServerPort,DataServerPort), argv[1], foreground=True, debug=True)
