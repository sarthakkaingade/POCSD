#!/usr/bin/env python
from __future__ import print_function, absolute_import, division

import logging

import xmlrpclib, pickle, hashlib, socket

from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time, sleep

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

if not hasattr(__builtins__, 'bytes'):
    bytes = str

class Memory(LoggingMixIn, Operations):
    'Example memory filesystem. Supports multiple level of files.'

    def __init__(self,MetaServerPort,DataServerPort):
        self.fd = 0
        self.BLKSIZE = 512 - 32 # Last 32 bytes are reserved for md5sum
        self.MetaServerPort = MetaServerPort
        self.DataServerPort = DataServerPort
        print(self.MetaServerPort)
        print(self.DataServerPort)
        self.MetaServerHandle = xmlrpclib.ServerProxy('http://localhost:' + str(self.MetaServerPort) + '/')
        self.DataServerHandles = []
        for i in range(0,len(self.DataServerPort)):
            self.DataServerHandles.append(xmlrpclib.ServerProxy('http://localhost:' + str(self.DataServerPort[i]) + '/'))
        print(self.MetaServerHandle)
        print(self.DataServerHandles)
        now = time()
        self.MetaServerHandle.put('/',pickle.dumps(dict(st_mode=(S_IFDIR | 0o755), st_ctime=now,st_mtime=now, st_atime=now, st_nlink=2, data = [])))
        print(pickle.loads(self.MetaServerHandle.get('/')))

    def chmod(self, path, mode):
        metaData = pickle.loads(self.MetaServerHandle.get(path))
        metaData['st_mode'] &= 0o770000
        metaData['st_mode'] |= mode
        self.MetaServerHandle.put(path,pickle.dumps(metaData))
        return 0

    def chown(self, path, uid, gid):
        metaData = pickle.loads(self.MetaServerHandle.get(path))
        metaData['st_uid'] = uid
        metaData['st_gid'] = gid
        self.MetaServerHandle.put(path,pickle.dumps(metaData))

    def create(self, path, mode):
        self.MetaServerHandle.put(path,pickle.dumps(dict(st_mode=(S_IFREG | mode), st_nlink=1, st_size=0, st_ctime=time(), st_mtime=time(), st_atime=time(), data = [], blocks = [])))
        pathSplit = path.split('/')
        if len(pathSplit) == 2:
            metaData = pickle.loads(self.MetaServerHandle.get('/'))
            metaData['data'].append(pathSplit[1])
            self.MetaServerHandle.put('/',pickle.dumps(metaData))
        else:
            localPath = []
            num = 1
            while num < (len(pathSplit) - 1):
                localPath.append('/')
                localPath.append(pathSplit[num])
                num += 1
            localPath = ''.join(localPath)
            metaData = pickle.loads(self.MetaServerHandle.get(localPath))
            metaData['data'].append(pathSplit[len(pathSplit) - 1])
            self.MetaServerHandle.put(localPath,pickle.dumps(metaData))
        self.fd += 1    #ToDo - Handle fd
        return self.fd

    def getattr(self, path, fh=None):
        if self.MetaServerHandle.get(path) == -1:
            raise FuseOSError(ENOENT)

        return pickle.loads(self.MetaServerHandle.get(path))

    def getxattr(self, path, name, position=0):
        if self.MetaServerHandle.get(path) == -1:
            return ''   # Should return ENOATTR
        metaData = pickle.loads(self.MetaServerHandle.get(path))
        attrs = metaData.get('attrs', {})

        try:
            return attrs[name]
        except KeyError:
            return ''       # Should return ENOATTR

    def listxattr(self, path):
        if self.MetaServerHandle.get(path) == -1:
            return ''   # Should return ENOATTR
        metaData = pickle.loads(self.MetaServerHandle.get(path))
        attrs = metaData.get('attrs', {})
        return attrs.keys()

    def mkdir(self, path, mode):
        self.MetaServerHandle.put(path,pickle.dumps(dict(st_mode=(S_IFDIR | mode), st_ctime=time(),st_mtime=time(), st_atime=time(), st_nlink=2, st_size=0, data = [])))

        pathSplit = path.split('/')
        if len(pathSplit) == 2:
            metaData = pickle.loads(self.MetaServerHandle.get('/'))
            metaData['data'].append(pathSplit[1])
            metaData['st_nlink'] += 1
            self.MetaServerHandle.put('/',pickle.dumps(metaData))
        else:
            localPath = []
            num = 1
            while num < (len(pathSplit) - 1):
                localPath.append('/')
                localPath.append(pathSplit[num])
                num += 1
            localPath = ''.join(localPath)
            metaData = pickle.loads(self.MetaServerHandle.get(localPath))
            metaData['data'].append(pathSplit[len(pathSplit) - 1])
            metaData['st_nlink'] += 1
            self.MetaServerHandle.put(localPath,pickle.dumps(metaData))


    def open(self, path, flags):
        self.fd += 1
        return self.fd

    def read(self, path, size, offset, fh):
        metaData = pickle.loads(self.MetaServerHandle.get(path))
        Data = self.readData(path,metaData['blocks'])
        return Data[offset:offset + size]

    def readdir(self, path, fh):
        dirlist = ['.', '..']
        metaData = pickle.loads(self.MetaServerHandle.get(path))
        return  dirlist + metaData['data']

    def readlink(self, path):
        metaData = pickle.loads(self.MetaServerHandle.get(path))
        Data = self.readData(path,metaData['blocks'])
        return Data

    def removexattr(self, path, name):
        if self.MetaServerHandle.get(path) == -1:
            return ''   # Should return ENOATTR
        metaData = pickle.loads(self.MetaServerHandle.get(path))
        attrs = metaData.get('attrs', {})

        try:
            del attrs[name]
            metaData.set('attrs', attrs)
            self.MetaServerHandle.put(path,pickle.dumps(metaData))
        except KeyError:
            pass        # Should return ENOATTR

    def rename(self, old, new):
        metaDataOld = pickle.loads(self.MetaServerHandle.pop_entry(old))
        metaDataNew = metaDataOld
        if metaDataOld['st_nlink'] == 1:
            metaDataNew = self.replaceFileData(new,old,metaDataOld)
        self.MetaServerHandle.put(new,pickle.dumps(metaDataNew))
        oldpathSplit = old.split('/')
        newpathSplit = new.split('/')
        # Check if renaming is done in same parent directory
        if len(oldpathSplit) == len(newpathSplit):
            if len(oldpathSplit) == 2:
                metaData = pickle.loads(self.MetaServerHandle.get('/'))
                metaData['data'].remove(oldpathSplit[1])
                metaData['data'].append(newpathSplit[1])
                self.MetaServerHandle.put('/',pickle.dumps(metaData))
            else:
                localPath = []
                num = 1
                while num < (len(oldpathSplit) - 1):
                    localPath.append('/')
                    localPath.append(oldpathSplit[num])
                    num += 1
                localPath = ''.join(localPath)
                metaData = pickle.loads(self.MetaServerHandle.get(localPath))
                metaData['data'].remove(oldpathSplit[len(oldpathSplit) - 1])
                metaData['data'].append(newpathSplit[len(newpathSplit) - 1])
                self.MetaServerHandle.put(localPath,pickle.dumps(metaData))
            # Replace if it is directory
            if metaDataOld['st_nlink'] != 1:
                for x in self.MetaServerHandle.get_keys():
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
                            metaDataOldx = pickle.loads(self.MetaServerHandle.pop_entry(oldx))
                            metaDataNewx= metaDataOldx
                            if metaDataOldx['st_nlink'] == 1:
                                metaDataNewx = self.replaceFileData(newx,oldx,metaDataOldx)
                            self.MetaServerHandle.put(newx,pickle.dumps(metaDataNewx))
        else:
            if len(oldpathSplit) == 2:
                metaData = pickle.loads(self.MetaServerHandle.get('/'))
                metaData['data'].remove(oldpathSplit[1])
                self.MetaServerHandle.put('/',pickle.dumps(metaData))
            else:
                oldParentPath = []
                num = 1
                while num < (len(oldpathSplit) - 1):
                    oldParentPath.append('/')
                    oldParentPath.append(oldpathSplit[num])
                    num += 1
                oldParentPath = ''.join(oldParentPath)
                metaData = pickle.loads(self.MetaServerHandle.get(oldParentPath))
                metaData['data'].remove(oldpathSplit[len(oldpathSplit) - 1])
                self.MetaServerHandle.put(oldParentPath,pickle.dumps(metaData))
            if len(newpathSplit) == 2:
                metaData = pickle.loads(self.MetaServerHandle.get('/'))
                metaData['data'].append(newpathSplit[1])
                self.MetaServerHandle.put('/',pickle.dumps(metaData))
            else:
                newParentPath = []
                num = 1
                while num < (len(newpathSplit) - 1):
                    newParentPath.append('/')
                    newParentPath.append(newpathSplit[num])
                    num += 1
                newParentPath = ''.join(newParentPath)
                metaData = pickle.loads(self.MetaServerHandle.get(newParentPath))
                metaData['data'].append(newpathSplit[len(newpathSplit) - 1])
                self.MetaServerHandle.put(newParentPath,pickle.dumps(metaData))
            # If it is a directory
            if metaDataOld['st_nlink'] != 1:
                for x in self.MetaServerHandle.get_keys():
                    if new != x[:len(new)]:
                        oldx = x;
                        newx = x.replace(old,new,1)
                        metaDataOldx = pickle.loads(self.MetaServerHandle.pop_entry(oldx))
                        metaDataNewx= metaDataOldx
                        if metaDataOldx['st_nlink'] == 1:
                            metaDataNewx = self.replaceFileData(newx,oldx,metaDataOldx)
                        self.MetaServerHandle.put(newx,pickle.dumps(metaDataNewx))


    def rmdir(self, path):
        # Todo - Free RAM
        self.MetaServerHandle.pop_entry(path)
        pathSplit = path.split('/')
        if len(pathSplit) == 2:
            metaData = pickle.loads(self.MetaServerHandle.get('/'))
            metaData['data'].remove(pathSplit[1])
            metaData['st_nlink'] -= 1
            self.MetaServerHandle.put('/',pickle.dumps(metaData))
        else:
            localPath = []
            num = 1
            while num < (len(pathSplit) - 1):
                localPath.append('/')
                localPath.append(pathSplit[num])
                num += 1
            localPath = ''.join(localPath)
            metaData = pickle.loads(self.MetaServerHandle.get(localPath))
            metaData['data'].remove(pathSplit[len(pathSplit) - 1])
            metaData['st_nlink'] -= 1
            self.MetaServerHandle.put(localPath,pickle.dumps(metaData))

    def setxattr(self, path, name, value, options, position=0):
        # Ignore options
        if self.MetaServerHandle.get(path) == -1:
            return ''   # Should return ENOATTR
        metaData = pickle.loads(self.MetaServerHandle.get(path))
        attrs = metaData.setdefault('attrs', {})
        attrs[name] = value
        metaData.set('attrs', attrs)
        self.MetaServerHandle.put(path,pickle.dumps(metaData))

    def statfs(self, path):
        return dict(f_bsize=self.BLKSIZE, f_blocks=4096, f_bavail=2048)

    def symlink(self, target, source):
        x = hash(path)
        newDataInBlocks = []
        blocks = []
        j = 1
        for i in range(0,len(source),self.BLKSIZE):
            newDataInBlocks.append(source[i : i + self.BLKSIZE])
            blocks.append((x + j - 1) % len(self.DataServerPort))
            j += 1;
        self.writeData(target,newDataInBlocks,blocks)
        self.MetaServerHandle.put(target,pickle.dumps(dict(st_mode=(S_IFLNK | 0o777), st_nlink=1, st_size=len(source), blocks = blocks)))

    def truncate(self, path, length, fh=None):
        newDataInBlocks = []
        blocks = []
        x = hash(path)
        metaData = pickle.loads(self.MetaServerHandle.get(path))
        oldData = self.readData(path,metaData['blocks'])
        newData = oldData[:length].ljust(length,'\x00')
        j = 1
        for i in range(0,len(newData),self.BLKSIZE):
            newDataInBlocks.append(newData[i : i + self.BLKSIZE])
            blocks.append((x + j - 1) % len(self.DataServerPort))
            j += 1;
        self.writeData(path,newDataInBlocks,blocks)
        metaData['st_size'] = len(newData)
        metaData['blocks'] = blocks
        self.MetaServerHandle.put(path,pickle.dumps(metaData))

    def unlink(self, path):
        metaData = pickle.loads(self.MetaServerHandle.pop_entry(path))
        if (metaData['st_mode'] & S_IFREG) == S_IFREG:
            self.rmData(path,metaData['blocks'])
            pathSplit = path.split('/')
            if len(pathSplit) == 2:
                metaData = pickle.loads(self.MetaServerHandle.get('/'))
                metaData['data'].remove(pathSplit[1])
                self.MetaServerHandle.put('/',pickle.dumps(metaData))
            else:
                localPath = []
                num = 1
                while num < (len(pathSplit) - 1):
                    localPath.append('/')
                    localPath.append(pathSplit[num])
                    num += 1
                localPath = ''.join(localPath)
                metaData = pickle.loads(self.MetaServerHandle.get(localPath))
                metaData['data'].remove(pathSplit[len(pathSplit) - 1])
                self.MetaServerHandle.put(localPath,pickle.dumps(metaData))

    def utimens(self, path, times=None):
        now = time()
        atime, mtime = times if times else (now, now)
        metaData = pickle.loads(self.MetaServerHandle.get(path))
        metaData['st_atime'] = atime
        metaData['st_mtime'] = mtime
        self.MetaServerHandle.put(path,pickle.dumps(metaData))

    def write(self, path, data, offset, fh):
        newDataInBlocks = []
        blocks = []
        x = hash(path)
        metaData = pickle.loads(self.MetaServerHandle.get(path))
        if len(metaData['blocks']) == 0:
            oldData = ''
        else:
            oldData = self.readData(path,metaData['blocks'])
        newData = oldData[:offset].ljust(offset,'\x00') + data + oldData[offset + len(data):]
        j = 1
        for i in range(0,len(newData),self.BLKSIZE):
            newDataInBlocks.append(newData[i : i + self.BLKSIZE])
            blocks.append((x + j - 1) % len(self.DataServerPort))
            j += 1;

        self.writeData(path,newDataInBlocks,blocks)
        metaData['st_size'] = len(newData)
        metaData['blocks'] = blocks
        self.MetaServerHandle.put(path,pickle.dumps(metaData))
        return len(data)

    def writeData(self,path,newDataInBlocks,blocks):
        for i in range(0,len(newDataInBlocks)):
            # Store First copy of block
            connection = False
            while connection is False:
                try:
                    # Store block
                    connection = self.DataServerHandles[blocks[i]].put(path + str(i),newDataInBlocks[i] + hashlib.md5(newDataInBlocks[i]).hexdigest())
                except socket.error:
                    print('WRITE - Connection to Data Server at port ' + str(self.DataServerPort[blocks[i]]) + ' is lost.')
                    print('Trying to re-connect.....')
                    sleep(5)

            # Store Second copy of block
            connection = False
            while connection is False:
                try:
                    # Store block
                    connection = self.DataServerHandles[(blocks[i] + 1) % len(self.DataServerPort)].put(path + str(i),newDataInBlocks[i] + hashlib.md5(newDataInBlocks[i]).hexdigest())
                except socket.error:
                    print('WRITE - Connection to Data Server at port ' + str(self.DataServerPort[(blocks[i] + 1) % len(self.DataServerPort)]) + ' is lost.')
                    print('Trying to re-connect.....')
                    sleep(5)


    def readData(self,path,blocks):
        result = ''
        for i in range(0,len(blocks)):
            try:
                localblock1 = self.DataServerHandles[blocks[i]].get(path + str(i))
            except socket.error:
                print('**WARNING** READ - Connection to Data Server at port ' + str(self.DataServerPort[blocks[i]]) + ' is lost.')
                localblock1 = -1
            try:
                localblock2 = self.DataServerHandles[(blocks[i] + 1) % len(self.DataServerPort)].get(path + str(i))
            except socket.error:
                print('**WARNING** READ - Connection to Data Server at port ' + str(self.DataServerPort[(blocks[i] + 1) % len(self.DataServerPort)]) + ' is lost.')
                localblock2 = -1
            # Both the copies are lost
            if (localblock1 == -1) and (localblock2 == -1):
                result = '**ERROR** - Two adjacent servers lost their respective persistance storage blocks. Cannot RECOVER.'
                break
            # Both the copies are present
            elif (localblock1 != -1) and (localblock2 != -1):
                # Verify Checksum
                if (localblock1[len(localblock1) - 32:] == hashlib.md5(localblock1[:len(localblock1) - 32]).hexdigest()) and (localblock2[len(localblock2) - 32:] == hashlib.md5(localblock2[:len(localblock2) - 32]).hexdigest()):
                    result += localblock1[:len(localblock1) - 32]
                elif (localblock1[len(localblock1) - 32:] != hashlib.md5(localblock1[:len(localblock1) - 32]).hexdigest()) and (localblock2[len(localblock2) - 32:] != hashlib.md5(localblock2[:len(localblock2) - 32]).hexdigest()):
                    result = '**ERROR** - Both the copies are corrupted.'
                    for i in range(0,2):
                        print(result)
                        sleep(2)
                    break
                elif (localblock1[len(localblock1) - 32:] != hashlib.md5(localblock1[:len(localblock1) - 32]).hexdigest()):
                    print('First copy is corrupted. Recovering....')
                    # Update First Copy
                    try:
                        self.DataServerHandles[blocks[i]].put(path + str(i),localblock2)
                    except socket.error:
                        print('**WARNING** - Data server at port ' + str(self.DataServerPort[blocks[i]]) + ' is absent while data correction.')
                    result += localblock2[:len(localblock2) - 32]
                elif (localblock2[len(localblock2) - 32:] != hashlib.md5(localblock2[:len(localblock2) - 32]).hexdigest()):
                    print('Second copy is corrupted. Recovering....')
                    # Update Second Copy
                    try:
                        self.DataServerHandles[(blocks[i] + 1) % len(self.DataServerPort)].put(path + str(i),localblock1)
                    except socket.error:
                        print('**WARNING** - Data server at port ' + str(self.DataServerPort[(blocks[i] + 1) % len(self.DataServerPort)]) + ' is absent while data correction.')
                    result += localblock1[:len(localblock1) - 32]
                else:
                    result = '**ERROR** - Unhandled Exception while verifying checksum'
                    break
            # First copy lost
            elif (localblock1 == -1):
                # Verify Checksum
                if (localblock2[len(localblock2) - 32:] != hashlib.md5(localblock2[:len(localblock2) - 32]).hexdigest()):
                    result = '**ERROR** - First copy is absent and Second copy is corrupted.'
                    for i in range(0,2):
                        print(result)
                        sleep(2)
                    break
                # Update First Copy
                try:
                    self.DataServerHandles[blocks[i]].put(path + str(i),localblock2)
                except socket.error:
                    print('**WARNING** - Data server at port ' + str(self.DataServerPort[blocks[i]]) + ' is absent while data correction.')
                result += localblock2[:len(localblock2) - 32]
            # Second copy lost
            elif (localblock2 == -1):
                # Verify Checksum
                if (localblock1[len(localblock1) - 32:] != hashlib.md5(localblock1[:len(localblock1) - 32]).hexdigest()):
                    result = '**ERROR** - Second copy is absent and First copy is corrupted.'
                    for i in range(0,2):
                        print(result)
                        sleep(2)
                    break
                # Update Second Copy
                try:
                    self.DataServerHandles[(blocks[i] + 1) % len(self.DataServerPort)].put(path + str(i),localblock1)
                except socket.error:
                    print('**WARNING** - Data server at port ' + str(self.DataServerPort[(blocks[i] + 1) % len(self.DataServerPort)]) + ' is absent while data correction.')
                result += localblock1[:len(localblock1) - 32]
            else:
                result = '**ERROR** - Unhandled Exception'
                break
        return result

    def rmData(self,path,blocks):
        for i in range(0,len(blocks)):
            try:
                self.DataServerHandles[blocks[i]].pop_entry(path + str(i))
            except socket.error:
                print('**WARNING** - Data Removel: Data server at port ' + str(self.DataServerPort[blocks[i]]) + ' is absent.')
            try:
                self.DataServerHandles[(blocks[i] + 1) % len(self.DataServerPort)].pop_entry(path + str(i))
            except socket.error:
                print('**WARNING** - Data Removal: Data server at port ' + str(self.DataServerPort[(blocks[i] + 1) % len(self.DataServerPort)]) + ' is absent.')

    def replaceFileData(self,new,old,metaDataOld):
        metaDataNew = metaDataOld
        Data = self.readData(old,metaDataOld['blocks'])
        self.rmData(old,metaDataOld['blocks'])
        DataInBlocks = []
        blocks = []
        x = hash(new)
        j = 1
        for i in range(0,len(Data),self.BLKSIZE):
            DataInBlocks.append(Data[i : i + self.BLKSIZE])
            blocks.append((x + j - 1) % len(self.DataServerPort))
            j += 1;

        self.writeData(new,DataInBlocks,blocks)
        metaDataNew['st_size'] = len(Data)
        metaDataNew['blocks'] = blocks
        return metaDataNew

if __name__ == '__main__':
    if len(argv) < 6:
        print('usage: %s <mountpoint> <metaserver port> <dataserver port0> <dataserver port1> <dataserver port2> ...' % argv[0])
        print('Number of dataservers should be greater than 2')
        exit(1)

    MetaServerPort = int(argv[2])
    DataServerPort = []
    for i in range(3,len(argv)):
        DataServerPort.append(int(argv[i]))

    logging.basicConfig(level=logging.DEBUG)
    fuse = FUSE(Memory(MetaServerPort,DataServerPort), argv[1], foreground=True, debug=True)
