#!/usr/bin/env python
"""
Author: David Wolinsky
Version: 0.03

Description:
The XmlRpc API for this library is:
  get(base64 key)
    Returns the value associated with the given key using a dictionary
      or an empty dictionary if there is no matching key
    Example usage:
      rv = rpc.get(Binary("key"))
      print rv => Binary
      print rv.data => "value"
  put(base64 key, base64 value)
    Inserts the key / value pair into the hashtable, using the same key will
      over-write existing values
    Example usage:  rpc.put(Binary("key"), Binary("value"))
  print_content()
    Print the contents of the HT
  read_file(string filename)
    Store the contents of the Hahelperable into a file
  write_file(string filename)
    Load the contents of the file into the Hahelperable

Changelog:
    0.03 - Modified to remove timeout mechanism for data.
"""

import sys, SimpleXMLRPCServer, getopt, pickle, shelve, time, threading, xmlrpclib, unittest
from datetime import datetime, timedelta
from xmlrpclib import Binary

# Presents a HT interface
class SimpleHT:
  def __init__(self,filename):
    self.filename = filename
    self.data = shelve.open(filename)
    self.data.close()

  def count(self):
    self.data = shelve.open(self.filename)
    length = len(self.data.keys())
    self.data.close()
    return length

  # Retrieve something from the HT
  def get(self, key):
    self.data = shelve.open(self.filename)
    # return value
    if self.data.has_key(key):
        localdata = self.data[key]
    else:
        localdata = -1
    self.data.close()
    return localdata

  # Insert something into the HT
  def put(self, key, value):
    self.data = shelve.open(self.filename)
    self.data[key] = value
    self.data.close()
    return True

  # Load contents from a file
  def read_file(self, filename):
    f = open(filename.data, "rb")
    #self.data = pickle.load(f)
    f.close()
    return True

  # Write contents to a file
  def write_file(self, filename):
    f = open(filename.data, "wb")
    #pickle.dump(self.data, f)
    f.close()
    return True

  # Print the contents of the hashtable
  def print_content(self):
    #print self.data
    return True

  def pop_entry(self,key):
    self.data = shelve.open(self.filename)
    if self.data.has_key(key):
        entry = self.data[key]
        del self.data[key]
    else:
      entry = ''
    self.data.close()
    return entry

  def get_keys(self):
    self.data = shelve.open(self.filename)
    localkeys = self.data.keys()
    self.data.close()
    return localkeys

  def corrupt(self,path):
    self.data = shelve.open(self.filename)
    for x in self.data.keys():
        if path == x[:len(path)]:
            local = self.data[x]
            self.data[x] = local[:len(local) - 7] + 'sarthak'
            self.data.close()
            return True
    self.data.close()
    return False

def main():
  optlist, args = getopt.getopt(sys.argv[1:], "", ["port=", "test"])
  ol={}
  for k,v in optlist:
    ol[k] = v

  port = int(sys.argv[int(sys.argv[1]) + 2])
  if "--port" in ol:
    port = int(ol["--port"])
  if "--test" in ol:
    sys.argv.remove("--test")
    unittest.main()
    return
  print('Dataserver ' + str(int(sys.argv[1])) + ' started at port ' + str(port))
  filename = sys.path[0] + '/datastore' + str(port)
  serve(port,filename)

# Start the xmlrpc server
def serve(port,filename):
  file_server = SimpleXMLRPCServer.SimpleXMLRPCServer(("localhost", port))
  file_server.register_introspection_functions()
  sht = SimpleHT(filename)
  file_server.register_function(sht.get)
  file_server.register_function(sht.put)
  file_server.register_function(sht.print_content)
  file_server.register_function(sht.read_file)
  file_server.register_function(sht.write_file)
  file_server.register_function(sht.pop_entry)
  file_server.register_function(sht.get_keys)
  file_server.register_function(sht.corrupt)
  file_server.serve_forever()

# Execute the xmlrpc in a thread ... needed for testing
class serve_thread:
  def __call__(self, port):
    serve(port)

# Wrapper functions so the tests don't need to be concerned about Binary blobs
class Helper:
  def __init__(self, caller):
    self.caller = caller

  def put(self, key, val, ttl):
    return self.caller.put(Binary(key), Binary(val), ttl)

  def get(self, key):
    return self.caller.get(Binary(key))

  def write_file(self, filename):
    return self.caller.write_file(Binary(filename))

  def read_file(self, filename):
    return self.caller.read_file(Binary(filename))

class SimpleHTTest(unittest.TestCase):
  def test_direct(self):
    helper = Helper(SimpleHT())
    self.assertEqual(helper.get("test"), {}, "DHT isn't empty")
    self.assertTrue(helper.put("test", "test", 10000), "Failed to put")
    self.assertEqual(helper.get("test")["value"], "test", "Failed to perform single get")
    self.assertTrue(helper.put("test", "test0", 10000), "Failed to put")
    self.assertEqual(helper.get("test")["value"], "test0", "Failed to perform overwrite")
    self.assertTrue(helper.put("test", "test1", 2), "Failed to put" )
    self.assertEqual(helper.get("test")["value"], "test1", "Failed to perform overwrite")
    time.sleep(2)
    self.assertEqual(helper.get("test"), {}, "Failed expire")
    self.assertTrue(helper.put("test", "test2", 20000))
    self.assertEqual(helper.get("test")["value"], "test2", "Store new value")

    helper.write_file("test")
    helper = Helper(SimpleHT())

    self.assertEqual(helper.get("test"), {}, "DHT isn't empty")
    helper.read_file("test")
    self.assertEqual(helper.get("test")["value"], "test2", "Load unsuccessful!")
    self.assertTrue(helper.put("some_other_key", "some_value", 10000))
    self.assertEqual(helper.get("some_other_key")["value"], "some_value", "Different keys")
    self.assertEqual(helper.get("test")["value"], "test2", "Verify contents")

  # Test via RPC
  def test_xmlrpc(self):
    output_thread = threading.Thread(target=serve_thread(), args=(51234, ))
    output_thread.setDaemon(True)
    output_thread.start()

    time.sleep(1)
    helper = Helper(xmlrpclib.Server("http://127.0.0.1:51234"))
    self.assertEqual(helper.get("test"), {}, "DHT isn't empty")
    self.assertTrue(helper.put("test", "test", 10000), "Failed to put")
    self.assertEqual(helper.get("test")["value"], "test", "Failed to perform single get")
    self.assertTrue(helper.put("test", "test0", 10000), "Failed to put")
    self.assertEqual(helper.get("test")["value"], "test0", "Failed to perform overwrite")
    self.assertTrue(helper.put("test", "test1", 2), "Failed to put" )
    self.assertEqual(helper.get("test")["value"], "test1", "Failed to perform overwrite")
    time.sleep(2)
    self.assertEqual(helper.get("test"), {}, "Failed expire")
    self.assertTrue(helper.put("test", "test2", 20000))
    self.assertEqual(helper.get("test")["value"], "test2", "Store new value")

if __name__ == "__main__":
  main()
