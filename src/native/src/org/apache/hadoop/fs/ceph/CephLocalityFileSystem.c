/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
This file's main function was originally in
$CEPH_HOME/src/client/test_ioctls.c .

In the present form, it converts ceph_ioctl_dataloc to BlockLocations.

BlockLocations require:
String[] names, String[] hosts, long offset, long length

The ceph_ioctl_dataloc supplies:
struct ceph_ioctl_dataloc {
	__u64 file_offset;           // in+out: file offset
	__u64 object_offset;         // out: offset in object
	__u64 object_no;             // out: object #
	__u64 object_size;           // out: object size
	char object_name[64];        // out: object name
	__u64 block_offset;          // out: offset in block
	__u64 block_size;            // out: block length
	__s64 osd;                   // out: osd #
	struct sockaddr_storage osd_addr; // out: osd address
};


The required items and the supplied items line up like so:
String[] names     dataloc.osd_addr
String[] hosts     dataloc.osd_addr
long offset        start
long length        len
*/

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <sys/ioctl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <netdb.h>

#include <string.h>  //memset

#include <errno.h>

#include <time.h>    //Debugging
//#include <iostream>  //Debugging
//#include <fstream>   //Debugging
//using namespace std; //Debugging

//JNI include
#include "org_apache_hadoop_fs_ceph_CephLocalityFileSystem.h"

//Ceph includes
#include "client/ioctl.h"

/**
 * Arguments:  (FileStatus file, long start, long len)
  Exemplar code:  Java_org_apache_hadoop_fs_ceph_CephTalker_ceph_1getdir
  Original Java function body:

  public BlockLocation[] getFileBlockLocations(FileStatus file,
      long start, long len) throws IOException {
    if (file == null) {
      return null;
    }

    if ( (start<0) || (len < 0) ) {
      throw new IllegalArgumentException("Invalid start or len parameter");
    }

    if (file.getLen() < start) {
      return new BlockLocation[0];

    }
    String[] name = { "localhost:50010" };
    String[] host = { "localhost" };
    return new BlockLocation[] { new BlockLocation(name, host, 0, file.getLen()) };
  }

   Reference for building new objects:
   http://java.sun.com/docs/books/jni/html/fldmeth.html#26254

   TODO Clean up memory.  Some reading is here:
   http://java.sun.com/developer/onlineTraining/Programming/JDCBook/jniref.html
 */
JNIEXPORT jobjectArray JNICALL Java_org_apache_hadoop_fs_ceph_CephLocalityFileSystem_getFileBlockLocations
  (JNIEnv *env, jobject obj, jobject j_file, jlong j_start, jlong j_len) {

  ////Variables
  //Native...
  //debug//const char *logpath = "/home/alex/TestGetFileBlockLocations.txt";
  const char *c_path = "";
  int fd, err;
  long c_start, c_len;
  long blocksize, numblocks;
  struct ceph_ioctl_layout ceph_layout;
  struct ceph_ioctl_dataloc dl;
  char errdesc[256];

  //Java...
  jmethodID constrid;              //This can probably be cached ( http://www.ibm.com/developerworks/java/library/j-jni/ )
  jmethodID filelenid;
  jmethodID methodid_getPathStringFromFileStatus;
  jfieldID pathfieldid;
  jclass BlockLocationClass, StringClass, FileStatusClass, CephLocalityFileSystemClass;
  jobjectArray aryBlockLocations;  //Returning item
  jstring j_path;
  jlong fileLength;
  jclass IllegalArgumentExceptionClass, IOExceptionClass, OutOfMemoryErrorClass;


  ////Debugging
  //Setup
  time_t rawtime;
  time(&rawtime);
  struct tm *timeinfo = localtime(&rawtime);
  //debug//ofstream debugstream(logpath, ios_base::app);
  //debug//debugstream << "Starting.  Current time:  " << asctime(timeinfo) << "." << endl;
  //debug//debugstream << "Arguments:  <j_file>, " << j_start << ", " << j_len << endl;
  memset(errdesc, 0, 256);


  ////Grab the exception classes for all the little things that can go wrong.
  IllegalArgumentExceptionClass = (*env)->FindClass(env, "java/lang/IllegalArgumentException");
  IOExceptionClass = (*env)->FindClass(env, "java/io/IOException");
  OutOfMemoryErrorClass = (*env)->FindClass(env, "java/lang/OutOfMemoryError");
  if (IllegalArgumentExceptionClass == NULL || IOExceptionClass == NULL || OutOfMemoryErrorClass == NULL) {
    //debug//debugstream << "Failed to get an exception to throw.  Giving up." << endl;
    return NULL;
  }


  ////Sanity-check arguments (one more check comes after class declarations)
  if (j_file == NULL) {
    return NULL;
  }


  if (j_start < 0) {
    (*env)->ThrowNew(env, IllegalArgumentExceptionClass, "Invalid start parameter (negative).");
    return NULL;
  }
  if (j_len <= 0) {
    (*env)->ThrowNew(env, IllegalArgumentExceptionClass, "Invalid len parameter (nonpositive).");
    return NULL;
  }


  ////Grab the reference to the Java classes needed to set up end structure
  StringClass = (*env)->FindClass(env, "java/lang/String");
  if (StringClass == NULL) {
    (*env)->ThrowNew(env, IOExceptionClass, "Java String class not found; dying a horrible, painful death.");
    return NULL;
  }
  BlockLocationClass = (*env)->FindClass(env, "org/apache/hadoop/fs/BlockLocation");
  if (BlockLocationClass == NULL) {
    (*env)->ThrowNew(env, IOExceptionClass, "Hadoop BlockLocation class not found.");
    return NULL;
  }
  FileStatusClass = (*env)->GetObjectClass(env, j_file);
  if (FileStatusClass == NULL) {
    (*env)->ThrowNew(env, IOExceptionClass, "Hadoop FileStatus class not found.");
    return NULL;
  }
  CephLocalityFileSystemClass = (*env)->GetObjectClass(env, obj);
  if (CephLocalityFileSystemClass == NULL) {
    (*env)->ThrowNew(env, IOExceptionClass, "Hadoop CephLocalityFileSystemClass class not found.");
    return NULL;
  }

  //debug//debugstream << "Classes retrieved." << endl;


  ////Grab class methods and members
  //(Type syntax reference: http://java.sun.com/javase/6/docs/technotes/guides/jni/spec/types.html#wp16432 )

  //Grab the file length method
  filelenid = (*env)->GetMethodID(env, FileStatusClass, "getLen", "()J");
  if (filelenid == NULL) {
    (*env)->ThrowNew(env, IOExceptionClass, "Could not get filelenid.");
    return NULL;
  }
  //debug//debugstream << "filelenid retrieval complete." << endl;

  //Grab the BlockLocation constructor
  constrid = (*env)->GetMethodID(env, BlockLocationClass, "<init>", "([Ljava/lang/String;[Ljava/lang/String;JJ)V");
  if (constrid == NULL) {
    (*env)->ThrowNew(env, IOExceptionClass, "Could not get constructor id for BlockLocationClass.");
    return NULL;
  }
  //debug//debugstream << "constrid retrieval complete." << endl;

  //Grab the helper method for quick path conversion
  methodid_getPathStringFromFileStatus = (*env)->GetMethodID(env, CephLocalityFileSystemClass, "getPathStringFromFileStatus", "(Lorg/apache/hadoop/fs/FileStatus;)Ljava/lang/String;");
  if (methodid_getPathStringFromFileStatus == NULL) {
    (*env)->ThrowNew(env, IOExceptionClass, "ERROR:  Could not get methodid_getPathStringFromFileStatus.");
    return NULL;
  }
  //debug//debugstream << "methodid_getPathStringFromFileStatus retrieval complete." << endl;


  ////Calling methods
  //Grab the file length
  fileLength = (*env)->CallLongMethod(env, j_file, filelenid);
  //debug//debugstream << "Called fileLen()." << endl;
  //One last sanity check
  if (fileLength < j_start) {
    //debug//debugstream << "Starting point after end of file; returning 0 block locations." << endl;
    aryBlockLocations = (*env)->NewObjectArray(env, 0, BlockLocationClass, NULL);
    if (aryBlockLocations == NULL) {
      (*env)->ThrowNew(env, OutOfMemoryErrorClass,"Unable to allocate 0-length BlockLocation array.");
      return NULL;
    }
    return aryBlockLocations;
  }
  //debug//debugstream << "File length according to FileStatus:  " << fileLength << endl;

  //Grab the file name
  j_path = (jstring) (*env)->CallObjectMethod(env, obj, methodid_getPathStringFromFileStatus, j_file);
  if (j_path == NULL) {
    (*env)->ThrowNew(env, IOExceptionClass, "j_path retrieval failed.");
    return NULL;
  }
  //debug//debugstream << "j_path retrieval complete." << endl;

  c_path = (*env)->GetStringUTFChars(env, j_path, NULL);
  if (c_path == NULL) {
    (*env)->ThrowNew(env, IOExceptionClass, "c_path is NULL.");
    return NULL;
  }
  //debug//debugstream << "c_path path is " << c_path << endl;


  ////Really-native code:  Start the file I/O.
  //Open the file (need descriptor for ioctl())
  fd = open(c_path , O_CREAT|O_RDWR, 0644);  //TODO Not sure why this opens RDRW and CREAT (code copied from Ceph test).  Necessary for ioctl?
  if (fd <= 0) {
    (*env)->ThrowNew(env, IOExceptionClass, "Couldn't open file.");
    //debug//debugstream << "ERROR:  Couldn't open file.  errno:  " << strerror(errno) << ".  fd:  " << strerror(fd) << endl;  //TODO Clean this up.
    return NULL;
  }
  //debug//debugstream << "File opening complete." << endl;
  //Cleanup:  Don't need file name characters anymore.
  (*env)->ReleaseStringUTFChars(env, j_path, c_path);

  //Get layout
  err = ioctl(fd, CEPH_IOC_GET_LAYOUT, (unsigned long)&ceph_layout);
  if (err) {
    (*env)->ThrowNew(env, IOExceptionClass, "ioctl failed (layout).");
    return NULL;
  }
  blocksize=ceph_layout.object_size;  //TODO (big) Expose this object size to the Java file system.
  //debug//debugstream << "Block size is " << blocksize << endl;

  //Determine the number of blocks we're looking for.  (The problem:  How many buckets.  Can't remember if there's a library call to find this quickly...)
  numblocks = (j_start+j_len-1)/blocksize - j_start/blocksize + 1;
  //debug//debugstream << "Expecting to work on " << numblocks << " blocks." << endl;

  aryBlockLocations = (jobjectArray) (*env)->NewObjectArray(env, numblocks, BlockLocationClass, NULL);
  if (aryBlockLocations == NULL) {
    (*env)->ThrowNew(env, OutOfMemoryErrorClass, "Unable to allocate BlockLocation array.");
    return NULL;
  }

  //Run an ioctl for each block.
  //jthrowable exc;
  char buf[80];
  jlong blocklength;
  jlong curoffset;
  //TODO This loop test will very probably suffer data races with updates to the file.  Oh; is that why ioctl() gets RDRW?
  jlong loopinit=j_start/blocksize;
  jlong i=loopinit;
  jlong imax;
  for (imax=j_start+j_len; i*blocksize < imax; i++) {
    //Note <=; we go through the last requested byte.
    //Set up the data location object
    curoffset = i*blocksize;
    dl.file_offset = curoffset;
    //debug//debugstream << "Running dataloc ioctl loop for dl.file_offset=" << dl.file_offset << " (curoffset=" << curoffset << ")" << endl;

    //Run the ioctl to get block location()
    err = ioctl(fd, CEPH_IOC_GET_DATALOC, (unsigned long)&dl);
    if (err) {
      sprintf(errdesc, "ioctl failed (dataloc); err=%d.", err);
      (*env)->ThrowNew(env, IOExceptionClass, errdesc);
      return NULL;
    }

    //Create string object.
    //TODO:  Check if freeing this causes a null pointer exception in Java.
    //jstring j_tmpname = env->NewStringUTF("localhost:50010");
    //jstring j_tmphost = env->NewStringUTF("localhost");
    memset(buf, 0, 80);
    getnameinfo((struct sockaddr *)&dl.osd_addr, sizeof(dl.osd_addr), buf, sizeof(buf), 0, 0, NI_NUMERICHOST);
    //debug//debugstream << "Found host " << buf << endl;
    jstring j_tmphost = (*env)->NewStringUTF(env, buf);
    //The names list should include the port number if following the example getFileBlockLocations from FileSystem;
    //however, as of 0.20.2, nothing invokes BlockLocation.getNames().
    jstring j_tmpname = (*env)->NewStringUTF(env, buf);
    if (j_tmphost == NULL || j_tmpname == NULL) {
      (*env)->ThrowNew(env, OutOfMemoryErrorClass, "Unable to convert String for name or host.");
      return NULL;
    }

    //Define an array of strings for names, and one for hosts (only going to be one element long for now)
    jobjectArray aryNames = (jobjectArray) (*env)->NewObjectArray(env, 1, StringClass, NULL);
    jobjectArray aryHosts = (jobjectArray) (*env)->NewObjectArray(env, 1, StringClass, NULL);
    if (aryHosts == NULL || aryNames == NULL) {
      (*env)->ThrowNew(env, OutOfMemoryErrorClass, "Unable to allocate String array for names or hosts.");
      return NULL;
    }

    (*env)->SetObjectArrayElement(env, aryNames, 0, j_tmpname);
    ////TODO Hunt for ArrayIndex exceptions
    //exc = env->ExceptionOccurred();
    //if (exc) {
    //  //debug//debugstream << "Exception occurred.";
    //  return NULL;
    //}
    (*env)->SetObjectArrayElement(env, aryHosts, 0, j_tmphost);
    ////Probably safe if the above one worked.


    //debug//debugstream << "imax:  " << imax << endl;
    //debug//debugstream << "curoffset:  " << curoffset << endl;
    //debug//debugstream << "blocksize:  " << blocksize << endl;
    //debug//debugstream << "imax-curoffset:  " << imax-curoffset << endl;
    blocklength = (imax-curoffset)<blocksize ? imax-curoffset : blocksize;  //TODO verify boundary condition on < vs. <=
    //debug//debugstream << "Block length:  " << blocklength << endl;
    jobject tmpBlockLocation = (*env)->NewObject(env, BlockLocationClass, constrid, aryNames, aryHosts, curoffset, blocklength);
    (*env)->SetObjectArrayElement(env, aryBlockLocations, i-loopinit, tmpBlockLocation);
    //TODO Hunt for ArrayIndex exceptions
  }
  //Reminder:  i will be 1 too large after the loop finishes.  No need to add another 1.
  //debug//debugstream << "Finished looping over " << (i-loopinit) << " of " << numblocks << " blocks." << endl;
  //debug//debugstream << "(Makes " << ((i-loopinit)==numblocks ? "" : "no ") << "sense.)" << endl;

  //Cleanup
  close(fd);
  //debug//debugstream.close();

  return aryBlockLocations;
}
