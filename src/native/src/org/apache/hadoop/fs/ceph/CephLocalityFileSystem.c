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
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>
#include <netdb.h>

#include "client/ioctl.h"

#include "org_apache_hadoop.h"
#include "org_apache_hadoop_fs_ceph_CephLocalityFileSystem.h"

/*
 * TODO:
 *   - strerror is not thread safe
 *   - add conditional debug statements
 *   - verify proper nested exception propagation
 *   - add memory cleanup for partial completeness?
 *   - use ClassNotFound error where appropriate
 *   - check throws() for all JNI functions
 */

static int get_file_length(JNIEnv *env, jobject j_file, jlong *len)
{
	jclass FileStatusClass;
	jmethodID getLenID;

	FileStatusClass = (*env)->GetObjectClass(env, j_file);
	if (!FileStatusClass) {
		THROW(env, "java/io/IOException", "FileStatus class not found");
		return -1;
	}

	getLenID = (*env)->GetMethodID(env, FileStatusClass, "getLen", "()J");
	if (!getLenID) {
		THROW(env, "java/io/IOException", "Could not find getLen()");
		return -1;
	}

	*len = (*env)->CallLongMethod(env, j_file, getLenID);

	return 0;
}

static int get_file_layout(JNIEnv *env, int fd,
		struct ceph_ioctl_layout *layout)
{
	struct ceph_ioctl_layout tmp_layout;
	int ret;

	ret = ioctl(fd, CEPH_IOC_GET_LAYOUT, &tmp_layout);
	if (ret < 0) {
		THROW(env, "java/io/IOException", strerror(errno));
		return ret;
	}

	*layout = tmp_layout;

	return 0;
}

static int open_ceph_file(JNIEnv *env, const char *path)
{
	int fd;

	fd = open(path, O_RDONLY);
	if (fd < 0)
		THROW(env, "java/io/IOException", strerror(errno));
	
	return fd;
}

static int get_file_offset_location(JNIEnv *env, int fd, long offset,
		struct ceph_ioctl_dataloc *dataloc)
{
	struct ceph_ioctl_dataloc tmp_dataloc;
	int ret;

	memset(&tmp_dataloc, 0, sizeof(tmp_dataloc));
	tmp_dataloc.file_offset = offset;

	ret = ioctl(fd, CEPH_IOC_GET_DATALOC, &tmp_dataloc);
	if (ret < 0) {
		THROW(env, "java/io/IOException", strerror(errno));
		return ret;
	}

	*dataloc = tmp_dataloc;

	return 0;
}

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
  int fd;
  long blocksize, numblocks;
  struct ceph_ioctl_layout ceph_layout;
  struct ceph_ioctl_dataloc dl;

  //Java...
  jmethodID constrid;              //This can probably be cached ( http://www.ibm.com/developerworks/java/library/j-jni/ )
  jmethodID methodid_getPathStringFromFileStatus;
  jclass BlockLocationClass, StringClass, CephLocalityFileSystemClass;
  jobjectArray aryBlockLocations;  //Returning item
  jstring j_path;
  jlong fileLength;
  jclass IOExceptionClass, OutOfMemoryErrorClass;


  ////Grab the exception classes for all the little things that can go wrong.
  IOExceptionClass = (*env)->FindClass(env, "java/io/IOException");
  OutOfMemoryErrorClass = (*env)->FindClass(env, "java/lang/OutOfMemoryError");
  if (IOExceptionClass == NULL || OutOfMemoryErrorClass == NULL) {
    //debug//debugstream << "Failed to get an exception to throw.  Giving up." << endl;
    return NULL;
  }

	if (!j_file)
		return NULL;

	if ((j_start < 0) || (j_len < 0)) {
		THROW(env, "java/lang/IllegalArgumentException", "Invalid start or len parameter");
		return NULL;
	}

	StringClass = (*env)->FindClass(env, "java/lang/String");
	if (!StringClass) {
		THROW(env, "java/lang/ClassNotFoundException", "java/lang/String not found");
		return NULL;
	}

	BlockLocationClass = (*env)->FindClass(env, "org/apache/hadoop/fs/BlockLocation");
	if (!BlockLocationClass) {
		THROW(env, "java/lang/ClassNotFoundException", "org/apache/hadoop/fs/BlockLocation");
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

	if (get_file_length(env, j_file, &fileLength))
		return NULL;

	if (fileLength < j_start)
		return (*env)->NewObjectArray(env, 0, BlockLocationClass, NULL);

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

  fd = open_ceph_file(env, c_path);
  if (fd < 0)
	  return NULL;

  //debug//debugstream << "File opening complete." << endl;
  //Cleanup:  Don't need file name characters anymore.
  (*env)->ReleaseStringUTFChars(env, j_path, c_path);

	if (get_file_layout(env, fd, &ceph_layout))
		return NULL;

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
    //debug//debugstream << "Running dataloc ioctl loop for dl.file_offset=" << dl.file_offset << " (curoffset=" << curoffset << ")" << endl;

	if (get_file_offset_location(env, fd, curoffset, &dl))
		return NULL;

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
