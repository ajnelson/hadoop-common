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
 *   - handle bad layout data gracefully
 *   - figure out a good way to handle ports
 *   - (DONE) verify proper nested exception propagation
 *   - (DONE) add memory cleanup for partial completeness?
 *   - (DONE) use ClassNotFound error where appropriate
 *   - (DONE) check throws() for all JNI functions
 *   - (DONE) check for exception handling backward compat
 *   - (DONE) should cache class and field ids
 */

#define EXCEPTION_PATH		"java/lang/Exception"
#define IOEXCEPTION_PATH	"java/io/IOException"
#define ARGEXCEPTION_PATH	"java/lang/IllegalArgumentException"

#define STRING_PATH			"java/lang/String"
#define BLOCKLOCATION_PATH	"org/apache/hadoop/fs/BlockLocation"

static jclass string_cls;
static jclass blocklocation_cls;
static jmethodID blocklocation_ctor;

static int get_file_layout(JNIEnv *env, int fd,
		struct ceph_ioctl_layout *layout)
{
	struct ceph_ioctl_layout tmp_layout;
	int ret;

	ret = ioctl(fd, CEPH_IOC_GET_LAYOUT, &tmp_layout);
	if (ret < 0) {
		THROW(env, IOEXCEPTION_PATH, strerror(errno));
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
		THROW(env, IOEXCEPTION_PATH, strerror(errno));
	
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
		THROW(env, IOEXCEPTION_PATH, strerror(errno));
		return ret;
	}

	*dataloc = tmp_dataloc;

	return 0;
}

static jobject build_block(JNIEnv *env, struct ceph_ioctl_dataloc *dl,
			   __u64 block_start, __u64 len)
{
	jobject block;
	jstring host, name;
	jobjectArray hosts, names;
	char hostbuf[NI_MAXHOST];

	memset(hostbuf, 0, sizeof(hostbuf));

	if (getnameinfo((struct sockaddr *)&dl->osd_addr, sizeof(dl->osd_addr),
				hostbuf, sizeof(hostbuf), NULL, 0, NI_NUMERICHOST)) {

		THROW(env, IOEXCEPTION_PATH, strerror(errno));
		return NULL;
	}

	/*
	 * Setup host
	 */

	host = (*env)->NewStringUTF(env, hostbuf);
	if (!host)
		return NULL;

	hosts = (*env)->NewObjectArray(env, 1, string_cls, NULL);
	if (!hosts)
		return NULL;

	(*env)->SetObjectArrayElement(env, hosts, 0, host);
	if ((*env)->ExceptionCheck(env))
		return NULL;

	(*env)->DeleteLocalRef(env, host);

	/*
	 * Setup name
	 */

	name = (*env)->NewStringUTF(env, ""); /* Java can re-assigns with port info */
	if (!name)
		return NULL;

	names = (*env)->NewObjectArray(env, 1, string_cls, NULL);
	if (!names)
		return NULL;

	(*env)->SetObjectArrayElement(env, names, 0, name);
	if ((*env)->ExceptionCheck(env))
		return NULL;

	(*env)->DeleteLocalRef(env, name);

	block = (*env)->NewObject(env, blocklocation_cls, blocklocation_ctor,
			names, hosts, block_start, len);
	if (!block)
		return NULL;

	(*env)->DeleteLocalRef(env, hosts);
	(*env)->DeleteLocalRef(env, names);

	return block;
}

JNIEXPORT void JNICALL
Java_org_apache_hadoop_fs_ceph_CephLocalityFileSystem_initIDs
	(JNIEnv *env, jclass class)
{
	jclass cls;
	
	/*
	 * Cache Java String class
	 */
	cls = (*env)->FindClass(env, STRING_PATH);
	if (!cls)
		goto out;

	string_cls = (*env)->NewGlobalRef(env, cls);

	/*
	 * Cache Hadoop BlockLocation classs
	 */
	cls = (*env)->FindClass(env, BLOCKLOCATION_PATH);
	if (!cls)
		goto out;

	blocklocation_cls = (*env)->NewGlobalRef(env, cls);

	/*
	 * Cache Hadoop BlockLocation class constructor
	 */
	blocklocation_ctor = (*env)->GetMethodID(env, blocklocation_cls,
			"<init>", "([Ljava/lang/String;[Ljava/lang/String;JJ)V");
	if (!blocklocation_ctor)
		goto out;

	return;

out:
	if (string_cls)
		(*env)->DeleteGlobalRef(env, string_cls);

	if (blocklocation_cls)
		(*env)->DeleteGlobalRef(env, blocklocation_cls);
}

JNIEXPORT jobjectArray JNICALL
Java_org_apache_hadoop_fs_ceph_CephLocalityFileSystem_getFileBlockLocations
	(JNIEnv *env, jobject obj, jobject j_file, jstring j_path,
	 jlong j_start, jlong j_len, jlong j_filelength)
{
	int fd;
	const char *c_path;
	struct ceph_ioctl_layout ceph_layout;
	struct ceph_ioctl_dataloc dl;
    __u64 offset_start, offset_base, offset_end;
    __u64 len, total_len, stripe_unit, num_blocks, i;
	__u64 block_start, block_end, stripe_end;

	jobject block;
	jobjectArray blocks;
	jlong fileLength;

	if (!j_file)
		return NULL;

	/*
	 * The striping algorithm below assumes len > 0
	 * TODO:
	 *   - Do any FileSystem users have len == 0 edge cases?
	 */
	if ((j_start < 0) || (j_len <= 0)) {
		THROW(env, ARGEXCEPTION_PATH, "Invalid start or len parameter");
		return NULL;
	}

    /* Upgrade to 64-bits */
    len = j_len;
    offset_start = j_start;
    fileLength = j_filelength;

	if (fileLength < j_start)
		return (*env)->NewObjectArray(env, 0, blocklocation_cls, NULL);
	
	c_path = (*env)->GetStringUTFChars(env, j_path, NULL);
	if (!c_path) {
		THROW(env, EXCEPTION_PATH, "GetStringUTFChars Failed");
		return NULL;
	}

	fd = open_ceph_file(env, c_path);
	if (fd < 0) {
		(*env)->ReleaseStringUTFChars(env, j_path, c_path);
		return NULL;
	}
	
	(*env)->ReleaseStringUTFChars(env, j_path, c_path);

	if (get_file_layout(env, fd, &ceph_layout))
		return NULL;

    stripe_unit = ceph_layout.stripe_unit;

    /*
     * Adjust for extents that span stripe units
     */
    offset_end = offset_start + len;
    offset_base = offset_start - (offset_start % stripe_unit);
    total_len = offset_end - offset_base;
    num_blocks = total_len / stripe_unit;
    
    if (total_len % stripe_unit)
        num_blocks++;
    
    blocks = (jobjectArray) (*env)->NewObjectArray(env, num_blocks, blocklocation_cls, NULL);
    if (!blocks)
        return NULL;

	block_start = offset_start;

	for (i = 0; i < num_blocks; i++) {

		stripe_end = block_start + stripe_unit - (block_start % stripe_unit);
		
		if (offset_end < stripe_end)
			block_end = offset_end;
		else
			block_end = stripe_end;

		if (get_file_offset_location(env, fd, block_start, &dl))
			return NULL;

		block = build_block(env, &dl, block_start, block_end-block_start);
		if (!block)
			return NULL;

		(*env)->SetObjectArrayElement(env, blocks, i, block);
		if ((*env)->ExceptionCheck(env))
			return NULL;

		(*env)->DeleteLocalRef(env, block);
	}

	if (close(fd) < 0) {
		THROW(env, IOEXCEPTION_PATH, strerror(errno));
		return NULL;
	}
	
	return blocks; 
}
