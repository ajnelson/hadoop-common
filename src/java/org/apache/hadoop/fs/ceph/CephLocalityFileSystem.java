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

package org.apache.hadoop.fs.ceph;

import java.io.*;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.FileLock;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.*;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Shell;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.RawLocalFileSystem;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.NativeCodeLoader;

/****************************************************************
 * Extend the RawFileSystem API for the Ceph filesystem.
 *
 * Presently, deployment must include manually-compiled C++ classes.
 * Please see $HADOOP_HOME/README-HADOOP-6779.txt.
 *
 *****************************************************************/
public class CephLocalityFileSystem extends RawLocalFileSystem {
  /**
   * Inherited from HADOOP-6253's CephFileSystem.java.
   */
  private boolean initialized = false;

  /**
   * Inherited from HADOOP-6253's CephFileSystem.java.
   * //TODO Determine if it's possible to set the authority on this uri; there's no setter field.
   */
  private URI uri;

  /**
   * Inherited from RawLocalFileSystem.java.
   */
  static final URI NAME = URI.create("ceph:///");

  private static final Log LOG = LogFactory.getLog(CephLocalityFileSystem.class);

  static {
    if (!NativeCodeLoader.isNativeCodeLoaded()) {
      LOG.warn("could not load native code");
    }
  }

  public CephLocalityFileSystem() {
    super();

    //Debug
    //System.out.println(String.format("CephLocalityFileSystem() called."));
    //System.out.println("System.getProperty(\"user.dir\"):\t" + System.getProperty("user.dir"));
    //System.out.println("new Path(System.getProperty(\"user.dir\")).makeQualified(this):\t" + (new Path(System.getProperty("user.dir")).makeQualified(this)));

    //TODO Maybe we want to tweak the workingDir?
    //this.workingDir =  new Path("file:///mnt/ceph");

    //Debug
    //System.out.println(String.format("CephLocalityFileSystem() finished."));
  }

  /**
   * The default constructor for RawLocalFileSystem
   * takes environment's cwd and makes that this file system's starting
   * point.  With this constructor, specify your own starting point.
   *
   * @param startDir
   */
  public CephLocalityFileSystem(Path startDir) {
    //Debug
    //System.out.println(String.format("CephLocalityFileSystem(%s) called.", startDir.toString()));

    //this.workingDir = startDir;

    //Debug
    //System.out.println(String.format("CephLocalityFileSystem(%s) finished.", startDir.toString()));
  }

  /**
   * Return an array containing hostnames, offset and size of
   * portions of the given file.  For a nonexistent
   * file or regions, null will be returned.
   *
   * For the Ceph file system, this employs ioctl() calls through JNI.
   * The ioctl() data structures are not returned directly to Java space
   * because running ioctl() requires an open file handle.  We operate
   * the file handle logistics in local code for now; an alternative
   * is to use a JNI call to open the file handle and another call to
   * destroy it.
   */
  @Override
  public native BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len) throws IOException;

  /**
   * Convenience method for JNI, to save a few crossings between the
   * C++/Java boundary.
   *
   * @param fs
   * @return Returns the string of the path in fs.
   */
  public String getPathStringFromFileStatus(FileStatus fs) {
    return fs.getPath().toUri().getRawPath();
  }

  //TODO Ceph exposes file block size through an ioctl.  getBlockSize is deprecated; overwrite getFileStatus() instead.

  /**
   * Code inherited and evolved from HADOOP-6253's CephFileSystem.java class.
   * Probably doesn't need to bother with this.initialized member variable; 6253 used that for starting up Ceph.
   */
  @Override
  public void initialize(URI uri, Configuration conf) throws IOException{
    //Debug
    //System.out.println("CephLocalityFileSystem.initialize() called.");
    if (!this.initialized) {
      super.initialize(uri, conf);
      setConf(conf);
      this.uri=uri;  //Note:  This is not set in the superclasses, must be set here.
      //Debug
      //System.out.println("uri:\t"+uri);
      //System.out.println("uri.getScheme():\t"+uri.getScheme());
      //System.out.println("uri.getAuthority():\t"+uri.getAuthority());
      //System.out.println("this.uri:\t"+this.uri);
      //System.out.println("this.uri.getAuthority():\t"+this.uri.getAuthority());
      //System.out.println("Working directory:\t" + this.workingDir);
      this.initialized = true;
    }
    //Debug
    //System.out.println("CephLocalityFileSystem.initialize() finished.");
  }

  /**
   * Overrides RawLocalFileSystem's "file:///".
   */
  public URI getUri() {
    return NAME;
  }
}
