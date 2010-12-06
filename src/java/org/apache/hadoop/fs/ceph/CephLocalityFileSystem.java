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

import java.io.IOException;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.NativeCodeLoader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.RawLocalFileSystem;

/**
 * The Ceph File System
 *
 * Currently this is a thin wrapper around RawLocalFileSystem
 * that redirects getFileBlockLocations to JNI provided code
 * that consults the Ceph kernel client.
 *
 * TODO:
 *  - Treatment of the URI doesn't appear to be correct. For example
 *    while this works well with MapReduce, "hadoop fs -ls" compalins
 *    that the URI isn't "file".
 */
public class CephLocalityFileSystem extends RawLocalFileSystem {
  
  private static final Log LOG =
        LogFactory.getLog(CephLocalityFileSystem.class);

  static final URI uri = URI.create("ceph:///");

  /**
   * Load libhadoop which should contain the JNI code that allows
   * getFileBlockLocations to function correctly by using a file's IOCTL to
   * consult the Ceph kernel client.
   */
  static {
    if (NativeCodeLoader.isNativeCodeLoaded()) {
      initIDs();
    } else {
      /* 
       * TODO: this should completely bomb out, or gracefully use no locality
       * info provided by Ceph.
       */
      LOG.warn("Could not load native code...");
    }
  }

  public CephLocalityFileSystem() {
    super();
  }

  /**
   * Return null if the file doesn't exist. Otherwise the locations of the
   * stripe units in objects in the Ceph file system system are returned.
   */
  @Override
  public BlockLocation[] getFileBlockLocations(FileStatus file, long start,
      long len) throws IOException {

    if (file == null)
      return null;

    /* JNI needs the full path */
    String path = file.getPath().toUri().getRawPath();

    /* Look up block location in JNI provided method */
    return this.getFileBlockLocations(file, path, start, len);
  }
  
  private native BlockLocation[] getFileBlockLocations(FileStatus file,
      String path, long start, long len) throws IOException;

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    super.initialize(uri, conf);
    setConf(conf);
  }

  @Override
  public URI getUri() {
    return uri;
  }

  private native static void initIDs(); 
}
