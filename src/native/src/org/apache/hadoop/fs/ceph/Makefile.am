vpath %.c $(HADOOP_NATIVE_SRCDIR)/$(subdir)

AM_CPPFLAGS = @JNI_CPPFLAGS@ -I$(HADOOP_NATIVE_SRCDIR)/src -I@CEPH_SRCDIR_PATH@/src
AM_LDFLAGS = @JNI_LDFLAGS@
AM_CFLAGS = -g -Wall -fPIC -O2 -m$(JVM_DATA_MODEL)

noinst_LTLIBRARIES = libnativeceph.la
libnativeceph_la_SOURCES = CephLocalityFileSystem.c 
libnativeceph_la_LIBADD = -ldl -ljvm
