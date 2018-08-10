package mil.nga.giat.geowave.test;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

public class HBaseClassloader2 extends
		URLClassLoader
{


	  /**
	   * Parent class loader.
	   */
	  protected final ClassLoader parent;
	/**
	 * Creates a JarClassLoader that loads classes from the given paths.
	 */
	public HBaseClassloader2(
			ClassLoader parent ) {
		super(new URL[]{},
				parent);
		this.parent = parent;
	}

	  @Override
	  public Class<?> loadClass(String name)
	      throws ClassNotFoundException {
//	    try {
	      return parent.loadClass(name);
//	    } catch (ClassNotFoundException e) {
//
//	        return tryRefreshClass(name);
//	    }
	  }


//	  private Class<?> tryRefreshClass(String name)
//	      throws ClassNotFoundException {
//	    synchronized (getClassLoadingLock(name)) {
//	        // Check whether the class has already been loaded:
//	        Class<?> clasz = findLoadedClass(name);
//	        if (clasz != null) {
//	          if (LOG.isDebugEnabled()) {
//	            LOG.debug("Class " + name + " already loaded");
//	          }
//	        }
//	        else {
//	          try {
//	            if (LOG.isDebugEnabled()) {
//	              LOG.debug("Finding class: " + name);
//	            }
//	            clasz = findClass(name);
//	          } catch (ClassNotFoundException cnfe) {
//	            // Load new jar files if any
//	            if (LOG.isDebugEnabled()) {
//	              LOG.debug("Loading new jar files, if any");
//	            }
//	            loadNewJars();
//
//	            if (LOG.isDebugEnabled()) {
//	              LOG.debug("Finding class again: " + name);
//	            }
//	            clasz = findClass(name);
//	          }
//	        }
//	        return clasz;
//	      }
//	  }
//
//	  private synchronized void loadNewJars() {
//	    // Refresh local jar file lists
//	    File[] files = localDir == null ? null : localDir.listFiles();
//	    if (files != null) {
//	      for (File file : files) {
//	        String fileName = file.getName();
//	        if (jarModifiedTime.containsKey(fileName)) {
//	          continue;
//	        }
//	        if (file.isFile() && fileName.endsWith(".jar")) {
//	          jarModifiedTime.put(fileName, Long.valueOf(file.lastModified()));
//	          try {
//	            URL url = file.toURI().toURL();
//	            addURL(url);
//	          } catch (MalformedURLException mue) {
//	            // This should not happen, just log it
//	            LOG.warn("Failed to load new jar " + fileName, mue);
//	          }
//	        }
//	      }
//	    }
//
//	    // Check remote files
//	    FileStatus[] statuses = null;
//	    if (remoteDir != null) {
//	      try {
//	        statuses = remoteDirFs.listStatus(remoteDir);
//	      } catch (IOException ioe) {
//	        LOG.warn("Failed to check remote dir status " + remoteDir, ioe);
//	      }
//	    }
//	    if (statuses == null || statuses.length == 0) {
//	      return; // no remote files at all
//	    }
//
//	    for (FileStatus status: statuses) {
//	      if (status.isDirectory()) continue; // No recursive lookup
//	      Path path = status.getPath();
//	      String fileName = path.getName();
//	      if (!fileName.endsWith(".jar")) {
//	        if (LOG.isDebugEnabled()) {
//	          LOG.debug("Ignored non-jar file " + fileName);
//	        }
//	        continue; // Ignore non-jar files
//	      }
//	      Long cachedLastModificationTime = jarModifiedTime.get(fileName);
//	      if (cachedLastModificationTime != null) {
//	        long lastModified = status.getModificationTime();
//	        if (lastModified < cachedLastModificationTime.longValue()) {
//	          // There could be some race, for example, someone uploads
//	          // a new one right in the middle the old one is copied to
//	          // local. We can check the size as well. But it is still
//	          // not guaranteed. This should be rare. Most likely,
//	          // we already have the latest one.
//	          // If you are unlucky to hit this race issue, you have
//	          // to touch the remote jar to update its last modified time
//	          continue;
//	        }
//	      }
//	      try {
//	        // Copy it to local
//	        File dst = new File(localDir, fileName);
//	        remoteDirFs.copyToLocalFile(path, new Path(dst.getPath()));
//	        jarModifiedTime.put(fileName, Long.valueOf(dst.lastModified()));
//	        URL url = dst.toURI().toURL();
//	        addURL(url);
//	      } catch (IOException ioe) {
//	        LOG.warn("Failed to load new jar " + fileName, ioe);
//	      }
//	    }
//	  }
}
