package mil.nga.giat.geowave.test;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.regex.Pattern;

import mil.nga.giat.geowave.core.store.util.ClasspathUtils;

public class HBaseClassloader extends
		URLClassLoader
{

	/**
	 * If the class being loaded starts with any of these strings, we will skip
	 * trying to load it from the coprocessor jar and instead delegate directly
	 * to the parent ClassLoader.
	 */
	private static final String[] CLASS_PREFIX_EXEMPTIONS = new String[] {
		// Java standard library:
		"com.sun.",
		"java.",
		"javax.",
		"org.ietf",
		"org.omg",
		"org.w3c",
		"org.xml",
		"sunw.",
		// logging
		"org.apache.commons.logging",
		"org.apache.log4j",
		"com.hadoop",
		// Hadoop/HBase/ZK:
		"org.apache.hadoop.security",
		// "org.apache.hadoop.HadoopIllegalArgumentException",
		"org.apache.hadoop.conf",
		"org.apache.hadoop.fs",
//		"org.apache.hadoop.hbase.test.MetricsAssertHelper",
//		"org.apache.hadoop.hbase.ipc.MetricsHBaseServerSourceFactory",
//		"org.apache.hadoop.hbase.ipc.MetricsHBaseServer",
//		"org.apache.hadoop.hbase.ipc.RpcServer",
//		"org.apache.xerces",
//		"org.apache.hadoop.hbase.http",
//		"org.mortbay.jetty",
//		"org.apache.jasper",
//		"org.apache.hadoop.hbase.regionserver.RegionServerAbortedException",
		// "org.apache.hadoop.http",
		// "org.apache.hadoop.io",
		// "org.apache.hadoop.ipc",
		// "org.apache.hadoop.metrics",
		// "org.apache.hadoop.metrics2",
		// "org.apache.hadoop.net",
		"org.apache.hadoop.util",
	// "org.apache.hadoop.hdfs",
	// "org.apache.hadoop.hbase",
	// "org.apache.zookeeper",
	};
	/**
	 * If the resource being loaded matches any of these patterns, we will first
	 * attempt to load the resource with the parent ClassLoader. Only if the
	 * resource is not found by the parent do we attempt to load it from the
	 * coprocessor jar.
	 */
	private static final Pattern[] RESOURCE_LOAD_PARENT_FIRST_PATTERNS = new Pattern[] {
		Pattern.compile("^[^-]+-default\\.xml$")
	};

	/**
	 * Creates a JarClassLoader that loads classes from the given paths.
	 */
	public HBaseClassloader(
			ClassLoader parent ) {
		super(
				new URL[] {},
				parent);
		// search for JAR files in the given directory
		FileFilter jarFilter = new FileFilter() {
			public boolean accept(
					File pathname ) {
				return pathname.getName().endsWith(
						".jar");
			}
		};

		// create URL for each JAR file found
		File[] jarFiles = new File(
				"target/hbase/lib").listFiles(jarFilter);
		URL[] urls;

		if (null != jarFiles) {
			urls = new URL[jarFiles.length];

			for (int i = 0; i < jarFiles.length; i++) {
				try {
					addURL(jarFiles[i].toURI().toURL());
				}
				catch (MalformedURLException e) {
					throw new RuntimeException(
							"Could not get URL for JAR file: " + jarFiles[i],
							e);
				}
			}

		}
		try {
			final String jarPath = ClasspathUtils.setupPathingJarClassPath(
					new File("target/hbase/lib3"),
					HBaseClassloader.class);
			addURL(new File(jarPath).toURI().toURL());
		}
		catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}

	@Override
	public Class<?> loadClass(
			String name )
			throws ClassNotFoundException {
		if (isClassExempt(
				name,
				null)) {
			return getParent().loadClass(
					name);
		}
		synchronized (getClassLoadingLock(name)) {
			// Check whether the class has already been loaded:
			Class<?> clasz = findLoadedClass(name);
			if (clasz != null) {
				return clasz;
			}
			try {
				// Try to find this class using the URLs passed to this
				// ClassLoader
				clasz = findClass(name);
			}
			catch (ClassNotFoundException e) {
				// Class not found using this ClassLoader, so delegate to parent
				try {
					clasz = getParent().loadClass(
							name);
				}
				catch (ClassNotFoundException e2) {
					// Class not found in this ClassLoader or in the parent
					// ClassLoader
					// Log some debug output before re-throwing
					// ClassNotFoundException
					throw e2;
				}
			}
			return clasz;
		}
	}

	@Override
	public URL getResource(
			String name ) {
		URL resource = null;
		boolean parentLoaded = false;

		// Delegate to the parent first if necessary
		if (loadResourceUsingParentFirst(name)) {
			resource = super.getResource(name);
			parentLoaded = true;
		}
		synchronized (getClassLoadingLock(name)) {
			// Try to find the resource in this jar
			resource = findResource(name);
			if ((resource == null) && !parentLoaded) {
				// Not found in this jar and we haven't attempted to load
				// the resource in the parent yet; fall back to the parent
				resource = super.getResource(name);
			}

		}
		return resource;
	}

	/**
	 * Determines whether the given class should be exempt from being loaded by
	 * this ClassLoader.
	 * 
	 * @param name
	 *            the name of the class to test.
	 * @return true if the class should *not* be loaded by this ClassLoader;
	 *         false otherwise.
	 */
	protected boolean isClassExempt(
			String name,
			String[] includedClassPrefixes ) {
		if (includedClassPrefixes != null) {
			for (String clsName : includedClassPrefixes) {
				if (name.startsWith(clsName)) {
					return false;
				}
			}
		}
		for (String exemptPrefix : CLASS_PREFIX_EXEMPTIONS) {
			if (name.startsWith(exemptPrefix)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Determines whether we should attempt to load the given resource using the
	 * parent first before attempting to load the resource using this
	 * ClassLoader.
	 * 
	 * @param name
	 *            the name of the resource to test.
	 * @return true if we should attempt to load the resource using the parent
	 *         first; false if we should attempt to load the resource using this
	 *         ClassLoader first.
	 */
	protected boolean loadResourceUsingParentFirst(
			String name ) {
		for (Pattern resourcePattern : RESOURCE_LOAD_PARENT_FIRST_PATTERNS) {
			if (resourcePattern.matcher(
					name).matches()) {
				return true;
			}
		}
		return false;
	}
}
