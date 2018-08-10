package mil.nga.giat.geowave.test;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;

import mil.nga.giat.geowave.core.store.util.ClasspathUtils;

/**
 * A parent-last classloader that will try the child classloader first and then
 * the parent.
 */
public class DirectoryBasedParentLastClassLoader extends
		ClassLoader
{
	private ChildURLClassLoader childClassLoader;

	/**
	 * This class delegates (child then parent) for the findClass method for a
	 * URLClassLoader. Need this because findClass is protected in
	 * URLClassLoader
	 */
	private class ChildURLClassLoader extends
			URLClassLoader
	{
		private ClassLoader realParent;

		public ChildURLClassLoader(
				URL[] urls,
				ClassLoader realParent ) {
			// pass null as parent so upward delegation disabled for first
			// findClass call
			super(
					urls,
					null);

			this.realParent = realParent;
		}

		@Override
		public Class<?> findClass(
				String name )
				throws ClassNotFoundException {
			// if (name.startsWith("org.apache.hadoop.fs") ||
			// name.contains("DistributedFileSystem")
			// || name.startsWith("org.apache.hadoop.conf") ||
			// name.startsWith("org.apache.hadoop.security") ||
			// name.startsWith("org.apache.hadoop.hbase.http")||name.startsWith("org.apache.hadoop.metrics2")||name.startsWith("org.apache.hadoop.hbase.metrics"))
			// {
			// System.err.println("child delegate " + name);
			//
			// return realParent.loadClass(name);
			// }

			if (name.contains("HashFunction")) {
				System.err.println("find class " + name);
			}
			try {
				// first try to use the URLClassLoader findClass
//				if (findLoadedClass(name) != null
//						&& (!name.startsWith("org.apache.hadoop.hbase") || (name.contains("CoordinatedStateManager")))) {
//					return findLoadedClass(name);
//				}
				return super.findClass(name);
			}
			catch (ClassNotFoundException e) {
				// if that fails, ask real parent classloader to load the
				// class (give up)
				if (name.contains("HashFunction")) {
					System.err.println("parent find class " + name);
				}
				return realParent.loadClass(name);
			}
		}

		@Override
		public URL findResource(
				String name ) {try {
					// first try to use the URLClassLoader findClass
					return super.findResource(name);
				}
				catch (Exception e) {
					// if that fails, ask real parent classloader to load the
					// class (give up)
					if (name.contains("HashFunction")) {
						System.err.println("parent find class " + name);
					}
					return realParent.getResource(name);
				}
		}
	}

	public DirectoryBasedParentLastClassLoader(
			String jarDir ) {
		super(
				Thread.currentThread().getContextClassLoader());

		// search for JAR files in the given directory
//		FileFilter jarFilter = new FileFilter() {
//			public boolean accept(
//					File pathname ) {
//				return pathname.getName().endsWith(
//						".jar");
//			}
//		};
//
//		// create URL for each JAR file found
//		File[] jarFiles = new File(
//				jarDir).listFiles(jarFilter);
//		URL[] urls;
//
//		if (null != jarFiles) {
//			urls = new URL[jarFiles.length];
//
//			for (int i = 0; i < jarFiles.length; i++) {
//				try {
//					urls[i] = jarFiles[i].toURI().toURL();
//				}
//				catch (MalformedURLException e) {
//					throw new RuntimeException(
//							"Could not get URL for JAR file: " + jarFiles[i],
//							e);
//				}
//			}
//
//		}
//		else {
//			// no JAR files found
//			urls = new URL[0];
//		}
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
			urls = new URL[jarFiles.length + 1];

			for (int i = 0; i < jarFiles.length; i++) {
				try {
					urls[i] = jarFiles[i].toURI().toURL();
				}
				catch (MalformedURLException e) {
					throw new RuntimeException(
							"Could not get URL for JAR file: " + jarFiles[i],
							e);
				}
			}

			try {
				final String jarPath = ClasspathUtils.setupPathingJarClassPath(
						new File("target/hbase/lib3"),
						HBaseClassloader.class);
				urls[urls.length -1 ] = new File(jarPath).toURI().toURL();
			}
			catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			childClassLoader = new ChildURLClassLoader(
					urls,
					this.getParent());
		}
	}

	@Override
	protected synchronized Class<?> loadClass(
			String name,
			boolean resolve )
			throws ClassNotFoundException {
		if (name.contains("HashFunction")) {
			System.err.println(name);
		}
		// else if (name.contains("HMaster")) {
		// System.err.println(name);
		// }
		// else if (name.contains("MetricsAssertHelper")) {
		// System.err.println(name);
		// }
		// if (name.startsWith("org.apache.hadoop.fs") ||
		// name.contains("DistributedFileSystem")
		// || name.startsWith("org.apache.hadoop.conf") ||
		// name.startsWith("org.apache.hadoop.security") ||
		// name.startsWith("org.apache.hadoop.hbase.http")||name.startsWith("org.apache.hadoop.metrics2")||name.startsWith("org.apache.hadoop.hbase.metrics"))
		// {
		// System.err.println("delegate " + name);
		// return super.loadClass(
		// name,
		// resolve);
		// }
		try {
			// first try to find a class inside the child classloader
			return childClassLoader.findClass(name);
		}
		catch (ClassNotFoundException e) {
			// didn't find it, try the parent
			if (name.contains("HashFunction")) {
				System.err.println("parent " + name);
			}
			return super.loadClass(
					name,
					resolve);
		}
	}

	@Override
	public URL getResource(
			String name ) {try {
				// first try to find a class inside the child classloader
				return childClassLoader.findResource(name);
			}
			catch (Exception e) {
				return super.getResource(
						name);
			}
	}
}
