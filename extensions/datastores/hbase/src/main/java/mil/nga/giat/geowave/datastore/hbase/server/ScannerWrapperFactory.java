package mil.nga.giat.geowave.datastore.hbase.server;

import java.util.Collection;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;

public interface ScannerWrapperFactory<T extends InternalScanner>
{
	public T createScannerWrapper(
			Collection<HBaseServerOp> orderedServerOps,
			T delegate,
			Scan scan );

	public static class RegionScannerWrapperFactory implements
			ScannerWrapperFactory<RegionScanner>
	{

		@Override
		public RegionScanner createScannerWrapper(
				final Collection<HBaseServerOp> orderedServerOps,
				final RegionScanner delegate,
				final Scan scan ) {
			return new ServerOpRegionScannerWrapper(
					orderedServerOps,
					delegate,
					scan);
		}
	}

	public static class InternalScannerWrapperFactory implements
			ScannerWrapperFactory<InternalScanner>
	{

		@Override
		public InternalScanner createScannerWrapper(
				final Collection<HBaseServerOp> orderedServerOps,
				final InternalScanner delegate,
				final Scan scan ) {
			return new ServerOpInternalScannerWrapper(
					orderedServerOps,
					delegate,
					scan);
		}
	}

}
