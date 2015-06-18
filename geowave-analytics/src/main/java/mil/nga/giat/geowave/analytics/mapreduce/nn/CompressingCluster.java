package mil.nga.giat.geowave.analytics.mapreduce.nn;

public interface CompressingCluster<INTYPE, OUTTYPE> extends
		Cluster<INTYPE>
{
	public OUTTYPE get();
}
