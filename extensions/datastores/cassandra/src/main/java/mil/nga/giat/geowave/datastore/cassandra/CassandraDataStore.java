package mil.nga.giat.geowave.datastore.cassandra;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataStore;
import mil.nga.giat.geowave.core.store.metadata.AdapterIndexMappingStoreImpl;
import mil.nga.giat.geowave.core.store.metadata.AdapterStoreImpl;
import mil.nga.giat.geowave.core.store.metadata.DataStatisticsStoreImpl;
import mil.nga.giat.geowave.core.store.metadata.IndexStoreImpl;
import mil.nga.giat.geowave.core.store.metadata.SecondaryIndexStoreImpl;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.datastore.cassandra.operations.CassandraOperations;
import mil.nga.giat.geowave.datastore.cassandra.operations.config.CassandraOptions;
import mil.nga.giat.geowave.mapreduce.BaseMapReduceDataStore;
import mil.nga.giat.geowave.mapreduce.output.GeoWaveOutputKey;

public class CassandraDataStore extends
		BaseMapReduceDataStore
{
	public CassandraDataStore(
			final CassandraOperations operations,
			final CassandraOptions options ) {
		this(
				new IndexStoreImpl(
						operations,
						options),
				new AdapterStoreImpl(
						operations,
						options),
				new DataStatisticsStoreImpl(
						operations,
						options),
				new AdapterIndexMappingStoreImpl(
						operations,
						options),
				new SecondaryIndexStoreImpl(),
				operations,
				options);
	}

	public CassandraDataStore(
			final IndexStore indexStore,
			final AdapterStore adapterStore,
			final DataStatisticsStore statisticsStore,
			final AdapterIndexMappingStore indexMappingStore,
			final SecondaryIndexDataStore secondaryIndexDataStore,
			final CassandraOperations operations,
			final CassandraOptions options ) {
		super(
				indexStore,
				adapterStore,
				statisticsStore,
				indexMappingStore,
				secondaryIndexDataStore,
				operations,
				options);

		secondaryIndexDataStore
				.setDataStore(
						this);
	}

	@Override
	public void prepareRecordWriter(
			Configuration conf ) {
		// because datastax cassandra driver requires guava 19.0, this user
		// classpath must override the default hadoop classpath which has an old
		// version of guava or there will be incompatibility issues
		conf
				.setBoolean(
						MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST,
						true);
	}

	@Override
	public List<InputSplit> getSplits(
			DistributableQuery query,
			QueryOptions queryOptions,
			AdapterStore adapterStore,
			AdapterIndexMappingStore aimStore,
			DataStatisticsStore statsStore,
			IndexStore indexStore,
			JobContext context,
			Integer minSplits,
			Integer maxSplits )
			throws IOException,
			InterruptedException {
		context
				.getConfiguration()
				.setBoolean(
						MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST,
						true);
		return super.getSplits(
				query,
				queryOptions,
				adapterStore,
				aimStore,
				statsStore,
				indexStore,
				context,
				minSplits,
				maxSplits);
	}
}
