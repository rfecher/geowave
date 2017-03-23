package mil.nga.giat.geowave.test;

import java.io.IOException;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.statistics.AbstractDataStatistics;
import mil.nga.giat.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount;
import mil.nga.giat.geowave.core.store.index.IndexMetaDataSet;
import mil.nga.giat.geowave.datastore.hbase.metadata.HBaseDataStatisticsStore;
import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;

public class RenameStat
{
	public static void main(
			String[] args )
			throws IOException {
		HBaseDataStatisticsStore store = new HBaseDataStatisticsStore(
				new BasicHBaseOperations(
						args[0],
						args[1]));
		AbstractDataStatistics<?> s = (AbstractDataStatistics<?>) store.getDataStatistics(
				new ByteArrayId(
						args[2]),
				DifferingFieldVisibilityEntryCount.composeId(TestUtils.DEFAULT_SPATIAL_INDEX.getId()));
		s.setStatisticsId(DifferingFieldVisibilityEntryCount.composeIdOldWay(TestUtils.DEFAULT_SPATIAL_INDEX.getId()));
		store.incorporateStatistics(s);
		AbstractDataStatistics<?> s2 = (AbstractDataStatistics<?>) store.getDataStatistics(
				new ByteArrayId(
						args[2]),
				IndexMetaDataSet.composeId(TestUtils.DEFAULT_SPATIAL_INDEX.getId()));
		s2.setStatisticsId(IndexMetaDataSet.composeIdOldWay(TestUtils.DEFAULT_SPATIAL_INDEX.getId()));
		store.incorporateStatistics(s2);
	}
}
