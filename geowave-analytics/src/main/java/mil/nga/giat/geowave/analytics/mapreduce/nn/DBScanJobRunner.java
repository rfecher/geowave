package mil.nga.giat.geowave.analytics.mapreduce.nn;

import java.util.Set;

import mil.nga.giat.geowave.accumulo.mapreduce.JobContextAdapterStore;
import mil.nga.giat.geowave.accumulo.mapreduce.input.GeoWaveInputKey;
import mil.nga.giat.geowave.analytics.clustering.ClusteringUtils;
import mil.nga.giat.geowave.analytics.mapreduce.nn.NNMapReduce.PartitionDataWritable;
import mil.nga.giat.geowave.analytics.parameters.ClusteringParameters;
import mil.nga.giat.geowave.analytics.parameters.ClusteringParameters.Clustering;
import mil.nga.giat.geowave.analytics.parameters.GlobalParameters;
import mil.nga.giat.geowave.analytics.parameters.GlobalParameters.Global;
import mil.nga.giat.geowave.analytics.parameters.HullParameters;
import mil.nga.giat.geowave.analytics.parameters.HullParameters.Hull;
import mil.nga.giat.geowave.analytics.parameters.ParameterEnum;
import mil.nga.giat.geowave.analytics.parameters.PartitionParameters;
import mil.nga.giat.geowave.analytics.parameters.PartitionParameters.Partition;
import mil.nga.giat.geowave.analytics.tools.AdapterWithObjectWritable;
import mil.nga.giat.geowave.analytics.tools.AnalyticFeature;
import mil.nga.giat.geowave.analytics.tools.Projection;
import mil.nga.giat.geowave.analytics.tools.PropertyManagement;
import mil.nga.giat.geowave.analytics.tools.RunnerUtils;
import mil.nga.giat.geowave.analytics.tools.SimpleFeatureProjection;

import org.apache.commons.cli.Option;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.Job;
import org.geotools.feature.type.BasicFeatureTypes;

/**
 * Run a single DBScan job producing micro clusters over a set of neighbors
 */
public class DBScanJobRunner extends
		NNJobRunner
{

	@Override
	public void configure(
			final Job job )
			throws Exception {
		super.configure(job);
		job.setMapperClass(NNMapReduce.NNMapper.class);
		job.setReducerClass(DBScanMapReduce.DBScanMapHullReducer.class);
		job.setMapOutputKeyClass(PartitionDataWritable.class);
		job.setMapOutputValueClass(AdapterWithObjectWritable.class);
		job.setOutputKeyClass(GeoWaveInputKey.class);
		job.setOutputValueClass(ObjectWritable.class);
		job.setSpeculativeExecution(false);
		job.getConfiguration().set(
				"mapreduce.map.java.opts",
				"-Xmx4096m");
	}

	@Override
	public int run(
			final Configuration config,
			final PropertyManagement runTimeProperties )
			throws Exception {

		runTimeProperties.storeIfEmpty(
				HullParameters.Hull.DATA_TYPE_ID,
				"concave_hull");
		final String adapterID = runTimeProperties.getPropertyAsString(
				HullParameters.Hull.DATA_TYPE_ID,
				"concave_hull");
		final String namespaceURI = runTimeProperties.storeIfEmpty(
				HullParameters.Hull.DATA_NAMESPACE_URI,
				BasicFeatureTypes.DEFAULT_NAMESPACE).toString();

		JobContextAdapterStore.addDataAdapter(
				config,
				AnalyticFeature.createGeometryFeatureAdapter(
						adapterID,
						new String[0],
						namespaceURI,
						ClusteringUtils.CLUSTERING_CRS));

		final Projection<?> projectionFunction = runTimeProperties.getClassInstance(
				HullParameters.Hull.PROJECTION_CLASS,
				Projection.class,
				SimpleFeatureProjection.class);

		projectionFunction.setup(
				runTimeProperties,
				config);

		RunnerUtils.setParameter(
				config,
				getScope(),
				runTimeProperties,
				new ParameterEnum[] {
					HullParameters.Hull.PROJECTION_CLASS,
					GlobalParameters.Global.BATCH_ID,
					HullParameters.Hull.ZOOM_LEVEL,
					HullParameters.Hull.HULL_BUILDER,
					HullParameters.Hull.ITERATION,
					HullParameters.Hull.DATA_TYPE_ID,
					HullParameters.Hull.DATA_NAMESPACE_URI,
					ClusteringParameters.Clustering.MINIMUM_SIZE
				});

		return super.run(
				config,
				runTimeProperties);

	}

	@Override
	public void fillOptions(
			final Set<Option> options ) {
		super.fillOptions(options);

		PartitionParameters.fillOptions(
				options,
				new PartitionParameters.Partition[] {
					Partition.PARTITIONER_CLASS,
					Partition.PARTITION_DISTANCE,
					Partition.MAX_MEMBER_SELECTION
				});

		GlobalParameters.fillOptions(
				options,
				new GlobalParameters.Global[] {
					Global.BATCH_ID
				});

		HullParameters.fillOptions(
				options,
				new HullParameters.Hull[] {
					Hull.HULL_BUILDER,
					Hull.DATA_TYPE_ID,
					Hull.PROJECTION_CLASS
				});

		ClusteringParameters.fillOptions(
				options,
				new ClusteringParameters.Clustering[] {
					Clustering.MINIMUM_SIZE,
					Clustering.GEOMETRIC_DISTANCE_UNIT,
					Clustering.DISTANCE_THRESHOLDS
				});
	}

}
