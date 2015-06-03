package mil.nga.giat.geowave.analytics.clustering.runners;

import java.util.Set;

import mil.nga.giat.geowave.accumulo.mapreduce.input.GeoWaveInputKey;
import mil.nga.giat.geowave.accumulo.mapreduce.output.GeoWaveOutputKey;
import mil.nga.giat.geowave.analytics.clustering.mapreduce.InputToOutputKeyReducer;
import mil.nga.giat.geowave.analytics.parameters.CentroidParameters;
import mil.nga.giat.geowave.analytics.parameters.MapReduceParameters;
import mil.nga.giat.geowave.analytics.parameters.OutputParameters;
import mil.nga.giat.geowave.analytics.parameters.ParameterEnum;
import mil.nga.giat.geowave.analytics.tools.IndependentJobRunner;
import mil.nga.giat.geowave.analytics.tools.PropertyManagement;
import mil.nga.giat.geowave.analytics.tools.RunnerUtils;
import mil.nga.giat.geowave.analytics.tools.mapreduce.GeoWaveAnalyticJobRunner;
import mil.nga.giat.geowave.analytics.tools.mapreduce.GeoWaveInputFormatConfiguration;
import mil.nga.giat.geowave.analytics.tools.mapreduce.MapReduceJobRunner;
import mil.nga.giat.geowave.analytics.tools.mapreduce.SequenceFileOutputFormatConfiguration;

import org.apache.commons.cli.Option;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * Run a map reduce job to extract a population of data from GeoWave (Accumulo),
 * remove duplicates, and output a SimpleFeature with the ID and the extracted
 * geometry from each of the GeoWave data item.
 * 
 */
public class GeoWaveInputLoadJobRunner extends
		GeoWaveAnalyticJobRunner implements
		MapReduceJobRunner,
		IndependentJobRunner
{
	public GeoWaveInputLoadJobRunner() {
		// defaults
		super.setInputFormatConfiguration(new GeoWaveInputFormatConfiguration());
		super.setOutputFormatConfiguration(new SequenceFileOutputFormatConfiguration());
	}

	@Override
	public void configure(
			final Job job )
			throws Exception {

		job.setMapperClass(Mapper.class);
		job.setReducerClass(InputToOutputKeyReducer.class);
		job.setMapOutputKeyClass(GeoWaveInputKey.class);
		job.setMapOutputValueClass(ObjectWritable.class);
		job.setOutputKeyClass(GeoWaveOutputKey.class);
		job.setOutputValueClass(Object.class);
		job.setSpeculativeExecution(false);

		job.setJobName("GeoWave Input to Output (" + namespace + ")");
		job.setReduceSpeculativeExecution(false);

	}

	@Override
	public Class<?> getScope() {
		return InputToOutputKeyReducer.class;
	}

	@Override
	public int run(
			final Configuration config,
			final PropertyManagement runTimeProperties )
			throws Exception {

		RunnerUtils.setParameter(
				config,
				getScope(),
				new Object[] {
					checkIndex(
							runTimeProperties,
							OutputParameters.Output.INDEX_ID,
							runTimeProperties.getPropertyAsString(
									CentroidParameters.Centroid.INDEX_ID,
									"hull_idx")),
				},
				new ParameterEnum[] {
					OutputParameters.Output.INDEX_ID
				});

		addDataAdapter(
				config,
				getAdapter(
						runTimeProperties,
						OutputParameters.Output.DATA_TYPE_ID,
						OutputParameters.Output.DATA_NAMESPACE_URI));

		RunnerUtils.setParameter(
				config,
				getScope(),
				runTimeProperties,
				new ParameterEnum[] {
					OutputParameters.Output.DATA_TYPE_ID,
					OutputParameters.Output.DATA_NAMESPACE_URI,
					OutputParameters.Output.INDEX_ID
				});

		return super.run(
				config,
				runTimeProperties);
	}

	@Override
	public void fillOptions(
			final Set<Option> options ) {

		OutputParameters.fillOptions(
				options,
				new OutputParameters.Output[] {
					OutputParameters.Output.INDEX_ID,
					OutputParameters.Output.DATA_TYPE_ID,
					OutputParameters.Output.DATA_NAMESPACE_URI
				});

		MapReduceParameters.fillOptions(options);

		super.fillOptions(options);

	}

}