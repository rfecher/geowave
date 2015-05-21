package mil.nga.giat.geowave.format.landsat8;

import java.awt.image.DataBuffer;
import java.awt.image.MultiPixelPackedSampleModel;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.awt.image.WritableRaster;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mil.nga.giat.geowave.adapter.raster.adapter.RasterDataAdapter;
import mil.nga.giat.geowave.adapter.raster.adapter.merge.nodata.NoDataMergeStrategy;
import mil.nga.giat.geowave.core.geotime.IndexType;
import mil.nga.giat.geowave.core.geotime.index.dimension.LatitudeDefinition;
import mil.nga.giat.geowave.core.geotime.index.dimension.LongitudeDefinition;
import mil.nga.giat.geowave.core.geotime.index.dimension.TimeDefinition;
import mil.nga.giat.geowave.core.geotime.store.dimension.LatitudeField;
import mil.nga.giat.geowave.core.geotime.store.dimension.LongitudeField;
import mil.nga.giat.geowave.core.geotime.store.dimension.TimeField;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.sfc.SFCFactory.SFCType;
import mil.nga.giat.geowave.core.index.sfc.tiered.TieredSFCIndexFactory;
import mil.nga.giat.geowave.core.ingest.AccumuloCommandLineOptions;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.dimension.DimensionField;
import mil.nga.giat.geowave.core.store.index.BasicIndexModel;
import mil.nga.giat.geowave.core.store.index.CustomIdIndex;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.format.landsat8.index.Landsat8TemporalBinningStrategy;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridCoverageFactory;
import org.geotools.gce.geotiff.GeoTiffReader;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ui.freemarker.FreeMarkerTemplateUtils;

import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;

public class Landsat8LocalIngestCLIDriver extends
		Landsat8DownloadCLIDriver
{
	private final static Logger LOGGER = LoggerFactory.getLogger(Landsat8LocalIngestCLIDriver.class);
	private static final int LONGITUDE_BITS = 27;
	private static final int LATITUDE_BITS = 27;
	private static final int TIME_BITS = 8;

	protected Landsat8IngestCommandLineOptions ingestOptions;
	protected AccumuloCommandLineOptions accumuloOptions;
	List<SimpleFeature> lastSceneBands = new ArrayList<SimpleFeature>();
	private Template coverageNameTemplate;
	private IndexWriter writer;
	private final Map<String, RasterDataAdapter> adapterCache = new HashMap<String, RasterDataAdapter>();

	public Landsat8LocalIngestCLIDriver(
			final String operation ) {
		super(
				operation);
	}

	@Override
	protected void runInternal(
			final String[] args ) {
		try {
			final DataStore geowaveDataStore = new AccumuloDataStore(
					accumuloOptions.getAccumuloOperations());
			final TimeDefinition timeDefinition = new TimeDefinition(
					new Landsat8TemporalBinningStrategy());
			final Index index = accumuloOptions.getIndex(new Index[] {
				IndexType.SPATIAL_RASTER.createDefaultIndex(),
				new CustomIdIndex(
						TieredSFCIndexFactory.createEqualIntervalPrecisionTieredStrategy(
								new NumericDimensionDefinition[] {
									new LatitudeDefinition(),
									new LongitudeDefinition(),
									timeDefinition
								},
								new int[] {
									LONGITUDE_BITS,
									LATITUDE_BITS,
									TIME_BITS
								},
								SFCType.HILBERT),
						new BasicIndexModel(
								new DimensionField[] {
									new LongitudeField(),
									new LatitudeField(),
									new TimeField(
											timeDefinition)
								}),
						new ByteArrayId(
								"SpatialTemporalLandsat8Index"))
			});
			writer = geowaveDataStore.createIndexWriter(index);

			super.runInternal(args);
		}
		catch (AccumuloException | AccumuloSecurityException e) {
			LOGGER.error(
					"Unable to connect to Accumulo",
					e);
		}
		finally {
			if (writer != null) {
				try {
					writer.close();
				}
				catch (final IOException e) {
					LOGGER.error(
							"Unable to close Accumulo writer",
							e);
				}
			}
		}
	}

	@Override
	protected void nextBand(
			final SimpleFeature band,
			final AnalysisInfo analysisInfo ) {
		super.nextBand(
				band,
				analysisInfo);
		// if (ingestOptions.isCoveragePerBand()) {
		// ingest this band
		// convert the simplefeature into a map to resolve the coverage name
		// using a user supplied freemarker template
		final Map<String, Object> model = new HashMap<String, Object>();
		final SimpleFeatureType type = band.getFeatureType();
		for (final AttributeDescriptor attr : type.getAttributeDescriptors()) {
			final String attrName = attr.getLocalName();
			final Object attrValue = band.getAttribute(attrName);
			if (attrValue != null) {
				model.put(
						attrName,
						attrValue);
			}
		}
		try {
			final String coverageName = FreeMarkerTemplateUtils.processTemplateIntoString(
					coverageNameTemplate,
					model);
			final File geotiffFile = Landsat8DownloadCLIDriver.getDownloadFile(
					band,
					landsatOptions.getWorkspaceDir());
			final GeoTiffReader reader = new GeoTiffReader(
					geotiffFile);
			final GridCoverage2D coverage = reader.read(null);
			RasterDataAdapter adapter = adapterCache.get(coverageName);
			next
			if (adapter == null) {
				final Map<String, String> metadata = new HashMap<String, String>();
				final String[] mdNames = reader.getMetadataNames(coverage.getName().toString());
				if ((mdNames != null) && (mdNames.length > 0)) {
					for (final String mdName : mdNames) {
						metadata.put(
								mdName,
								reader.getMetadataValue(
										coverageName,
										mdName));
					}
				}
				adapter = new RasterDataAdapter(
						coverageName,
						metadata,
						nextCov,
						ingestOptions.getTileSize(),
						ingestOptions.isCreatePyramid(),
						ingestOptions.isCreateHistogram(),
						new double[][] {
							new double[] {
								0
							}
						},
						new NoDataMergeStrategy());
				adapterCache.put(
						coverageName,
						adapter);
			}
			writer.write(
					adapter,
					nextCov);
		}
		catch (IOException | TemplateException e) {
			LOGGER.error(
					"Unable to ingest band " + band.getID() + " because coverage name cannot be resolved from template",
					e);
		}
		// }
		// else {
		// lastSceneBands.add(band);
		// }
	}

	@Override
	protected void lastSceneComplete(
			final AnalysisInfo analysisInfo ) {
		super.lastSceneComplete(analysisInfo);
		processLastScene();
	}

	@Override
	protected void nextScene(
			final SimpleFeature firstBandOfScene,
			final AnalysisInfo analysisInfo ) {
		processLastScene();
		super.nextScene(
				firstBandOfScene,
				analysisInfo);
	}

	protected void processLastScene() {
		// if (!ingestOptions.isCoveragePerBand()) {
		// if (!lastSceneBands.isEmpty()) {
		// TODO ingest as single image for all bands
		// do we need to capture all possible permutations of bands (ie.
		// this scene may not contain all bands for this coverage)?

		// also we need to consider supersampling if the panchromatic
		// band (B8) is provided, because it will be twice the
		// resolution of the other bands

		// }
		// lastSceneBands.clear();
		// }
	}

	@Override
	protected void parseOptionsInternal(
			final CommandLine commandLine )
			throws ParseException {
		super.parseOptionsInternal(commandLine);
		accumuloOptions = AccumuloCommandLineOptions.parseOptions(commandLine);
		ingestOptions = Landsat8IngestCommandLineOptions.parseOptions(commandLine);
		try {
			coverageNameTemplate = new Template(
					"user defined template",
					new StringReader(
							ingestOptions.getCoverageName()),
					new Configuration());
		}
		catch (final IOException e) {
			LOGGER.error(
					"Unable to read freemarker template for coverage name",
					e);
			throw new ParseException(
					e.getMessage());
		}
	}

	@Override
	protected void applyOptionsInternal(
			final Options allOptions ) {
		super.applyOptionsInternal(allOptions);
		AccumuloCommandLineOptions.applyOptions(allOptions);
		Landsat8IngestCommandLineOptions.applyOptions(allOptions);
	}
}
