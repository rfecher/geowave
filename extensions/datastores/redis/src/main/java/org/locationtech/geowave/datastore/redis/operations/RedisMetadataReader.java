package org.locationtech.geowave.datastore.redis.operations;

import java.util.Arrays;
import java.util.Iterator;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.locationtech.geowave.core.store.operations.MetadataQuery;
import org.locationtech.geowave.core.store.operations.MetadataReader;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.locationtech.geowave.core.store.util.StatisticsRowIterator;
import org.locationtech.geowave.datastore.redis.util.RedisUtils;
import org.redisson.api.RScoredSortedSet;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;

public class RedisMetadataReader implements
		MetadataReader
{
	private final RScoredSortedSet<GeoWaveMetadata> set;
	private final MetadataType metadataType;

	public RedisMetadataReader(
			final RScoredSortedSet<GeoWaveMetadata> set,
			final MetadataType metadataType ) {
		this.set = set;
		this.metadataType = metadataType;
	}

	@Override
	public CloseableIterator<GeoWaveMetadata> query(
			final MetadataQuery query ) {
		Iterator<GeoWaveMetadata> results;
		if (query.getPrimaryId() != null) {
			if (query.getPrimaryId().length > 6) {
				// this primary ID and next prefix are going to be the same
				// score
				final double score = RedisUtils
						.getScore(
								query.getPrimaryId());
				results = set
						.valueRange(
								score,
								true,
								score,
								true)
						.iterator();
			}
			else {
				// the primary ID prefix is short enough that we can use the
				// score of the next prefix to subset the data
				results = set
						.valueRange(
								RedisUtils
										.getScore(
												query.getPrimaryId()),
								true,
								RedisUtils
										.getScore(
												ByteArray
														.getNextPrefix(
																query.getPrimaryId())),
								false)
						.iterator();
			}
		}
		else {
			results = set.iterator();
		}
		if (query.hasPrimaryId() || query.hasSecondaryId()) {
			results = Iterators
					.filter(
							results,
							new Predicate<GeoWaveMetadata>() {

								@Override
								public boolean apply(
										@Nullable
								final GeoWaveMetadata input ) {
									if (query.hasPrimaryId() && !startsWith(
											input.getPrimaryId(),
											query.getPrimaryId())) {
										return false;
									}
									if (query.hasSecondaryId() && !Arrays
											.equals(
													input.getSecondaryId(),
													query.getSecondaryId())) {
										return false;
									}
									return true;
								}
							});
		}
		final CloseableIterator<GeoWaveMetadata> retVal = new CloseableIterator.Wrapper<>(
				results);
		return MetadataType.STATS
				.equals(
						metadataType)
								? new StatisticsRowIterator(
										retVal,
										query.getAuthorizations())
								: retVal;
	}

	public static boolean startsWith(
			final byte[] source,
			final byte[] match ) {

		if (match.length > (source.length)) {
			return false;
		}

		for (int i = 0; i < match.length; i++) {
			if (source[i] != match[i]) {
				return false;
			}
		}
		return true;
	}
}
