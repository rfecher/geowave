package org.locationtech.geowave.datastore.redis.util;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Semaphore;

import org.redisson.api.BatchOptions;
import org.redisson.api.RBatch;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RScoredSortedSetAsync;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.Codec;
import org.redisson.client.protocol.ScoredEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisScoredSetWrapper<V> implements
		AutoCloseable
{
	private final static Logger LOGGER = LoggerFactory
			.getLogger(
					RedisScoredSetWrapper.class);
	private static int BATCH_SIZE = 1000;
	private RScoredSortedSetAsync<V> currentSetBatch;
	private RScoredSortedSet<V> currentSet;
	private RBatch currentBatch;
	private final RedissonClient client;
	private final String setName;
	private final Codec codec;
	private int batchCmdCounter = 0;
	private final static int MAX_CONCURRENT_WRITE = 100;
	private final Semaphore writeSemaphore = new Semaphore(
			MAX_CONCURRENT_WRITE);

	public RedisScoredSetWrapper(
			final RedissonClient client,
			final String setName,
			final Codec codec ) {
		this.setName = setName;
		this.client = client;
		this.codec = codec;
	}
	//
	// public void executeAsync() {
	// batch.getScoredSortedSet(s);
	// }
	//
	// @Override
	// public void close() {
	// batch.executeAsync();
	// }

	public boolean remove(
			final Object o ) {
		return getCurrentSet()
				.remove(
						o);
	}

	private RScoredSortedSet<V> getCurrentSet() {
		// avoid synchronization if unnecessary by checking for null outside
		// synchronized block
		if (currentSet == null) {
			synchronized (this) {
				// check again within synchronized block
				if (currentSet == null) {
					currentSet = client
							.getScoredSortedSet(
									setName,
									codec);
				}
			}
		}
		return currentSet;
	}

	private RScoredSortedSetAsync<V> getCurrentBatch() {
		// avoid synchronization if unnecessary by checking for null outside
		// synchronized block
		if (currentSetBatch == null) {
			synchronized (this) {
				// check again within synchronized block
				if (currentSetBatch == null) {
					currentBatch = client
							.createBatch(
									BatchOptions.defaults());
					currentSetBatch = currentBatch
							.getScoredSortedSet(
									setName,
									codec);
				}
			}
		}
		return currentSetBatch;
	}

	public Iterator<ScoredEntry<V>> entryRange(
			final double startScore,
			final boolean startScoreInclusive,
			final double endScore,
			final boolean endScoreInclusive ) {
		final RScoredSortedSet<V> currentSet = getCurrentSet();
		final Collection<ScoredEntry<V>> currentResult = currentSet
				.entryRange(
						startScore,
						startScoreInclusive,
						endScore,
						endScoreInclusive,
						0,
						RedisUtils.MAX_ROWS_FOR_PAGINATION);
		if (currentResult.size() >= RedisUtils.MAX_ROWS_FOR_PAGINATION) {
			return new LazyPaginatedEntryRange<>(
					startScore,
					startScoreInclusive,
					endScore,
					endScoreInclusive,
					currentSet,
					currentResult);
		}
		return currentResult.iterator();
	}

	public void add(
			final double score,
			final V object ) {
		if (++batchCmdCounter > BATCH_SIZE) {
			synchronized (this) {
				// check again inside the synchronized block
				if (batchCmdCounter > BATCH_SIZE) {
					flush();
				}
			}
		}
		getCurrentBatch()
				.addAsync(
						score,
						object);
	}

	public void flush() {
		batchCmdCounter = 0;
		final RBatch flushBatch = this.currentBatch;
		currentSetBatch = null;
		currentBatch = null;
		try {
			writeSemaphore.acquire();
//			System.err.println(MAX_CONCURRENT_WRITE - writeSemaphore.availablePermits());
			flushBatch
					.executeAsync()
					.handle(
							(
									r,
									t ) -> {
								writeSemaphore.release();
								if ((t != null) && !(t instanceof CancellationException)) {
									LOGGER
											.error(
													"Exception in batched write",
													t);
								}
								return r;
							});
		}
		catch (final InterruptedException e) {
			LOGGER
					.warn(
							"async batch write semaphore interrupted",
							e);
			writeSemaphore.release();
		}
	}

	@Override
	public void close()
			throws Exception {
		flush();
		// need to wait for all asynchronous batches to finish writing
		// before exiting close() method
		writeSemaphore
				.acquire(
						MAX_CONCURRENT_WRITE);
		writeSemaphore
				.release(
						MAX_CONCURRENT_WRITE);
	}

	// public Iterator<ScoredEntry<V>> entryRangeAsync(
	// final double startScore,
	// final boolean startScoreInclusive,
	// final double endScore,
	// final boolean endScoreInclusive ) {
	// return currentSet
	// .entryRangeAsync(
	// startScore,
	// startScoreInclusive,
	// endScore,
	// endScoreInclusive);
	// }
}
