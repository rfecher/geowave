package mil.nga.giat.geowave.datastore.dynamodb.operations;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.log4j.Logger;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.SinglePartitionQueryRanges;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.operations.Reader;
import mil.nga.giat.geowave.core.store.operations.ReaderParams;
import mil.nga.giat.geowave.datastore.dynamodb.DynamoDBRow;
import mil.nga.giat.geowave.datastore.dynamodb.DynamoDBRow.DynamoDBRowMergingIterator;
import mil.nga.giat.geowave.datastore.dynamodb.util.AsyncPaginatedQuery;
import mil.nga.giat.geowave.datastore.dynamodb.util.AsyncPaginatedScan;
import mil.nga.giat.geowave.datastore.dynamodb.util.DynamoDBUtils;
import mil.nga.giat.geowave.datastore.dynamodb.util.LazyPaginatedQuery;
import mil.nga.giat.geowave.datastore.dynamodb.util.LazyPaginatedScan;
import mil.nga.giat.geowave.mapreduce.splits.GeoWaveRowRange;
import mil.nga.giat.geowave.mapreduce.splits.RecordReaderParams;

public class DynamoDBReader implements
		Reader
{
	private final static Logger LOGGER = Logger.getLogger(DynamoDBReader.class);
	private static final boolean ASYNC = false;
	private final ReaderParams readerParams;
	private final RecordReaderParams recordReaderParams;
	private final DynamoDBOperations operations;
	private final boolean clientSideRowMerging;

	private final boolean wholeRowEncoding;
	private final int partitionKeyLength;
	private Iterator<DynamoDBRow> iterator;

	private Map<String, AttributeValue> expressionAttributeValues = null;
	private String filterExpression = null;

	public DynamoDBReader(
			final ReaderParams readerParams,
			final DynamoDBOperations operations ) {
		this.readerParams = readerParams;
		recordReaderParams = null;
		processAuthorizations(readerParams.getAdditionalAuthorizations());
		this.operations = operations;

		partitionKeyLength = readerParams.getIndex().getIndexStrategy().getPartitionKeyLength();
		wholeRowEncoding = readerParams.isMixedVisibility() && !readerParams.isServersideAggregation();
		clientSideRowMerging = readerParams.isClientsideRowMerging();

		initScanner();
	}

	public DynamoDBReader(
			final RecordReaderParams recordReaderParams,
			final DynamoDBOperations operations ) {
		readerParams = null;
		this.recordReaderParams = recordReaderParams;
		processAuthorizations(recordReaderParams.getAdditionalAuthorizations());
		this.operations = operations;

		partitionKeyLength = recordReaderParams.getIndex().getIndexStrategy().getPartitionKeyLength();
		wholeRowEncoding = recordReaderParams.isMixedVisibility() && !recordReaderParams.isServersideAggregation();
		clientSideRowMerging = false;

		initRecordScanner();
	}

	private void processAuthorizations(
			final String[] authorizations ) {
		final StringBuilder filterExpressionBuilder = new StringBuilder(
				"attribute_not_exists(").append(
				DynamoDBRow.GW_VISIBILITY_KEY).append(
				")");
		if ((authorizations != null) && (authorizations.length > 0)) {
			expressionAttributeValues = Maps.newHashMap();
			int authCount = 1;
			filterExpressionBuilder.append(
					" or ").append(
					DynamoDBRow.GW_VISIBILITY_KEY).append(
					" in (");
			for (final String auth : authorizations) {
				if (authCount > 1) {
					filterExpressionBuilder.append(",");
				}
				final String authKey = ":auth" + authCount;
				expressionAttributeValues.put(
						authKey,
						new AttributeValue().withB(ByteBuffer.wrap(StringUtils.stringToBinary(auth))));
				filterExpressionBuilder.append(authKey);
				authCount++;
			}
			filterExpressionBuilder.append(")");
		}

		filterExpression = filterExpressionBuilder.toString();
	}

	protected void initScanner() {
		final String tableName = operations.getQualifiedTableName(
				readerParams.getIndex().getId().getString());

//		if ((readerParams.getLimit() != null) && (readerParams.getLimit() > 0)) {
			//TODO: we should do something here
//		}

		final List<QueryRequest> requests = new ArrayList<>();

		final Collection<SinglePartitionQueryRanges> ranges = readerParams.getQueryRanges().getPartitionQueryRanges();

		if ((ranges != null) && !ranges.isEmpty()) {
			ranges.forEach(
					(queryRequest -> requests.addAll(
							addQueryRanges(
									tableName,
									queryRequest,
									readerParams.getAdapterIds(),
									readerParams.getAdapterStore()))));

		}
//		else if ((adapterIds != null) && !adapterIds.isEmpty()) {
//			//TODO this isn't going to work because there aren't partition keys being passed along
//			requests.addAll(
//					getAdapterOnlyQueryRequests(
//							tableName,
//							adapterIds));
//		}

		startRead(
				requests,
				tableName);
	}

	protected void initRecordScanner() {
		final String tableName = operations.getQualifiedTableName(recordReaderParams.getIndex().getId().getString());

		final ArrayList<ByteArrayId> adapterIds = Lists.newArrayList();
		if ((recordReaderParams.getAdapterIds() != null) && !recordReaderParams.getAdapterIds().isEmpty()) {
			for (final ByteArrayId adapterId : recordReaderParams.getAdapterIds()) {
				adapterIds.add(adapterId);
			}
		}

		final List<QueryRequest> requests = new ArrayList<>();

		final GeoWaveRowRange range = recordReaderParams.getRowRange();
		for (final ByteArrayId adapterId : adapterIds) {
			final ByteArrayId startKey = range.isInfiniteStartSortKey() ? null : new ByteArrayId(
					range.getStartSortKey());
			final ByteArrayId stopKey = range.isInfiniteStopSortKey() ? null : new ByteArrayId(
					range.getEndSortKey());
			requests.add(getQuery(
					tableName,
					range.getPartitionKey(),
					new ByteArrayRange(
							startKey,
							stopKey),
					adapterId));
		}
		startRead(
				requests,
				tableName);
	}

	private void startRead(
			final List<QueryRequest> requests,
			final String tableName ) {
		Iterator<Map<String, AttributeValue>> rawIterator;
		if (!requests.isEmpty()) {
			if (ASYNC) {
				rawIterator = Iterators.concat(
						requests.parallelStream().map(
								this::executeAsyncQueryRequest).iterator());
			}
			else {
				rawIterator = Iterators.concat(
						requests.parallelStream().map(
								this::executeQueryRequest).iterator());
			}
		}
		else {
			if (ASYNC) {
				final ScanRequest request = authorizationFilter(new ScanRequest(
						tableName));
				rawIterator = new AsyncPaginatedScan(
						request,
						operations.getClient());
			}
			else {
				// query everything
				final ScanRequest request = authorizationFilter(new ScanRequest(
						tableName));
				final ScanResult scanResult = operations.getClient().scan(
						request);
				rawIterator = new LazyPaginatedScan(
						scanResult,
						request,
						operations.getClient());
			}
		}
		
		iterator = new DynamoDBRowMergingIterator(rawIterator);
	}

	@Override
	public void close()
			throws Exception {

	}

	@Override
	public boolean hasNext() {
		return iterator.hasNext();
	}

	@Override
	public GeoWaveRow next() {
		return iterator.next();
	}

	private List<QueryRequest> getAdapterOnlyQueryRequests(
			final String tableName,
			final ArrayList<ByteArrayId> adapterIds ) {
		final List<QueryRequest> allQueries = new ArrayList<>();

		for (final ByteArrayId adapterId : adapterIds) {
			final QueryRequest singleAdapterQuery = new QueryRequest(
					tableName);

			final byte[] start = adapterId.getBytes();
			final byte[] end = adapterId.getNextPrefix();
			singleAdapterQuery.addKeyConditionsEntry(
					DynamoDBRow.GW_RANGE_KEY,
					new Condition().withComparisonOperator(
							ComparisonOperator.BETWEEN).withAttributeValueList(
							new AttributeValue().withB(ByteBuffer.wrap(start)),
							new AttributeValue().withB(ByteBuffer.wrap(end))));

			allQueries.add(singleAdapterQuery);
		}

		return allQueries;
	}

	private QueryRequest authorizationFilter(
			final QueryRequest request ) {
		if (expressionAttributeValues != null) {
			request.withExpressionAttributeValues(expressionAttributeValues);
		}
		if (filterExpression != null) {
			request.setFilterExpression(filterExpression);
		}
		return request;
	}

	private ScanRequest authorizationFilter(
			final ScanRequest request ) {
		if (expressionAttributeValues != null) {
			request.withExpressionAttributeValues(expressionAttributeValues);
		}
		if (filterExpression != null) {
			request.setFilterExpression(filterExpression);
		}
		return request;
	}

	private QueryRequest getQuery(
			final String tableName,
			final byte[] partitionId,
			final ByteArrayRange sortRange,
			final ByteArrayId adapterID ) {
		final byte[] start;
		final byte[] end;
		final QueryRequest query = new QueryRequest(
				tableName).addKeyConditionsEntry(
				DynamoDBRow.GW_PARTITION_ID_KEY,
				new Condition().withComparisonOperator(
						ComparisonOperator.EQ).withAttributeValueList(
						new AttributeValue().withB(ByteBuffer.wrap(partitionId))));
		if (sortRange == null) {
			start = adapterID.getBytes();
			end = adapterID.getNextPrefix();
		}
		else if (sortRange.isSingleValue()) {
			start = ByteArrayUtils.combineArrays(
					adapterID.getBytes(),
					DynamoDBUtils.encodeSortableBase64(sortRange.getStart().getBytes()));
			end = ByteArrayUtils.combineArrays(
					adapterID.getBytes(),
					DynamoDBUtils.encodeSortableBase64(sortRange.getStart().getNextPrefix()));
		}
		else {
			if (sortRange.getStart() == null) {
				start = adapterID.getBytes();
			}
			else {
				start = ByteArrayUtils.combineArrays(
						adapterID.getBytes(),
						DynamoDBUtils.encodeSortableBase64(sortRange.getStart().getBytes()));
			}
			if (sortRange.getEnd() == null) {
				end = adapterID.getNextPrefix();
			}
			else {
				end = ByteArrayUtils.combineArrays(
						adapterID.getBytes(),
						DynamoDBUtils.encodeSortableBase64(sortRange.getEndAsNextPrefix().getBytes()));
			}
		}
		query.addKeyConditionsEntry(
				DynamoDBRow.GW_RANGE_KEY,
				new Condition().withComparisonOperator(
						ComparisonOperator.BETWEEN).withAttributeValueList(
						new AttributeValue().withB(ByteBuffer.wrap(start)),
						new AttributeValue().withB(ByteBuffer.wrap(end))));
		return authorizationFilter(query);
	}

	private List<QueryRequest> addQueryRanges(
			final String tableName,
			final SinglePartitionQueryRanges r,
			List<ByteArrayId> adapterIds,
			final AdapterStore adapterStore ) {
		final List<QueryRequest> retVal = new ArrayList<>();
		final ByteArrayId partitionKey = r.getPartitionKey();
		final byte[] partitionId = ((partitionKey == null) || (partitionKey.getBytes().length == 0))
				? DynamoDBWriter.EMPTY_PARTITION_KEY : partitionKey.getBytes();
		if (adapterIds == null) {
			adapterIds = Lists.newArrayList();
		}
		if (adapterIds.isEmpty() && adapterStore != null) {
			final CloseableIterator<DataAdapter<?>> adapters = adapterStore.getAdapters();

			final List<ByteArrayId> adapterIDList = Lists.newArrayList();
			adapters.forEachRemaining(
					new Consumer<DataAdapter<?>>() {
						@Override
						public void accept(
								final DataAdapter<?> t ) {
							adapterIDList.add(
									t.getAdapterId());
						}
					});
			adapterIds.addAll(
					adapterIDList);
		}

		for (final ByteArrayId adapterId : adapterIds) {
			final Collection<ByteArrayRange> sortKeyRanges = r.getSortKeyRanges();
			if ((sortKeyRanges != null) && !sortKeyRanges.isEmpty()) {
				sortKeyRanges.forEach(
						(sortKeyRange -> retVal.add(
								getQuery(
										tableName,
										partitionId,
										sortKeyRange,
										adapterId))));
			}
			else {
				retVal.add(
						getQuery(
								tableName,
								partitionId,
								null,
								adapterId));
			}
		}
		return retVal;
	}

	private Iterator<Map<String, AttributeValue>> executeQueryRequest(
			final QueryRequest queryRequest ) {
		final QueryResult result = operations.getClient().query(
				queryRequest);
		return new LazyPaginatedQuery(
				result,
				queryRequest,
				operations.getClient());
	}

	/**
	 * Asynchronous version of the query request. Does not block
	 */
	public Iterator<Map<String, AttributeValue>> executeAsyncQueryRequest(
			final QueryRequest queryRequest ) {
		return new AsyncPaginatedQuery(
				queryRequest,
				operations.getClient());
	}
}
