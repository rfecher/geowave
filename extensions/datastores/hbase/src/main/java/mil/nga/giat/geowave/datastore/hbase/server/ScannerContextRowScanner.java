package mil.nga.giat.geowave.datastore.hbase.server;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.regionserver.ScannerContext.NextState;
import org.apache.log4j.Logger;

/**
 * this is required to be in org.apache.hadoop.hbase.regionserver because it
 * accesses package private methods within ScannerContext
 *
 *
 */
public class ScannerContextRowScanner implements
		RowScanner
{
	private final static Logger LOGGER = Logger.getLogger(
			ScannerContextRowScanner.class);
	private static ReflectionParams INSTANCE;
	private final InternalScanner scanner;
	private final ScannerContext scannerContext;
	private final List<Cell> cells;
	private boolean done = false;
	private final Scan scan;
	private Map<String, Object> hints;

	private static synchronized ReflectionParams getReflectionInstance() {
		if (INSTANCE == null) {
			INSTANCE = new ReflectionParams();
		}
		return INSTANCE;
	}

	public ScannerContextRowScanner(
			final InternalScanner scanner,
			final List<Cell> cells,
			final ScannerContext scannerContext,
			final Scan scan ) {
		this.scanner = scanner;
		this.cells = cells;
		this.scannerContext = scannerContext;
		this.scan = scan;

	}

	@Override
	public boolean isMidRow() {
		if ((scannerContext == null) || done) {
			return false;
		}
		return partialResultFormed();
	}

	private boolean partialResultFormed() {
		final NextState state = getReflectionInstance().getScannerState(
				scannerContext);
		return (state == NextState.SIZE_LIMIT_REACHED_MID_ROW) || (state == NextState.TIME_LIMIT_REACHED_MID_ROW);

	}

	@Override
	public List<Cell> nextCellsInRow()
			throws IOException {
		if (!isMidRow()) {
			return Collections.EMPTY_LIST;
		}
		getReflectionInstance().reset(
				scannerContext);
		done = !scanner.next(
				cells,
				scannerContext);
		return cells;
	}

	@Override
	public List<Cell> currentCellsInRow() {
		return cells;
	}

	@Override
	public boolean isDone() {
		return done;
	}

	@Override
	public Map<String, Object> getHints() {
		if (hints == null) {
			// this isn't threadsafe but shouldn't need to be
			hints = new HashMap<>();
		}
		return hints;
	}

	@Override
	public Scan getScan() {
		return scan;
	}

	private static class ReflectionParams
	{
		// if we ever encounter an exception
		private boolean exception = false;

		private Method clearProgress;
		private Method setScannerState;
		private Field scannerState;

		public ReflectionParams() {
			try {
				scannerState = ScannerContext.class.getField(
						"scannerState");
				scannerState.setAccessible(
						true);
				clearProgress = ScannerContext.class.getMethod(
						"clearProgress");
				clearProgress.setAccessible(
						true);
				setScannerState = ScannerContext.class.getMethod(
						"setScannerState",
						NextState.class);
				setScannerState.setAccessible(
						true);
			}
			catch (final Exception e) {
				LOGGER.warn(
						"Unable to get accessible methods for ScannerContextRowScanner",
						e);
				exception = true;
			}
		}

		public NextState getScannerState(
				final ScannerContext instance ) {
			if (!exception) {
				try {
					return (NextState) scannerState.get(
							instance);
				}
				catch (final Exception e) {
					LOGGER.warn(
							"Unable to check partial result of scanner context",
							e);
					exception = true;
				}
			}
			return NextState.MORE_VALUES;
		}

		public void reset(
				final ScannerContext instance ) {
			if (!exception) {
				try {
					clearProgress.invoke(
							instance);

					setScannerState.invoke(
							instance,
							NextState.MORE_VALUES);
				}
				catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
					LOGGER.warn(
							"Unable to invoke reset of scanner context",
							e);
					exception = true;
				}
			}
		}
	}
}
