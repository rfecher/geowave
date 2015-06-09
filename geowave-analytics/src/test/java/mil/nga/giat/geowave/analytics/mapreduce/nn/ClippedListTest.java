package mil.nga.giat.geowave.analytics.mapreduce.nn;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.util.AbstractMap;
import java.util.Map;

import mil.nga.giat.geowave.index.ByteArrayId;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ClippedListTest
{

	private Map.Entry<ByteArrayId, Integer> createEntry(
			final Integer v ) {
		return new AbstractMap.SimpleEntry<ByteArrayId, Integer>(
				new ByteArrayId(
						v.toString()),
				v);
	}

	@Test
	public void test() {
		final ClippedList<Integer> clippedList = new ClippedList<Integer>(
				8);
		for (int i = 0; i < 16; i++) {
			clippedList.add(createEntry(i));
		}
		assertEquals(
				8,
				clippedList.size());
		assertEquals(
				16,
				clippedList.addCount());

		clippedList.remove(1);
		assertEquals(
				7,
				clippedList.size());
		assertEquals(
				15,
				clippedList.addCount());

		final ClippedList<Integer> clippedList2 = new ClippedList<Integer>(
				8);
		for (int i = 0; i < 16; i++) {
			clippedList.add(createEntry(i + 100));
		}
		clippedList.addAll(clippedList2);

		assertEquals(
				8,
				clippedList.size());
		assertEquals(
				31,
				clippedList.addCount());

		clippedList.remove(createEntry(3));

		assertEquals(
				7,
				clippedList.size());
		assertEquals(
				30,
				clippedList.addCount());
		clippedList.add(
				3,
				createEntry(3));
		assertEquals(
				8,
				clippedList.size());
		assertEquals(
				31,
				clippedList.addCount());

		assertEquals(
				new Integer(
						3),
				clippedList.get(
						3).getValue());

		assertFalse(clippedList.isEmpty());

		assertEquals(
				3,
				clippedList.indexOf(createEntry(3)));

		assertNotNull(clippedList.toArray());

		assertNotNull(clippedList.toArray(new Map.Entry[7]));

	}

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void unsupportedRetainTest() {
		thrown.expect(UnsupportedOperationException.class);
		final ClippedList<Integer> clippedList = new ClippedList<Integer>(
				8);
		clippedList.retainAll(null);
	}

	@Test
	public void unsupportedContainsTest() {
		thrown.expect(UnsupportedOperationException.class);
		final ClippedList<Integer> clippedList = new ClippedList<Integer>(
				8);
		clippedList.contains(null);
	}
}
