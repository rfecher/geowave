package mil.nga.giat.geowave.core.store.index;

import java.util.List;

import mil.nga.giat.geowave.core.index.QueryConstraints;

public class CompositeConstraints implements
		QueryConstraints
{
	private List<QueryConstraints> constraints;

	public CompositeConstraints(
			List<QueryConstraints> constraints ) {
		super();
		this.constraints = constraints;
	}

	public List<QueryConstraints> getConstraints() {
		return constraints;
	}

	@Override
	public int getDimensionCount() {
		return constraints == null ? 0 : constraints.size();
	}

	@Override
	public boolean isEmpty() {
		return constraints == null || constraints.isEmpty();
	}

}
