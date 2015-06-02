package mil.nga.giat.geowave.analytic.param;

import org.apache.commons.cli.Option;

public interface ParameterEnum
{
	Class<?> getBaseClass();

	Enum<?> self();

	Option getOption();
}
