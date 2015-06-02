package mil.nga.giat.geowave.analytic;

public interface ConfigurationWrapper
{
	public int getInt(
			Enum<?> property,
			int defaultValue );

	public double getDouble(
			Enum<?> property,
			double defaultValue );

	public String getString(
			Enum<?> property,
			String defaultValue );

	public byte[] getBytes(
			Enum<?> property );

	public <T> T getInstance(
			Enum<?> property,
			Class<T> iface,
			Class<? extends T> defaultValue )
			throws InstantiationException,
			IllegalAccessException;
}
