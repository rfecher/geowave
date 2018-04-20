package mil.nga.giat.geowave.format.stanag4676;

import com.beust.jcommander.Parameter;

import mil.nga.giat.geowave.core.ingest.spi.IngestFormatOptionProvider;

public class Stanag4676Options implements
		IngestFormatOptionProvider
{
	@Parameter(names = "--disableImageChips")
	private boolean disableImageChips = false;

	public boolean isDisableImageChips() {
		return disableImageChips;
	}

	public void setDisableImageChips(
			boolean disableImageChips ) {
		this.disableImageChips = disableImageChips;
	}
}
