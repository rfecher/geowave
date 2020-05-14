package org.locationtech.geowave.datastore.filesystem.cli;

import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.DefaultOperation;
import org.locationtech.geowave.core.cli.operations.util.UtilSection;
import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "filesystem", parentOperation = UtilSection.class)
@Parameters(commandDescription = "FileSystem datastore commands, currently just listformats to list available data format plugins")
public class FileSystemSection extends DefaultOperation {

}
