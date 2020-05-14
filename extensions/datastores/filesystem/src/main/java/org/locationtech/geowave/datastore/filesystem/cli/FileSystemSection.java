package org.locationtech.geowave.datastore.filesystem.cli;

import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.DefaultOperation;
import org.locationtech.geowave.core.cli.operations.util.UtilSection;
import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "filesystem", parentOperation = UtilSection.class)
@Parameters(commandDescription = "DynamoDB embedded server commands")
public class FileSystemSection extends DefaultOperation {

}
