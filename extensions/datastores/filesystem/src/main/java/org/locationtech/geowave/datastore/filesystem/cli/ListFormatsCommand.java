package org.locationtech.geowave.datastore.filesystem.cli;

import java.util.Map.Entry;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.api.ServiceEnabledCommand;
import org.locationtech.geowave.datastore.filesystem.FileSystemDataFormatterRegistry;
import org.locationtech.geowave.datastore.filesystem.FileSystemDataFormatterSpi;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "listformats", parentOperation = FileSystemSection.class)
@Parameters(
    commandDescription = "List available formats for usage with --format option with FileSystem datastore")
public class ListFormatsCommand extends ServiceEnabledCommand<String> {

  @Override
  public void execute(final OperationParams params) {
    JCommander.getConsole().println(computeResults(params));
  }

  @Override
  public String computeResults(final OperationParams params) {
    final StringBuilder builder = new StringBuilder();

    builder.append("Available data formats currently registered as plugins:\n");
    for (final Entry<String, FileSystemDataFormatterSpi> dataFormatterEntry : FileSystemDataFormatterRegistry.getDataFormatterRegistry().entrySet()) {
      final FileSystemDataFormatterSpi pluginProvider = dataFormatterEntry.getValue();
      final String desc =
          pluginProvider.getFormatDescription() == null ? "no description"
              : pluginProvider.getFormatDescription();
      builder.append(String.format("%n  %s:%n    %s%n", dataFormatterEntry.getKey(), desc));
    }
    return builder.toString();
  }
}
