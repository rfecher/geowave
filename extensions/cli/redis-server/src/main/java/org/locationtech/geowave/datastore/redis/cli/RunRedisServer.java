package org.locationtech.geowave.datastore.redis.cli;

import java.util.concurrent.TimeUnit;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.Command;
import org.locationtech.geowave.core.cli.api.DefaultOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import redis.embedded.RedisServer;

@GeowaveOperation(name = "run", parentOperation = RedisSection.class)
@Parameters(commandDescription = "Runs a standalone Redis server for test and debug with GeoWave")
public class RunRedisServer extends DefaultOperation implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(RunRedisServer.class);

  @ParametersDelegate
  private final RunRedisServerOptions options = new RunRedisServerOptions();
  // @Parameter(
  // names = {"--interactive", "-i"},
  // description = "Whether to prompt for user input to end the process")
  private final boolean interactive = true;

  /**
   * Prep the driver & run the operation.
   */
  @Override
  public void execute(final OperationParams params) {
    try {
      final RedisServer server = options.getServer();
      server.start();

      if (interactive) {
        System.out.println("hit any key to shutdown ..");
        System.in.read();
        System.out.println("Shutting down!");
        server.stop();
      } else {
        Runtime.getRuntime().addShutdownHook(new Thread() {
          @Override
          public void run() {
            try {
              server.stop();
            } catch (final Exception e) {
              LOGGER.warn("Unable to shutdown redis", e);
              System.out.println("Error shutting down redis.");
            }
            System.out.println("Shutting down!");
          }
        });

        while (true) {
          Thread.sleep(TimeUnit.MILLISECONDS.convert(Long.MAX_VALUE, TimeUnit.DAYS));
        }
      }

    } catch (final Exception e) {
      LOGGER.error("Unable to run embedded Redis server", e);
    }

  }
}
