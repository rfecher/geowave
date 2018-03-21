package mil.nga.giat.geowave.service.grpc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.Server;
import io.grpc.ServerBuilder;

public class GeoWaveGrpcServer
{
	private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveGrpcServer.class.getName());

	private final int port;
	private final Server server;

	public GeoWaveGrpcServer(
			final int port )
			throws IOException {
		this.port = port;

		// This is a bare-bones implementation to be used as a template, add
		// more services as desired
		server = ServerBuilder.forPort(
				port).addService(
				new GeoWaveGrpcVectorQueryService()).build();
	}

	/** Start serving requests. */
	public void start()
			throws IOException {
		server.start();
		LOGGER.info("Server started, listening on " + port);
		Runtime.getRuntime().addShutdownHook(
				new Thread() {
					@Override
					public void run() {
						// Use stderr here since the logger may have been reset
						// by its JVM shutdown hook.
						System.err.println("*** shutting down gRPC server since JVM is shutting down");
						GeoWaveGrpcServer.this.stop();
						System.err.println("*** server shut down");
					}
				});
	}

	/** Stop serving requests and shutdown resources. */
	public void stop() {
		if (server != null) {
			server.shutdown();
		}
	}

	/**
	 * Await termination on the main thread since the grpc library uses daemon
	 * threads.
	 */
	public void blockUntilShutdown()
			throws InterruptedException {
		if (server != null) {
			server.awaitTermination();
		}
	}

}
