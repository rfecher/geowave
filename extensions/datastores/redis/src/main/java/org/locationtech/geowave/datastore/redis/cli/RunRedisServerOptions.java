package org.locationtech.geowave.datastore.redis.cli;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.io.FileUtils;

import com.beust.jcommander.Parameter;

import redis.embedded.RedisExecProvider;
import redis.embedded.RedisServer;
import redis.embedded.RedisServerBuilder;

public class RunRedisServerOptions
{
	@Parameter(names = {
		"--port",
		"-p"
	}, description = "Select the port for Redis to listen on (default is port 6379)")
	private Integer port = 6379;

	@Parameter(names = {
		"--directory",
		"-d"
	}, description = "The directory to use for Redis. If set, the data will be persisted and durable. If none, it will use a temp directory and delete when complete")
	private String directory = null;
	@Parameter(names = {
		"--maxMemory",
		"-m"
	}, description = "The maximum memory to use (in the form such as 512M)")
	private String memory = "512M";

	@Parameter(names = {
		"--setting",
		"-s"
	}, description = "A setting to apply to Redis in the form of <name>=<value>")
	private List<String> settings = new ArrayList<>();

	public RedisServer getServer()
			throws IOException {
		final RedisServerBuilder bldr = RedisServer.builder().port(
				port).setting(
				"bind 127.0.0.1"); // secure + prevents popups on
									// Windows
		boolean appendOnlySet = false;
		for (final String s : settings) {
			final String[] kv = s.split("=");
			if (kv.length == 2) {
				if (kv[0].equalsIgnoreCase("appendonly")) {
					appendOnlySet = true;
				}
				bldr.setting(kv[0] + " " + kv[1]);
			}
		}
		if ((directory != null) && (directory.trim().length() > 0)) {
			final File f = RedisExecProvider.defaultProvider().get();

			final File directoryFile = new File(
					directory);
			directoryFile.mkdirs();

			final File newExecFile = new File(
					directoryFile,
					f.getName());
			FileUtils.moveFile(
					f,
					newExecFile);
			if (!appendOnlySet) {
				bldr.setting("appendonly yes");
				bldr.setting("appendfsync everysec");
			}
		}
		bldr.setting("maxmemory " + memory.trim());
		return bldr.build();
	}
}
