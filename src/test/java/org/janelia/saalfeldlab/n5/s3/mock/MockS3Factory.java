package org.janelia.saalfeldlab.n5.s3.mock;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

public class MockS3Factory {

	public static final String DEFAULT_ACCESS_KEY = "n5test";

	public static final String DEFAULT_SECRET_KEY = "n5testsecret";

	public static Path mockServerDirectory;

	private static final StringBuilder perTestHttpOut = new StringBuilder();

	public static final URI mockUri = URI.create("http://localhost:9000/");

	private static Process process;

	private static S3Client s3;

	public static S3Client getOrCreateS3() {

		if (s3 == null) {

			if (process == null) {
				try {
					startMockServer();
				} catch (Exception e) {}
			}

			final String user = accessKey();
			final String pw = secretKey();
			final AwsCredentialsProvider creds = new AwsCredentialsProvider() {

				@Override
				public AwsCredentials resolveCredentials() {
					return AwsBasicCredentials.create(user, pw);
				}
			};

			try {
				s3 = S3Client.builder()
						.forcePathStyle(true)
						.region(Region.US_WEST_2)
						.endpointOverride(new URI("http://localhost:9000"))
						.credentialsProvider(creds)
						.build();
			} catch (URISyntaxException e) {
				e.printStackTrace();
			}
		}

		return s3;
	}

	private static String accessKey() {

		final String env = System.getenv("AWS_ACCESS_KEY_ID");
		return env != null ? env : DEFAULT_ACCESS_KEY;
	}

	private static String secretKey() {

		final String env = System.getenv("AWS_SECRET_ACCESS_KEY");
		return env != null ? env : DEFAULT_SECRET_KEY;
	}

	public static void startMockServer() throws Exception {

		if (isMockServerRunning()) {
			return;
		}

		mockServerDirectory = createTmpServerDirectory();
		/* absolute paths; with a relative `-dir` some components still write into the JVM's working directory */
		final String dir = mockServerDirectory.toAbsolutePath().toString();
		final ProcessBuilder processBuilder = new ProcessBuilder(
				"weed", "mini",
				"-dir=" + dir,
				"-master.dir=" + dir,
				"-admin.dataDir=" + dir + "/admin",
				"-ip=127.0.0.1",
				"-s3.port=9000",
				"-s3.port.iceberg=0",
				"-s3.port.lance=0",
				"-admin.ui=false",
				"-master.telemetry=false");
		/* the S3 gateway reads its static credentials from the standard AWS env vars */
		processBuilder.environment().put("AWS_ACCESS_KEY_ID", accessKey());
		processBuilder.environment().put("AWS_SECRET_ACCESS_KEY", secretKey());
		processBuilder.environment().put("PWD", dir);
		processBuilder.directory(mockServerDirectory.toFile());
		processBuilder.redirectErrorStream(true);
		process = processBuilder.start();
		/* only reached when we launched the server ourselves; an already running server is left alone */
		Runtime.getRuntime().addShutdownHook(new Thread(process::destroy));
		waitForReady();
		final Thread clearStdout = new Thread(() -> {
			try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
				String line;
				while ((line = reader.readLine()) != null) {
					perTestHttpOut.append(line).append("\n");
				}
			} catch (IOException e) {
			}
		});
		clearStdout.setDaemon(true);
		clearStdout.start();
	}

	private static Path createTmpServerDirectory() throws IOException {

		/* deleteOnExit doesn't work on temporary files, so delete it manually and recreate explicitly...*/
		final Path tempDirectory = Files.createTempDirectory("n5-s3-mock-server-");
		tempDirectory.toFile().delete();
		tempDirectory.toFile().mkdirs();
		tempDirectory.toFile().deleteOnExit();
		return tempDirectory;
	}

	public static boolean isMockServerRunning() {

		try {
			mockUri.toURL().openConnection().connect();
			return true;
		} catch (IOException e) {
		}
		return false;
	}

	private static void waitForReady() throws IOException, InterruptedException {

		final Thread waitForConnect = new Thread(() -> {
			while (true) {
				try {
					mockUri.toURL().openConnection().connect();
					return;
				} catch (Exception e) {
					 try {
						Thread.sleep(100);
					} catch (InterruptedException e1) {
					}
				}
			}
		});
		waitForConnect.start();
		waitForConnect.join(10_000);
	}

	public static void stop() {

		// probably not necessary
		s3.close();
		s3 = null;

		// we didn't start the server ourselves
		if (process == null)
			return;

		process.destroy();
		try {
			process.waitFor(1, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			process.destroyForcibly();
		}
	}

}
