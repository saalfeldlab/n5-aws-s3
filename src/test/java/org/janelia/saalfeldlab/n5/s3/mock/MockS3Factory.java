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

	public static Path minioServerDirectory;

	private static final StringBuilder perTestHttpOut = new StringBuilder();

	public static final URI minioUri = URI.create("http://localhost:9000/");

	private static Process process;

    private static S3Client s3;

    public static S3Client getOrCreateS3() {

        if (s3 == null) {
        	
			if (process == null) {
				try {
					startMinioServer();
				} catch (Exception e) {}
			}

			final String userEnv = System.getenv("MINIO_ROOT_USER");
			final String user = userEnv != null ? userEnv : "minioadmin";

			final String pwEnv = System.getenv("MINIO_ROOT_PASSWORD");
			final String pw = pwEnv != null ? pwEnv : "minioadmin";
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

	public static void startMinioServer() throws Exception {

		if( isMinioServerRunning() ) {
			return;
		}

		minioServerDirectory = createTmpServerDirectory();
		ProcessBuilder processBuilder = new ProcessBuilder("minio", "server", ".");
		processBuilder.directory(minioServerDirectory.toFile());
		processBuilder.redirectErrorStream(true);
		process = processBuilder.start();
		waitForReady();
		/* give the server some time to finish startup */
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
		final Path tempDirectory = Files.createTempDirectory("n5-minio-test-server-");
		tempDirectory.toFile().delete();
		tempDirectory.toFile().mkdirs();
		tempDirectory.toFile().deleteOnExit();
		return tempDirectory;
	}

	public static boolean isMinioServerRunning() {

		try {
			minioUri.toURL().openConnection().connect();
			return true;
		} catch (IOException e) {
		}
		return false;
	}

	private static void waitForReady() throws IOException, InterruptedException {

		final Thread waitForConnect = new Thread(() -> {
			while (true) {
				try {
					minioUri.toURL().openConnection().connect();
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
