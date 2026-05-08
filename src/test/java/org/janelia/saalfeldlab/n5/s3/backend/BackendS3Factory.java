package org.janelia.saalfeldlab.n5.s3.backend;

import software.amazon.awssdk.services.s3.S3Client;

public class BackendS3Factory {

    private static S3Client s3;

    public static S3Client getOrCreateS3() {

        if (s3 == null)
            s3 = S3Client.builder().build();
        return s3;
    }
}
