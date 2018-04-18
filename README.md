# N5 AWS S3
N5 library implementation using Amazon Web Services S3 backend.

### Implementation specifics
* N5 containers are represented by buckets.
* An `attributes.json` with an empty map is always created for any group. It is used to reliably check group existence as S3 does not have conventional directories.

### Authentication

This [test](https://github.com/saalfeldlab/n5-aws-s3/blob/master/src/test/java/org/janelia/saalfeldlab/n5/s3/N5AmazonS3Test.java) shows how to create an S3 client. It is excluded from the test run configuration by default and requires a few steps to set up:

1. Create access keys in the [AWS console](https://console.aws.amazon.com/iam/home?#/security_credential).
1. Configure them on your machine using the credentials profile:
    * Install [AWS Command Line Interface](https://aws.amazon.com/cli/).
    * Run `aws configure` and enter your access key ID, secret key, and geographical region as described [here](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html#cli-quick-configuration).
