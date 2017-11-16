# n5-aws-s3
N5 library implementation using Amazon Web Services S3 backend.

### Authentication

This [test](https://github.com/saalfeldlab/n5-aws-s3/blob/master/src/test/java/org/janelia/saalfeldlab/n5/s3/N5AmazonS3Test.java) shows how to create an S3 client. It is excluded from the test run configuration by default and requires a few steps to set up:

1. Create access keys in the [AWS console](https://console.aws.amazon.com/iam/home?#/security_credential).
1. It is expected that your AWS credentials (access key ID and secret key) are provided in one of the following sources:
    * **Environment variables**: `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
    * **Java system properties**: `aws.accessKeyId` and `aws.secretKey`
    * **Credentials profile file**: typically located at `~/.aws/credentials` (location can vary per platform). This is the recommended way as the file can be conveniently created with [AWS CLI](https://aws.amazon.com/cli/) by `aws configure` command.
