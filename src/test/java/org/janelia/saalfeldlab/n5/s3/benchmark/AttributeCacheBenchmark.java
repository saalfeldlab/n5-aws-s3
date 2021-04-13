package org.janelia.saalfeldlab.n5.s3.benchmark;

import java.io.IOException;
import java.util.HashMap;
import java.util.Optional;

import org.janelia.saalfeldlab.n5.s3.N5AmazonS3Reader;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3URI;
import com.google.gson.JsonElement;

public class AttributeCacheBenchmark
{

	public static void main( String[] args ) throws IOException
	{
		final String n5RootUrl = "https://janelia-cosem.s3.amazonaws.com/jrc_hela-2/jrc_hela-2.n5";
		final String dataset = "/em/fibsem-uint16/s4";


		final AmazonS3URI uri = new AmazonS3URI( n5RootUrl );	
		final boolean cacheAttributes = true;
		final N5AmazonS3Reader n5 = new N5AmazonS3Reader( createS3( n5RootUrl ), uri, cacheAttributes );
		
		
		final long startTime = System.currentTimeMillis();
		HashMap< String, JsonElement > attrs = n5.getAttributes( dataset );
		
		// a non-existant attribute
		for ( int i = 0; i < 20; i++ )
			n5.getAttribute( dataset, "cow", String.class );

		n5.getAttribute( dataset, "pixelResolution", FinalVoxelDimensions.class );
		n5.getAttribute( dataset, "blockSize", int[].class );
		
		final long endTime = System.currentTimeMillis();

		final long totalTime = endTime - startTime;
		System.out.println( " total time: " +  totalTime );

	}

	public static AmazonS3 createS3( final String url )
	{

		AmazonS3 s3;
		AWSCredentials credentials = null;
		try
		{
			credentials = new DefaultAWSCredentialsProviderChain().getCredentials();
		}
		catch ( final Exception e )
		{
			System.out.println( "Could not load AWS credentials, falling back to anonymous." );
		}
		final AWSStaticCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider( credentials == null ? new AnonymousAWSCredentials() : credentials );

		final AmazonS3URI uri = new AmazonS3URI( url );
		final Optional< String > region = Optional.ofNullable( uri.getRegion() );

		if ( region.isPresent() )
		{
			s3 = AmazonS3ClientBuilder.standard().withCredentials( credentialsProvider ).withRegion( region.map( Regions::fromName ).orElse( Regions.US_EAST_1 ) ).build();
		}
		else
		{
			s3 = AmazonS3ClientBuilder.standard().withCredentials( credentialsProvider ).withRegion( Regions.US_EAST_1 ).build();
		}

		return s3;
	}
	
	public static class FinalVoxelDimensions
	{
		private final String unit;

		private final double[] dimensions;

		public FinalVoxelDimensions( final String unit, final double... dimensions )
		{
			this.unit = unit;
			this.dimensions = dimensions.clone();
		}

		public int numDimensions()
		{
			return dimensions.length;
		}

		public String unit()
		{
			return unit;
		}

		public void dimensions( final double[] dims )
		{
			for ( int d = 0; d < dims.length; ++d )
				dims[ d ] = this.dimensions[ d ];
		}

		public double dimension( final int d )
		{
			return dimensions[ d ];
		}
	}

}
