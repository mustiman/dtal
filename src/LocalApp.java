import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.StartInstancesRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

/**
 * @author Tomp
 *
 */
public class LocalApp {

	static AmazonEC2 ec2;
	static AmazonS3 s3;

	public static void main1(String[] args) throws Exception {

		File inputImagesFile;
		// Obtain input arguments
		try
		{
			inputImagesFile = new File(args[0]);
			int numOfWorkers = Integer.parseInt(args[1]);
		}

		catch (Exception e)
		{
			System.out.println("Problem with input.");
			System.out.println("Error message: " + e.getMessage());
			System.out.println("Program was terminated.");
			return;
		}

		// Initialize EC2 service
		init();

		// Retrieve the manager instance from EC2
		Instance managerInstance = getManager();

		// Verify that manager was found and valid
		if ( managerInstance == null )
		{
			System.out.println("No manager insatnce was found.");
			System.out.println("Program was terminated.");
			return;
		}

		// if manager is not running (running is code 16), start it
		// TODO - handle case where manager can't run
		if ( managerInstance.getState().getCode() != 16 )
		{
			System.out.println("Manager was stopped. Starting manager...");
			startIntance(managerInstance.getInstanceId());
		}

		// upload the file with the list of images to S3
		String eTag = s3.putObject(new PutObjectRequest("testBucketcm_sdlm4er", "hello2.txt", inputImagesFile)).getETag();
		if (eTag != null)
		{
			System.out.println("File uploaded, tag is: " + eTag);
		}
		
		else return;
		
		S3Object object = s3.getObject(new GetObjectRequest("testBucketcm_sdlm4er","hello2.txt"));
		InputStream objectData = object.getObjectContent();
		
		/* just for debugging */
		int b = objectData.read();
		while ( b > -1 )
		{
			System.out.print((char)b);
			if ((char)b == '\n')
				System.out.println("new line");
			b = objectData.read();
		}
		objectData.close();
		
		

	}

	/**
	 * Initialize EC2 and S3 clients.
	 * 
	 * @throws Exception
	 */
	private static void init() throws Exception {
		/*
		 * This credentials provider implementation loads your AWS credentials
		 * from a properties file at the root of your classpath.
		 */
		AWSCredentialsProvider credentialsProvider = new ClasspathPropertiesFileCredentialsProvider();
		ec2 = new AmazonEC2Client(credentialsProvider);
		s3  = new AmazonS3Client(credentialsProvider);

	}

	/**
	 * Create and run numOfInstances number of instances.
	 * 
	 * @param numOfInstances number of instances to run.
	 * @param ami Machine Image in which all instances will be created.
	 * @return A list of the instances created and running.
	 */
	private static List<Instance> CreateRunInstances(int numOfInstances, String ami)
	{
		RunInstancesRequest runInstancesRequest = 
				new RunInstancesRequest();

		runInstancesRequest.withImageId(ami)
		.withInstanceType("t1.micro")
		.withMinCount(1)
		.withMaxCount(numOfInstances)
		.withKeyName("ass1")
		.withSecurityGroups("default");

		RunInstancesResult runInstancesResult = 
				ec2.runInstances(runInstancesRequest);

		return runInstancesResult.getReservation().getInstances();
	}

	/**
	 * Retrieve the manager instance from EC2.
	 * 
	 * @return manager instance object, or null if not found
	 */
	private static Instance getManager()
	{
		// create a filter
		List<String> valuesT1 = new ArrayList<String>();
		valuesT1.add("Manager");
		Filter filter = new Filter("tag:Name", valuesT1);

		// configure request for instances, based on filter
		DescribeInstancesRequest request = new DescribeInstancesRequest();
		DescribeInstancesResult result = ec2.describeInstances(request.withFilters(filter));
		List<Reservation> reservations = result.getReservations();

		// retrieve the manager instance from EC2 and return it
		try
		{
			return reservations.get(0).getInstances().get(0);
		}

		// TODO - validate manager (is active as expected)

		// in case no manager was was return null
		catch (Exception e)
		{
			return null;
		}
	}

	/**
	 * Start specific (existing) instance, based on ID.
	 * 
	 * @param instanceID the target instance to run ID.
	 */
	private static void startIntance(String instanceID)
	{
		List<String> instancesToStart = new ArrayList<String>();
		instancesToStart.add(instanceID);
		StartInstancesRequest starter = new StartInstancesRequest();
		starter.setInstanceIds(instancesToStart);
		ec2.startInstances(starter);
	}


}