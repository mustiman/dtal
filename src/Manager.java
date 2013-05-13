import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.UUID;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

/**
 * 
 */

/**
 * @author Tal
 *
 */
public class Manager {

	static AmazonSQS sqs;
	static AmazonS3 s3;

	/**
	 * Initialize SQS and S3 clients.
	 * 
	 * @throws Exception
	 */
	private static void init() throws Exception {
		/*
		 * This credentials provider implementation loads your AWS credentials
		 * from a properties file at the root of your classpath.
		 */
		AWSCredentialsProvider credentialsProvider = new ClasspathPropertiesFileCredentialsProvider();
		sqs = new AmazonSQSClient(credentialsProvider);
		s3  = new AmazonS3Client(credentialsProvider);
	}


	private static String getMessageAndDelete(String queueUrl){
		String ans = "";
		// Receive messages
		System.out.println("Receiving messages");
		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl);
		List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
		if (messages.size()>0){
			Message message = messages.get(0);
			ans = message.getBody();
			String messageRecieptHandle = message.getReceiptHandle();
			sqs.deleteMessage(new DeleteMessageRequest(queueUrl, messageRecieptHandle));
		}
		else{
			System.out.println("queue is empty");
			return "";
		}
		return ans;
	}


	public static void main(String[] args) throws Exception {

		String localAppQueue = "https://sqs.us-east-1.amazonaws.com/152554501442/new_task_queue"; //args[1];
		String workersQueue = "https://sqs.us-east-1.amazonaws.com/152554501442/new_image_task_queue"; //args[0];

		ReceiveMessageRequest receiveMessageRequest;
		List<Message> messages;
		boolean queue_is_empty=false;

		int n = Integer.parseInt(args[1]);
		int messageCount=0;

		//Initialize s3 and sqs 
		init();

		while (getMessageAndDelete(localAppQueue) == "")
		{
			System.out.println("waiting for image list message from local app...");
		}

		//get image list
		S3Object imageList = s3.getObject(new GetObjectRequest("testBucketcm_sdlm4er","text.images.txt"));

		//read images file
		InputStream objectData = imageList.getObjectContent();
		int c = objectData.read();
		String imageUrl ="";
		while ( c > -1 )
		{
			imageUrl += (char)c;
			if ((char)c == '\n')
			{
				try {
					// Send a message
					System.out.println(imageUrl + " - was sent");
					sqs.sendMessage(new SendMessageRequest(workersQueue, imageUrl));
					messageCount++;
					imageUrl="";
				} catch (Exception e) {
					System.out.println("An error occurred during message sending");
				}
			}
			c = objectData.read();
		}
		objectData.close();

		//System.out.println(getMessageAndDelete(workersQueue) +"     "+ messageCount);
		for (int i=0; i< (int)Math.ceil((messageCount/n)); i++){
			System.out.println("creat worker...");
		}

		while(!queue_is_empty){
			receiveMessageRequest = new ReceiveMessageRequest(workersQueue);
			messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
			if (messages.size()==0){
				queue_is_empty = true;
			}
		}
		
		System.out.println("all done");
		try {
			/*			// Create a queue
			System.out.println("Creating a new SQS queue called MyQueue.\n");
			CreateQueueRequest createQueueRequest = new CreateQueueRequest("MyQueue"+ UUID.randomUUID());
			String myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();

			// List queues
			System.out.println("Listing all queues in your account.\n");
			for (String queueUrl : sqs.listQueues().getQueueUrls()) {
				System.out.println("  QueueUrl: " + queueUrl);
			}
			System.out.println();

			// Send a message
			System.out.println("Sending a message to MyQueue.\n");
			sqs.sendMessage(new SendMessageRequest(myQueueUrl, "This is my message text."));

			// Receive messages
			System.out.println("Receiving messages from MyQueue.\n");
			ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(myQueueUrl);
			List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
			for (Message message : messages) {
				System.out.println("  Message");
				System.out.println("    MessageId:     " + message.getMessageId());
				System.out.println("    ReceiptHandle: " + message.getReceiptHandle());
				System.out.println("    MD5OfBody:     " + message.getMD5OfBody());
				System.out.println("    Body:          " + message.getBody());
				for (Entry<String, String> entry : message.getAttributes().entrySet()) {
					System.out.println("  Attribute");
					System.out.println("    Name:  " + entry.getKey());
					System.out.println("    Value: " + entry.getValue());
				}
			}
			System.out.println();

			// Delete a message
//			System.out.println("Deleting a message.\n");
//			String messageRecieptHandle = messages.get(0).getReceiptHandle();
//			sqs.deleteMessage(new DeleteMessageRequest(myQueueUrl, messageRecieptHandle));

			// Delete a queue
//			System.out.println("Deleting the test queue.\n");
//			sqs.deleteQueue(new DeleteQueueRequest(myQueueUrl));
			 */		} catch (AmazonServiceException ase) {
				 System.out.println("Caught an AmazonServiceException, which means your request made it " +
						 "to Amazon SQS, but was rejected with an error response for some reason.");
				 System.out.println("Error Message:    " + ase.getMessage());
				 System.out.println("HTTP Status Code: " + ase.getStatusCode());
				 System.out.println("AWS Error Code:   " + ase.getErrorCode());
				 System.out.println("Error Type:       " + ase.getErrorType());
				 System.out.println("Request ID:       " + ase.getRequestId());
			 } catch (AmazonClientException ace) {
				 System.out.println("Caught an AmazonClientException, which means the client encountered " +
						 "a serious internal problem while trying to communicate with SQS, such as not " +
						 "being able to access the network.");
				 System.out.println("Error Message: " + ace.getMessage());
			 }
	}
}

