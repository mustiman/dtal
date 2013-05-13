import java.awt.image.BufferedImage;
import java.io.IOException;
import java.net.URL;
import java.util.List;

import javax.imageio.ImageIO;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.asprise.util.ocr.OCR;


public class Worker {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		AWSCredentialsProvider credentialsProvider = new ClasspathPropertiesFileCredentialsProvider();
		AmazonSQS sqs = new AmazonSQSClient(credentialsProvider);
		String workersQueue = "https://sqs.us-east-1.amazonaws.com/152554501442/new_image_task_queue";
		String managerQueue = "https://sqs.us-east-1.amazonaws.com/152554501442/done_image_task_queue";
		
		ReceiveMessageRequest receiveMessageRequest;
		List<Message> messages;
		Message message;
		String image_url;
		BufferedImage image = null;
		String parsed_image;
		URL url;

		while(true){			
			
			// Receive messages
			receiveMessageRequest = new ReceiveMessageRequest(workersQueue);
			messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
			if (messages.size()>0){
				message = messages.get(0);
				image_url = message.getBody();
				try {
					url =new URL(image_url);
					// read the url
					image = ImageIO.read(url);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} 
				// recognizes characters 
				parsed_image= new OCR().recognizeCharacters(image);
				// sends the results.
				sqs.sendMessage(new SendMessageRequest(managerQueue, parsed_image));
				
				//delete the message from queue
				String messageRecieptHandle = message.getReceiptHandle();
				sqs.deleteMessage(new DeleteMessageRequest(workersQueue, messageRecieptHandle));
			}
			else
			{
				break;
			}
		}
	}
}
