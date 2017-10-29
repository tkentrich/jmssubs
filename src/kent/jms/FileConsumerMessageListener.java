package kent.jms;

import java.util.Enumeration;

import java.io.IOException;
import java.io.File;
import java.io.FileOutputStream;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.BytesMessage;

public class FileConsumerMessageListener implements MessageListener {
	private String consumerName;
	private File destination;
	private int messageIndex;

	public FileConsumerMessageListener(String consumerName, File destinationDirectory) {
		this.consumerName = consumerName;
		this.destination = destinationDirectory;
		this.messageIndex = 1;
		if (!destinationDirectory.exists()) {
			if (!destinationDirectory.mkdirs()) {
				System.err.println("ERROR: Unable to create " + destinationDirectory);
				System.exit(1);
			}
			System.out.println("Directory " + destinationDirectory + " created");
		} else if (!destinationDirectory.isDirectory()) {
			System.err.println("ERROR: " + destinationDirectory + " is not a directory");
			System.exit(1);
		}
	}

	public void onMessage(Message message) {
		BytesMessage bytesMessage = (BytesMessage) message;
		try {
			System.out.println(consumerName + " received a file");
			String filename = destination + File.separator;
			boolean nameGiven = false;
			System.out.println("Message properties:");
			for (Enumeration e = bytesMessage.getPropertyNames(); e.hasMoreElements(); ) {
				String prop = e.nextElement().toString();
				System.out.println(prop + ":" + bytesMessage.getStringProperty(prop));
				if (prop.equals("FileName")) {
					filename += bytesMessage.getStringProperty(prop);
					nameGiven = true;
				}
			}
			if (!nameGiven) {
				filename += consumerName + "_" + messageIndex;
			}
			FileOutputStream outFile = new FileOutputStream(filename);

			byte[] messageBytes = new byte[(int)bytesMessage.getBodyLength()];
			bytesMessage.readBytes(messageBytes);
			outFile.write(messageBytes);
			System.out.println(consumerName + ":" + filename + " written");
			outFile.close();
		} catch (JMSException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
