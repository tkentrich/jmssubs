package kent.jms;

import java.util.Enumeration;

import java.io.IOException;
import java.io.FileOutputStream;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.BytesMessage;

public class FileConsumerMessageListener implements MessageListener {
	private String consumerName;
	private int messageIndex;

	public FileConsumerMessageListener(String consumerName) {
		this.consumerName = consumerName;
		this.messageIndex = 1;
	}
	public void onMessage(Message message) {
		BytesMessage bytesMessage = (BytesMessage) message;
		try {
			System.out.println(consumerName + " received a file");
			String filename = "/tmp/file" + consumerName + messageIndex++;
			System.out.println("Message properties:");
			//Enumeration<String> propNames = bytesMessage.getPropertyNames();
			//for (String prop : propNames) {
			for (Enumeration e = bytesMessage.getPropertyNames(); e.hasMoreElements(); ) {
				String prop = e.nextElement().toString();
				System.out.println(prop + ":" + bytesMessage.getStringProperty(prop));
				if (prop.equals("Name")) {
					filename += "_" + bytesMessage.getStringProperty(prop);
				}
			}
			// File outFile = new File(filename, "w");
			FileOutputStream outFile = new FileOutputStream(filename);

			byte[] messageBytes = new byte[(int)bytesMessage.getBodyLength()];
			bytesMessage.readBytes(messageBytes);
			outFile.write(messageBytes);
			System.out.println(filename + " written");
			outFile.close();
		} catch (JMSException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
