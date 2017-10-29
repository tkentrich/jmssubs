package kent.jms;

import java.io.File;
import java.io.RandomAccessFile;
import java.io.FileNotFoundException;
import java.io.IOException;

import java.util.Arrays;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.Message;
import javax.jms.BytesMessage;
import javax.jms.MessageProducer;
import javax.jms.JMSException;

import org.apache.activemq.ActiveMQConnectionFactory;

public class FilePublisherClient {
	public static void main(String[] args) throws JMSException {
		Connection connection = null;
		try {
			ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
			connection = connectionFactory.createConnection();
			Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

			String topicName = "fileTopic";
			boolean argsDone = false;
			boolean verbose = false;

			String[] filenames = {};

			for (int i = 0; i < args.length; i++) {
				if (args[i].equals("-t")) {
					i++;
					if (i < args.length) {
						topicName = args[i];
					}
				} else if (args[i].startsWith("--topic=")) {
					topicName = args[i].substring(9);
				} else if (args[i].equals("-v")) {
					verbose = true;
				} else {
					argsDone = true;
				}
				if (argsDone) {
					filenames = Arrays.copyOfRange(args, i, args.length);
					break;
				}
			}

			Topic topic = session.createTopic(topicName);
			MessageProducer producer = session.createProducer(topic);

			for (String fn : filenames) {
				try {
					File fileObj = new File(fn);
					if (fileObj.exists() && fileObj.canRead()) {
						RandomAccessFile file = new RandomAccessFile(fileObj, "r");
						BytesMessage msg = session.createBytesMessage();
						byte[] b = new byte[(int)file.length()];
						file.readFully(b);

						msg.writeBytes(b);
						msg.setStringProperty("SourceFile", fn);
						msg.setStringProperty("FileName", fileObj.getName());
						if (verbose) {
							System.out.println("Sending file: " + fn);
						}
						producer.send(msg);
					}
				} catch (IOException e) {
				}

				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
				}
			}
	
			session.close();
		} finally {
			connection.close();
		}
	}
}
