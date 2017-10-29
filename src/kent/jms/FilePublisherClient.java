package kent.jms;

import java.io.File;
import java.io.RandomAccessFile;
import java.io.FileNotFoundException;
import java.io.IOException;

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
			Topic topic = session.createTopic("fileTopic");
	
			MessageProducer producer = session.createProducer(topic);

			// String[] filenames = {"/tmp/File1", "/tmp/File2"};

			for (String fn : args) {
				try {
					RandomAccessFile file = new RandomAccessFile(fn, "r");
					BytesMessage msg = session.createBytesMessage();
					byte[] b = new byte[(int)file.length()];
					file.readFully(b);

					msg.writeBytes(b);
					msg.setStringProperty("Name", fn);

					producer.send(msg);
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
