package kent.jms;

import java.io.File;

import java.util.Arrays;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.JMSException;

import org.apache.activemq.ActiveMQConnectionFactory;

public class FileSubscriberClient {
	public static void main(String[] args) throws JMSException {
		Connection connection = null;

		boolean argsDone = false;
		boolean verbose = false;
		String[] topics = {};
		String destination = "/tmp";

		// Parse Arguments
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-v")) {
				verbose = true;
			} else {
				argsDone = true;
			}
			if (argsDone) {
				topics = Arrays.copyOfRange(args, i, args.length);
				break;
			}
		}

		ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
		connection = connectionFactory.createConnection();
		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

		for (String topicName : topics) {
			if (verbose) {
				System.out.println("Subscribing to topic: " + topicName);
			}
			Topic topic = session.createTopic(topicName);

			MessageConsumer consumer = session.createConsumer(topic);
			consumer.setMessageListener(new FileConsumerMessageListener(topicName, new File(destination)));
		}

		connection.start();
	}
}
