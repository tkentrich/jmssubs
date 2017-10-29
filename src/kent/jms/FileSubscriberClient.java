package kent.jms;

import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;

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
		String defaultDestination = "/tmp";
		String subsFile = "/etc/subscribers.conf";

		// Parse Arguments
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-v")) {
				verbose = true;
			} else if (args[i].equals("-s")) {
				i++;
				if (i < args.length) {
					subsFile = args[i];
				}
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

		// Read in subs
		// for (String topicName : topics) {
		try {
			BufferedReader br = new BufferedReader(new FileReader(subsFile));
			String subLine = br.readLine();
			while (subLine != null) {
				String[] info = subLine.split(":");
				String topicName = "";
				String destination = "";
				if (info.length != 2) {
					System.err.println("WARNING: Subscription " + subLine + " has no destination. Using default" + defaultDestination);
					topicName = subLine;
					destination = defaultDestination;
				} else {
					topicName = info[0];
					destination = info[1];
				}
				if (verbose) {
					System.out.println("Subscribing to topic \"" + topicName + "\" with destination: " + destination);
				}
				Topic topic = session.createTopic(topicName);
	
				MessageConsumer consumer = session.createConsumer(topic);
				consumer.setMessageListener(new FileConsumerMessageListener(topicName, new File(destination)));
				subLine = br.readLine();
			}

			connection.start();
		} catch (FileNotFoundException e) {
			System.err.println("ERROR: Subscription Specification file " + subsFile + " not found!");
			System.exit(1);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
