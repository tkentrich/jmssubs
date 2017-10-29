package kent.jms;

import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.JMSException;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.commons.daemon.*;

public class FileSubscriberDaemon implements Daemon {

	Connection connection;
	String topicName;
	String defaultDestination;
	String subsFile;
	boolean verbose;

	@Override
	public void init(DaemonContext dc) throws DaemonInitException, Exception {
		verbose = false;
		topicName = "DefaultTopic";
		defaultDestination = "/tmp";
		subsFile = "/etc/subscribers.conf";

		boolean argsDone = false;
		String[] args = dc.getArguments();
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
		}

		connection = null;
		ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
		connection = connectionFactory.createConnection();
		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

		// Read in subscriptions
		try {
			BufferedReader br = new BufferedReader(new FileReader(subsFile));
			String subLine = br.readLine();
			while (subLine != null) {
				String[] info = subLine.split(":");
				String topicName = "";
				String destination = "";
				if (info.length != 2) {
					System.err.println("WARNING: Subscription " + subLine + " has no specified destination. Using " + defaultDestination);
					destination = defaultDestination;
					topicName = subLine;
				} else {
					topicName = info[0];
					destination = info[1];
				}
				if (verbose) {
					System.out.println("Subscribing to topic " + topicName + " with destination: " + destination);
				}
				Topic topic = session.createTopic(topicName);
				MessageConsumer consumer = session.createConsumer(topic);
				consumer.setMessageListener(new FileConsumerMessageListener(topicName, new File(destination)));
			}
		} catch (FileNotFoundException e) {
			System.err.println("ERROR: Subscription Specification file not found");
			System.exit(1);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void start() {
		if (connection != null) {
			try {
				connection.start();
			} catch (JMSException e) {
			}
		}
	}

	@Override
	public void stop() {
		if (connection != null) {
			try {
				connection.close();
			} catch (JMSException e) {
			}
		}
	}

	@Override
	public void destroy() {
		if (connection != null) {
			connection = null;
		}
	}
}
