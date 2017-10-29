package kent.jms;

import java.io.File;

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
	String destination;

	@Override
	public void init(DaemonContext dc) throws DaemonInitException, Exception {
		topicName = "DefaultTopic";
		destination = "/tmp";
		for (String s : dc.getArguments()) {
			topicName = s;
		}
		connection = null;
		ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
		connection = connectionFactory.createConnection();
		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		Topic topic = session.createTopic(topicName);
		MessageConsumer consumer = session.createConsumer(topic);
		consumer.setMessageListener(new FileConsumerMessageListener(topicName, new File(destination)));
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
