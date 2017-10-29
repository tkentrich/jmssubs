package kent.jms;

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
		try {
			ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
			connection = connectionFactory.createConnection();
			Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			Topic topic = session.createTopic("fileTopic");
	
			MessageConsumer consumer = session.createConsumer(topic);
			consumer.setMessageListener(new FileConsumerMessageListener("Consumer1"));

			connection.start();
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
			}
			// session.close();
		} finally {
			// connection.close();
		}
	}
}
