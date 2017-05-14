package de.sven_torben.axon.jms;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TemporaryTopic;
import javax.jms.TopicConnection;
import javax.jms.TopicSession;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.junit.EmbeddedActiveMQBroker;
import org.axonframework.eventhandling.GenericEventMessage;
import org.axonframework.eventhandling.SimpleEventBus;
import org.axonframework.serialization.xml.XStreamSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class JmsIntegrationTest {

  @Rule
  public EmbeddedActiveMQBroker embeddedBroker = new EmbeddedActiveMQBroker();

  private SimpleEventBus out;
  private SimpleEventBus in;
  private JmsPublisher publisher;
  private JmsMessageSource jmsMessageSource;
  private MessageConsumer consumer;
  private TopicConnection topicConnection;

  @Before
  public void setUp() throws Exception {
    embeddedBroker.start();
    out = new SimpleEventBus();
    in = new SimpleEventBus();

    ActiveMQConnectionFactory connectionFactory = embeddedBroker.createConnectionFactory();
    topicConnection = connectionFactory.createTopicConnection();
    topicConnection.start();
    TopicSession topicSession = topicConnection.createTopicSession(
        true, Session.SESSION_TRANSACTED);
    TemporaryTopic topic = topicSession.createTemporaryTopic();
    consumer = topicSession.createConsumer(topic);

    publisher = new JmsPublisher(out);
    publisher.setTopic(topic);
    publisher.setConnectionFactory(connectionFactory);
    publisher.postConstruct();
    publisher.start();

    jmsMessageSource = new JmsMessageSource(this.consumer,
        new DefaultJmsMessageConverter(new XStreamSerializer()));
    jmsMessageSource.subscribe(in::publish);
  }

  @After
  public void tearDown() throws JMSException {
    publisher.shutDown();
    topicConnection.stop();
    embeddedBroker.stop();
  }

  @Test
  public void test() throws InterruptedException {
    final GenericEventMessage<String>[] myMessage = new GenericEventMessage[1];
    in.subscribe(eventMessages -> myMessage[0] =
        (GenericEventMessage<String>) eventMessages.get(0));
    out.publish(new GenericEventMessage<String>("MyMessage"));
    Thread.sleep(1000);
    assertThat(myMessage[0].getPayload(), is(equalTo("MyMessage")));
  }

}
