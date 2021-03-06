package de.sven_torben.axon.jms;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.function.Consumer;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.junit.EmbeddedActiveMQBroker;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventhandling.GenericEventMessage;
import org.axonframework.messaging.MetaData;
import org.axonframework.serialization.xml.XStreamSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class JmsMessageSourceTest {

  @Rule
  public EmbeddedActiveMQBroker embeddedBroker = new EmbeddedActiveMQBroker();

  private JmsMessageSource cut;

  private TopicPublisher publisher;

  private TopicSession topicSession;

  private MessageConsumer consumer;

  private TopicConnection topicConnection;

  private final DefaultJmsMessageConverter converter = 
          new DefaultJmsMessageConverter(new XStreamSerializer());

  @Before
  public void setup() throws JMSException {

    final ActiveMQConnectionFactory connectionFactory = embeddedBroker.createConnectionFactory();
    topicConnection = connectionFactory.createTopicConnection();

    topicSession = topicConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);

    embeddedBroker.start();
    topicConnection.start();

    final Topic topic = topicSession.createTemporaryTopic();

    publisher = topicSession.createPublisher(topic);
    consumer = topicSession.createConsumer(topic);

    cut = new JmsMessageSource(consumer, converter);
  }

  @After
  public void tearDown() throws JMSException {
    topicConnection.stop();
    embeddedBroker.stop();
  }

  @Test
  public void messageGetsPublished() throws JMSException, InterruptedException {
    final TestConsumer testConsumer = new TestConsumer();
    cut.subscribe(testConsumer);

    EventMessage<?> eventMessage = GenericEventMessage
            .asEventMessage("SomePayload")
            .withMetaData(MetaData.with("key", "value"));

    Message jmsMessage = converter.createJmsMessage(eventMessage, topicSession);

    publisher.publish(jmsMessage);

    Thread.sleep(1000L);

    assertNotNull(testConsumer.latest);
  }

  @Test
  public void containsTheMessage() throws JMSException, InterruptedException {
    final TestConsumer testConsumer = new TestConsumer();
    cut.subscribe(testConsumer);

    EventMessage<?> eventMessage = GenericEventMessage
            .asEventMessage("SomePayload")
            .withMetaData(MetaData.with("key", "value"));

    Message jmsMessage = converter.createJmsMessage(eventMessage, topicSession);

    publisher.publish(jmsMessage);

    Thread.sleep(1000L);

    assertTrue(testConsumer.latest.get(0)
            .getMetaData().get("key").equals("value"));
  }

  private class TestConsumer implements Consumer<List<? extends EventMessage<?>>> {

    private List<? extends EventMessage<?>> latest;

    @Override
    public void accept(List<? extends EventMessage<?>> messages) {
      this.latest = messages;
    }
  }

}
