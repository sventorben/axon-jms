package de.sven_torben.axon.jms;

import java.util.Optional;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.axonframework.eventhandling.EventMessage;

/**
 * Interface describing a mechanism that converts a JMS Message from an Axon Message and vice versa.
 *
 * @author Sven-Torben Janus
 */
public interface JmsMessageConverter {

  /**
   * Creates a JmsMessage from given {@code eventMessage}.
   *
   * @param eventMessage The EventMessage to create the JMS Message from
   * @param session      The JMS Session used to create the JMS Message
   * @return a JMS Message containing the data and characteristics (properties) of the Message
    to send to the JMS Message Broker.
   */
  TextMessage createJmsMessage(EventMessage<?> eventMessage, Session session) throws JMSException;

  /**
   * Reconstruct an EventMessage from the given {@code jmsMessage}. The returned optional
   * resolves to a message if the given input parameters represented a correct event message.
   *
   * @param jmsMessage The JMS Message
   * @return The Event Message to publish on the local event processors
   */
  Optional<EventMessage<?>> readJmsMessage(TextMessage jmsMessage) throws JMSException;
}
