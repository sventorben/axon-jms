/*
 * Copyright (c) 2017. Sven-Torben Janus
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package de.sven_torben.axon.jms;

import java.util.Optional;

import javax.jms.JMSException;
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
