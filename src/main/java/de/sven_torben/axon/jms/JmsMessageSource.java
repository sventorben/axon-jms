/*
 * The MIT License
 *
 * Copyright 2017 Sven-Torben Janus.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package de.sven_torben.axon.jms;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.TextMessage;
import org.axonframework.common.Registration;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.messaging.SubscribableMessageSource;

public class JmsMessageSource implements MessageListener, SubscribableMessageSource {

  private final List<Consumer<List<? extends EventMessage<?>>>> eventProcessors = new CopyOnWriteArrayList<>();
  private JmsMessageConverter converter;

  public JmsMessageSource(MessageConsumer consumer,
          JmsMessageConverter converter) {
    try {
      this.converter = converter;
      consumer.setMessageListener(this);
    } catch (JMSException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public Registration subscribe(Consumer cnsmr) {
    eventProcessors.add(cnsmr);
    return () -> eventProcessors.remove(cnsmr);
  }

  @Override
  public void onMessage(Message msg) {
    try {
      converter.readJmsMessage(msg)
              .map(Collections::singletonList)
              .ifPresent(this::publish);
    } catch (JMSException ex) {
      throw new RuntimeException(ex);
    }
  }

  private void publish(List<? extends EventMessage<?>> events) {
    eventProcessors.forEach(p -> p.accept(events));
  }

}
