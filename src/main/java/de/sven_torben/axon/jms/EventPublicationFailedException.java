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

import java.io.Serializable;

import org.axonframework.common.AxonException;

/**
 * Exception indication that an error occurred while publishing an event to a JMS Message Broker.
 *
 * @author Sven-Torben Janus
 */
public class EventPublicationFailedException extends AxonException implements Serializable {

  private static final long serialVersionUID = 5808422847604083264L;

  /**
   * Initialize the exception using given descriptive {@code message} and {@code cause}.
   *
   * @param message A message describing the exception
   * @param cause   The exception describing the cause of the failure
   */
  public EventPublicationFailedException(String message, Throwable cause) {
    super(message, cause);
  }
}
