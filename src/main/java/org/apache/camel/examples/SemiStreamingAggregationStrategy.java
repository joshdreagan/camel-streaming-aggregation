/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.camel.examples;

import java.util.Comparator;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.camel.CamelContext;
import org.apache.camel.CamelContextAware;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Processor;
import org.apache.camel.RuntimeCamelException;
import org.apache.camel.processor.aggregate.AggregateProcessor;
import org.apache.camel.processor.aggregate.AggregationStrategy;
import org.apache.camel.util.ExchangeHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;

public class SemiStreamingAggregationStrategy implements AggregationStrategy, CamelContextAware, InitializingBean {

  private static final Logger log = LoggerFactory.getLogger(SemiStreamingAggregationStrategy.class);

  public static final String LAST_PROCESSED_INDEX = "CamelAggregatorLastProcessedIndex";

  private String aggregateProcessorId;
  private CamelContext camelContext;
  private String sequenceIdHeaderName;

  // Lazily initialized.
  private AggregateProcessor _aggregateProcessor;
  private Comparator<Message> _messageComparator;

  public String getAggregateProcessorId() {
    return aggregateProcessorId;
  }

  public void setAggregateProcessorId(String aggregateProcessorId) {
    this.aggregateProcessorId = aggregateProcessorId;
  }

  @Override
  public void setCamelContext(CamelContext camelContext) {
    this.camelContext = camelContext;
  }

  @Override
  public CamelContext getCamelContext() {
    return camelContext;
  }

  public String getSequenceIdHeaderName() {
    return sequenceIdHeaderName;
  }

  public void setSequenceIdHeaderName(String sequenceIdHeaderName) {
    this.sequenceIdHeaderName = sequenceIdHeaderName;
  }

  protected AggregateProcessor _aggregateProcessor() {
    if (_aggregateProcessor == null) {
      _aggregateProcessor = camelContext.getProcessor(aggregateProcessorId, AggregateProcessor.class);
    }
    return _aggregateProcessor;
  }

  protected Comparator<Message> _messageComparator() {
    if (_messageComparator == null) {
      _messageComparator = (Message t, Message t1) -> t.getHeader(sequenceIdHeaderName, Comparable.class).compareTo(t1.getHeader(sequenceIdHeaderName, Comparable.class));
    }
    return _messageComparator;
  }

  @Override
  public void afterPropertiesSet() throws Exception {
    Objects.requireNonNull(aggregateProcessorId, "The aggregateProcessorId property must not be null.");
    Objects.requireNonNull(camelContext, "The camelContext property must not be null.");
    Objects.requireNonNull(sequenceIdHeaderName, "The sequenceIdHeaderName property must not be null.");
  }

  @Override
  public Exchange aggregate(Exchange oldExchange, Exchange newExchange) {

    Exchange aggregateExchange = initializeAggregateExchange(oldExchange, newExchange);
    log.info(String.format("Pending messages: [%s] messages", aggregateExchange.getIn().getBody(SortedSet.class).size()));

    appendMessage(aggregateExchange, newExchange.getIn());

    findAndEmitSequencedMessages(aggregateExchange);

    return aggregateExchange;
  }

  protected Exchange initializeAggregateExchange(Exchange oldExchange, Exchange newExchange) {

    Exchange aggregateExchange;
    if (oldExchange == null) {
      aggregateExchange = ExchangeHelper.copyExchangeAndSetCamelContext(newExchange, camelContext);
      SortedSet<Message> pendingMessages = new TreeSet<>(_messageComparator());
      aggregateExchange.getIn().setBody(pendingMessages);
      aggregateExchange.setProperty(LAST_PROCESSED_INDEX, -1L);
    } else {
      aggregateExchange = oldExchange;
    }

    return aggregateExchange;
  }

  protected void appendMessage(Exchange aggregateExchange, Message message) {
    log.info(String.format("Adding message: index [%s], body [%s]", message.getHeader(sequenceIdHeaderName), message.getBody()));
    aggregateExchange.getIn().getBody(SortedSet.class).add(message);
  }

  protected void findAndEmitSequencedMessages(Exchange aggregateExchange) {

    SortedSet<Message> pendingMessages = aggregateExchange.getIn().getBody(SortedSet.class);
    Long lastProcessedIndex = aggregateExchange.getProperty(LAST_PROCESSED_INDEX, Long.class);

    Message currentMessage;
    Long currentMessageIndex;
    SortedSet<Message> messagesToBeEmitted = new TreeSet<>(_messageComparator());
    do {
      currentMessage = pendingMessages.first();
      currentMessageIndex = currentMessage.getHeader(sequenceIdHeaderName, Long.class);
      if (currentMessageIndex == lastProcessedIndex + 1) {
        messagesToBeEmitted.add(currentMessage);
        pendingMessages.remove(currentMessage);
        lastProcessedIndex = currentMessageIndex;
      } else {
        break;
      }
    } while (!pendingMessages.isEmpty());
    if (!messagesToBeEmitted.isEmpty()) {
      log.info(String.format("Messages to be emitted: [%s] messages", messagesToBeEmitted.size()));
      aggregateExchange.setProperty(LAST_PROCESSED_INDEX, lastProcessedIndex);
      aggregateExchange.getIn().setBody(pendingMessages);
      Exchange exchangeToBeEmitted = ExchangeHelper.copyExchangeAndSetCamelContext(aggregateExchange, camelContext);
      exchangeToBeEmitted.getIn().setBody(messagesToBeEmitted);
      try {
        for (Processor processor : _aggregateProcessor().next()) {
          processor.process(exchangeToBeEmitted);
        }
      } catch (Exception e) {
        throw new RuntimeCamelException(e);
      }
    }
  }
}
