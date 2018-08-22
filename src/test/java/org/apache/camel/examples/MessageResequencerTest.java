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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.camel.CamelContext;
import org.apache.camel.EndpointInject;
import org.apache.camel.Produce;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.AdviceWithRouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.model.ModelCamelContext;
import org.apache.camel.test.spring.CamelSpringJUnit4ClassRunner;
import org.apache.camel.test.spring.DisableJmx;
import org.apache.camel.test.spring.UseAdviceWith;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;

@RunWith(CamelSpringJUnit4ClassRunner.class)
@ContextConfiguration({"/META-INF/spring/applicationContext.xml"})
@DisableJmx(true)
@UseAdviceWith(true)
public class MessageResequencerTest {

  private static final Logger log = LoggerFactory.getLogger(MessageResequencerTest.class);

  @Autowired
  private CamelContext camelContext;
  
  private final AtomicBoolean adviced = new AtomicBoolean(false);

  @Produce(uri = "direct:semiStreamingResequencer")
  private ProducerTemplate producer;

  @EndpointInject(uri = "mock:semiStreamingResequencer")
  private MockEndpoint mock;

  @Before
  public void adviceResequencerRoute() throws Exception {
    if (!adviced.get()) {
      camelContext.getRouteDefinition("resequencerRoute").adviceWith(camelContext.adapt(ModelCamelContext.class), new AdviceWithRouteBuilder() {
        @Override
        public void configure() throws Exception {
          weaveById("processMessage")
            .after()
            .to(mock);
        }
      });
      camelContext.start();
      adviced.set(true);
    }
  }

  @Test
  public void testUnorderedMessages() throws Exception {
    String message = "Hello world!";
    List<Integer> scrambledIndexes = new ArrayList<>(message.length());
    for (int i = 0; i < message.length(); ++i) {
      scrambledIndexes.add(i);
    }
    Collections.shuffle(scrambledIndexes);
    
    for (Integer i : scrambledIndexes) {
      log.info(String.format("Sending message: index [%s], letter [%s]...", i, message.charAt(i)));
      Map<String, Object> headers = new HashMap<>();
      headers.put("MyGroupingID", 1);
      headers.put("MyGroupingSize", message.length());
      headers.put("MySequenceID", i);
      producer.sendBodyAndHeaders(message.charAt(i), headers);
    }
    mock.expectedMessageCount(message.length());
    mock.expectsAscending().simple("${header[MySequenceID]}");
    mock.assertIsSatisfied(30000L);
  }
}
