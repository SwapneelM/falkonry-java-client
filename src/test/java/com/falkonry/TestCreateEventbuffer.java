package com.falkonry;

import com.falkonry.client.Falkonry;
import com.falkonry.helper.models.Eventbuffer;
import com.falkonry.helper.models.Subscription;
import org.junit.*;

import java.util.*;

/*!
 * falkonry-java-client
 * Copyright(c) 2016 Falkonry Inc
 * MIT Licensed
 */

public class TestCreateEventbuffer {
  Falkonry falkonry = null;
  String host = "http://localhost:8080";
  String token = "";
  List<Eventbuffer> eventbuffers = new ArrayList<Eventbuffer>();

  @Before
  public void setUp() throws Exception {
    falkonry = new Falkonry(host, token);
  }

  //@Test
  public void createEventbuffer() throws Exception {
    Eventbuffer eb = new Eventbuffer();
    eb.setName("Test-EB-"+Math.random());
    eb.setTimeIdentifier("time");
    eb.setTimeFormat("iso_8601");
    eb.setValueColumn("value");
    eb.setSignalsDelimiter("_");
    eb.setSignalsLocation("prefix");
    eb.setSignalsTagField("tag");

    Eventbuffer eventbuffer = falkonry.createEventbuffer(eb);
    eventbuffers.add(eventbuffer);

    Assert.assertEquals(eb.getName(), eventbuffer.getName());
    Assert.assertNotEquals(null, eventbuffer.getId());
    Assert.assertEquals(1, eventbuffer.getSchemaList().size());
    Assert.assertEquals(1, eventbuffer.getSubscriptionList().size());

    Assert.assertEquals(eventbuffer.getTimeIdentifier(), eb.getTimeIdentifier());
    Assert.assertEquals(eventbuffer.getTimeFormat(), eb.getTimeFormat());

    Assert.assertEquals(eventbuffer.getValueColumn(), eb.getValueColumn());
    Assert.assertEquals(eventbuffer.getSignalsDelimiter(), eb.getSignalsDelimiter());
    Assert.assertEquals(eventbuffer.getSignalsTagField(), eb.getSignalsTagField());
    Assert.assertEquals(eventbuffer.getSignalsLocation(), eb.getSignalsLocation());
  }

  //@Test
  public void createWideEventbuffer() throws Exception {
    Eventbuffer eb = new Eventbuffer();
    eb.setName("Test-EB-"+Math.random());
    eb.setTimeIdentifier("time");
    eb.setTimeFormat("iso_8601");
    eb.setThingIdentifier("thing");

    Eventbuffer eventbuffer = falkonry.createEventbuffer(eb);
    eventbuffers.add(eventbuffer);

    Assert.assertEquals(eb.getName(), eventbuffer.getName());
    Assert.assertNotEquals(null, eventbuffer.getId());
    Assert.assertEquals(1, eventbuffer.getSchemaList().size());
    Assert.assertEquals(1, eventbuffer.getSubscriptionList().size());

    Assert.assertEquals(eventbuffer.getTimeIdentifier(), eb.getTimeIdentifier());
    Assert.assertEquals(eventbuffer.getTimeFormat(), eb.getTimeFormat());
  }

  //@Test
  public void createEventbufferWithJsonData() throws Exception {
    Eventbuffer eb = new Eventbuffer();
    eb.setName("Test-EB-"+Math.random());
    eb.setTimeIdentifier("time");
    eb.setTimeFormat("iso_8601");
    eb.setValueColumn("value");
    eb.setSignalsDelimiter("_");
    eb.setSignalsLocation("prefix");
    eb.setSignalsTagField("tag");

    Eventbuffer eventbuffer = falkonry.createEventbuffer(eb);
    eventbuffers.add(eventbuffer);

    String data = "{\"time\" : \"2016-03-01 01:01:01\", \"tag\" : \"signal1_thing1\", \"value\" : 3.4}";
    Map<String, String> options = new HashMap<String, String>();
    falkonry.addInput(eventbuffer.getId(), data, options);

    Assert.assertEquals(eb.getName(), eventbuffer.getName());
    Assert.assertNotEquals(null, eventbuffer.getId());
    Assert.assertEquals(1, eventbuffer.getSchemaList().size());
    Assert.assertEquals(1, eventbuffer.getSubscriptionList().size());

    Assert.assertEquals(eventbuffer.getTimeFormat(), eb.getTimeFormat());
    Assert.assertEquals(eventbuffer.getTimeIdentifier(), eb.getTimeIdentifier());

    Assert.assertEquals(eventbuffer.getValueColumn(), eb.getValueColumn());
    Assert.assertEquals(eventbuffer.getSignalsDelimiter(), eb.getSignalsDelimiter());
    Assert.assertEquals(eventbuffer.getSignalsTagField(), eb.getSignalsTagField());
    Assert.assertEquals(eventbuffer.getSignalsLocation(), eb.getSignalsLocation());
  }

  //@Test
  public void createEventbufferWithWideJsonData() throws Exception {
    Eventbuffer eb = new Eventbuffer();
    eb.setName("Test-EB-"+Math.random());
    eb.setThingIdentifier("thing");
    eb.setTimeIdentifier("time");
    eb.setThingIdentifier("thing");
    eb.setTimeFormat("millis");

    Eventbuffer eventbuffer = falkonry.createEventbuffer(eb);
    eventbuffers.add(eventbuffer);

    String data = "{\"time\":1467729675422,\"thing\":\"thing1\",\"signal1\":41.11,\"signal2\":82.34,\"signal3\":74.63,\"signal4\":4.8,\"signal5\":72.01}";
    Map<String, String> options = new HashMap<String, String>();
    falkonry.addInput(eventbuffer.getId(), data, options);

    Assert.assertEquals(eb.getName(), eventbuffer.getName());
    Assert.assertNotEquals(null, eventbuffer.getId());
    Assert.assertEquals(1, eventbuffer.getSchemaList().size());
    Assert.assertEquals(1, eventbuffer.getSubscriptionList().size());

    Assert.assertEquals(eventbuffer.getTimeFormat(), eb.getTimeFormat());
    Assert.assertEquals(eventbuffer.getTimeIdentifier(), eb.getTimeIdentifier());

    Assert.assertEquals(eventbuffer.getValueColumn(), eb.getValueColumn());
    Assert.assertEquals(eventbuffer.getSignalsDelimiter(), eb.getSignalsDelimiter());
    Assert.assertEquals(eventbuffer.getSignalsTagField(), eb.getSignalsTagField());
    Assert.assertEquals(eventbuffer.getSignalsLocation(), eb.getSignalsLocation());
  }

  //@Test
  public void createEventbufferWithCsvData() throws Exception {
    Eventbuffer eb = new Eventbuffer();
    eb.setName("Test-EB-"+Math.random());
    eb.setTimeIdentifier("time");
    eb.setTimeFormat("iso_8601");
    eb.setValueColumn("value");
    eb.setSignalsDelimiter("_");
    eb.setSignalsLocation("prefix");
    eb.setSignalsTagField("tag");

    Eventbuffer eventbuffer = falkonry.createEventbuffer(eb);
    eventbuffers.add(eventbuffer);

    String data = "time, tag, value\n2016-03-01 01:01:01, signal1_thing1, 3.4";
    Map<String, String> options = new HashMap<String, String>();
    falkonry.addInput(eventbuffer.getId(), data, options);

    Assert.assertEquals(eb.getName(), eventbuffer.getName());
    Assert.assertNotEquals(null, eventbuffer.getId());
    Assert.assertEquals(1, eventbuffer.getSchemaList().size());
    Assert.assertEquals(1, eventbuffer.getSubscriptionList().size());

    Assert.assertEquals(eventbuffer.getValueColumn(), eb.getValueColumn());
    Assert.assertEquals(eventbuffer.getSignalsDelimiter(), eb.getSignalsDelimiter());
    Assert.assertEquals(eventbuffer.getSignalsTagField(), eb.getSignalsTagField());
    Assert.assertEquals(eventbuffer.getSignalsLocation(), eb.getSignalsLocation());
  }

  //@Test
  public void createEventbufferWithWideCsvData() throws Exception {
    Eventbuffer eb = new Eventbuffer();
    eb.setName("Test-EB-"+Math.random());
    eb.setThingIdentifier("thing");
    eb.setTimeIdentifier("time");
    eb.setThingIdentifier("thing");
    eb.setTimeFormat("millis");

    Eventbuffer eventbuffer = falkonry.createEventbuffer(eb);
    eventbuffers.add(eventbuffer);
    String data = "time,thing,signal1,signal2,signal3,signal4,signal5\n" +
            "1467729675422,thing1,41.11,82.34,74.63,4.8,72.01";
    Map<String, String> options = new HashMap<String, String>();
    falkonry.addInput(eventbuffer.getId(), data, options);

    Assert.assertEquals(eb.getName(), eventbuffer.getName());
    Assert.assertNotEquals(null, eventbuffer.getId());
    Assert.assertEquals(1, eventbuffer.getSchemaList().size());
    Assert.assertEquals(1, eventbuffer.getSubscriptionList().size());

    Assert.assertEquals(eventbuffer.getValueColumn(), eb.getValueColumn());
    Assert.assertEquals(eventbuffer.getSignalsDelimiter(), eb.getSignalsDelimiter());
    Assert.assertEquals(eventbuffer.getSignalsTagField(), eb.getSignalsTagField());
    Assert.assertEquals(eventbuffer.getSignalsLocation(), eb.getSignalsLocation());
  }

  //@Test
  public void createEventbufferWithMqttSubscription() throws Exception {
    Eventbuffer eb = new Eventbuffer();
    eb.setName("Test-EB-"+Math.random());
    eb.setTimeIdentifier("time");
    eb.setTimeFormat("iso_8601");
    eb.setValueColumn("value");
    eb.setSignalsDelimiter("_");
    eb.setSignalsLocation("prefix");
    eb.setSignalsTagField("tag");

    Subscription sub = new Subscription();
    sub.setType("MQTT")
            .setPath("mqtt://test.mosquito.com")
            .setTopic("falkonry-eb-1-test")
            .setUsername("test-user")
            .setPassword("test");

    Eventbuffer eventbuffer = falkonry.createEventbuffer(eb);

    String data = "time,thing,signal1,signal2,signal3,signal4,signal5\n" +
            "1467729675422,thing1,41.11,82.34,74.63,4.8,72.01";
    Map<String, String> options = new HashMap<String, String>();
    falkonry.addInput(eventbuffer.getId(), data, options);

    eventbuffers.add(eventbuffer);
    Assert.assertEquals(1, eventbuffer.getSubscriptionList().size());

    Subscription subscription = falkonry.createSubscription(eventbuffer.getId(), sub);

    Assert.assertNotEquals(null, subscription.getKey());
    Assert.assertEquals(sub.getType(), subscription.getType());
    Assert.assertEquals(sub.getPath(), subscription.getPath());
    Assert.assertEquals(sub.getTopic(), subscription.getTopic());
    Assert.assertEquals(sub.getUsername(), subscription.getUsername());
    Assert.assertEquals(sub.getPassword(), subscription.getPassword());

    Assert.assertEquals(eventbuffer.getTimeIdentifier(), eb.getTimeIdentifier());
    Assert.assertEquals(eventbuffer.getTimeFormat(), eb.getTimeFormat());

    Assert.assertEquals(eventbuffer.getValueColumn(), eb.getValueColumn());
    Assert.assertEquals(eventbuffer.getSignalsDelimiter(), eb.getSignalsDelimiter());
    Assert.assertEquals(eventbuffer.getSignalsTagField(), eb.getSignalsTagField());
    Assert.assertEquals(eventbuffer.getSignalsLocation(), eb.getSignalsLocation());

    falkonry.deleteSubscription(eventbuffer.getId(),subscription.getKey());
  }

  //@Test
  public void createEventbufferWithOutflowSubscription() throws Exception {
    Eventbuffer eb = new Eventbuffer();
    eb.setName("Test-EB-"+Math.random());
    eb.setTimeIdentifier("time");
    eb.setTimeFormat("iso_8601");
    eb.setValueColumn("value");
    eb.setSignalsDelimiter("_");
    eb.setSignalsLocation("prefix");
    eb.setSignalsTagField("tag");

    Subscription sub = new Subscription();
    sub.setType("PIPELINEOUTFLOW")
            .setPath("urn:falkonry:pipeline:zmusfprsf7zspf");

    Eventbuffer eventbuffer = falkonry.createEventbuffer(eb);
    String data = "time,thing,signal1,signal2,signal3,signal4,signal5\n"
            + "1467729675422,thing1,41.11,82.34,74.63,4.8,72.01";
    Map<String, String> options = new HashMap<String, String>();
    falkonry.addInput(eventbuffer.getId(), data, options);

    eventbuffers.add(eventbuffer);

    Assert.assertEquals(1, eventbuffer.getSubscriptionList().size());

    Subscription subscription = falkonry.createSubscription(eventbuffer.getId(), sub);

    Assert.assertNotEquals(null, subscription.getKey());
    Assert.assertEquals(sub.getType(), subscription.getType());
    Assert.assertEquals(sub.getPath(), subscription.getPath());

    Assert.assertEquals(eventbuffer.getTimeIdentifier(), eb.getTimeIdentifier());
    Assert.assertEquals(eventbuffer.getTimeFormat(), eb.getTimeFormat());

    Assert.assertEquals(eventbuffer.getValueColumn(), eb.getValueColumn());
    Assert.assertEquals(eventbuffer.getSignalsDelimiter(), eb.getSignalsDelimiter());
    Assert.assertEquals(eventbuffer.getSignalsTagField(), eb.getSignalsTagField());
    Assert.assertEquals(eventbuffer.getSignalsLocation(), eb.getSignalsLocation());

    falkonry.deleteSubscription(eventbuffer.getId(),subscription.getKey());
  }

  //@Test
  public void createEventbufferWithMqttSubscriptionForNarrowFormatData() throws Exception {
    Eventbuffer eb = new Eventbuffer();
    eb.setName("Test-EB-"+Math.random());
    eb.setTimeIdentifier("time");
    eb.setTimeFormat("iso_8601");
    eb.setValueColumn("value");
    eb.setSignalsDelimiter("_");
    eb.setSignalsLocation("prefix");
    eb.setSignalsTagField("tag");

    Subscription sub = new Subscription();
    sub.setType("MQTT")
            .setPath("mqtt://test.mosquito.com")
            .setTopic("falkonry-eb-1-test")
            .setUsername("test-user")
            .setPassword("test");

    Eventbuffer eventbuffer = falkonry.createEventbuffer(eb);
    String data = "time,thing,signal1,signal2,signal3,signal4,signal5\n" +
            "1467729675422,thing1,41.11,82.34,74.63,4.8,72.01";
    Map<String, String> options = new HashMap<String, String>();
    falkonry.addInput(eventbuffer.getId(), data, options);

    eventbuffers.add(eventbuffer);
    Assert.assertEquals(1, eventbuffer.getSubscriptionList().size());

    Subscription subscription = falkonry.createSubscription(eventbuffer.getId(), sub);

    Assert.assertNotEquals(null, subscription.getKey());
    Assert.assertEquals(sub.getType(), subscription.getType());
    Assert.assertEquals(sub.getPath(), subscription.getPath());
    Assert.assertEquals(sub.getTopic(), subscription.getTopic());
    Assert.assertEquals(sub.getUsername(), subscription.getUsername());
    Assert.assertEquals(sub.getPassword(), subscription.getPassword());

    Assert.assertEquals(eventbuffer.getTimeIdentifier(), eb.getTimeIdentifier());
    Assert.assertEquals(eventbuffer.getTimeFormat(), eb.getTimeFormat());

    Assert.assertEquals(eventbuffer.getValueColumn(), eb.getValueColumn());
    Assert.assertEquals(eventbuffer.getSignalsDelimiter(), eb.getSignalsDelimiter());
    Assert.assertEquals(eventbuffer.getSignalsTagField(), eb.getSignalsTagField());
    Assert.assertEquals(eventbuffer.getSignalsLocation(), eb.getSignalsLocation());
  }

  @After
  public void cleanUp() throws Exception {
    Iterator<Eventbuffer> itr = eventbuffers.iterator();
    while(itr.hasNext()) {
      Eventbuffer eb = itr.next();
      falkonry.deleteEventbuffer(eb.getId());
    }
  }
}
