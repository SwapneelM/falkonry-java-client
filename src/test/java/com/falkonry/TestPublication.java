package com.falkonry;

import com.falkonry.client.Falkonry;
import com.falkonry.helper.models.*;
import org.junit.*;

import java.util.*;

/*!
 * falkonry-java-client
 * Copyright(c) 2016 Falkonry Inc
 * MIT Licensed
 */

public class TestPublication {
  Falkonry falkonry = null;
  String host = "http://localhost:8080";
  String token = "";          //auth token
  List<Eventbuffer> eventbuffers = new ArrayList<Eventbuffer>();
  List<Pipeline> pipelines = new ArrayList<Pipeline>();

  @Before
  public void setUp() throws Exception {
    falkonry = new Falkonry(host, token);
  }

  //@Test
  public void createPublication() throws Exception {
    Eventbuffer eb = new Eventbuffer();
    eb.setName("Test-EB-"+Math.random());
    eb.setTimeIdentifier("time");
    eb.setTimeFormat("iso_8601");
    eb.setValueColumn("value");
    eb.setSignalsDelimiter("_");
    eb.setSignalsLocation("prefix");
    eb.setSignalsTagField("tag");

    List<Signal> signals = new ArrayList<Signal>();
    signals.add(new Signal().setName("signal1").setValueType(new ValueType().setType("Numeric"))
            .setEventType(new EventType().setType("Samples")));

    List<String> inputList = new ArrayList<String>();
    inputList.add("signal1");

    List<Assessment> assessments = new ArrayList<Assessment>();
    Assessment assessment = new Assessment();
    assessment.setName("Health");
    assessment.setInputList(inputList);
    assessments.add(assessment);

    Map<String, String> options = new HashMap<String, String>();

    Eventbuffer eventbuffer = falkonry.createEventbuffer(eb);
    eventbuffers.add(eventbuffer);

    String data = "time, tag, value\n2016-03-01 01:01:01, signal1_thing1, 3.4";
    falkonry.addInput(eventbuffer.getId(), data, options);
    Interval interval = new Interval();
    interval.setDuration("PT1S");

    Pipeline pipeline = new Pipeline();
    String name = "Test-PL-" + Math.random();
    pipeline.setName(name);
    pipeline.setEventbuffer(eventbuffer.getId());
    pipeline.setInputList(signals);
    pipeline.setAssessmentList(assessments);
    pipeline.setInterval(interval);

    Pipeline pl = falkonry.createPipeline(pipeline);

    Publication publication = new Publication();
    publication.setType("MQTT")
        .setPath("mqtt://test.mosquito.com")
        .setTopic("falkonry-eb-1-test")
        .setUsername("test-user")
        .setPassword("test")
        .setContentType("application/json");

    Publication pb = falkonry.createPublication(pl.getId(),publication);
    Assert.assertEquals(pb.getUsername(),publication.getUsername());
    falkonry.deletePublication(pl.getId(),pb.getKey());
    falkonry.deletePipeline(pl.getId());
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
