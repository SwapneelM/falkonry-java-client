package com.falkonry;

/*!
 * falkonry-java-client
 * Copyright(c) 2016 Falkonry Inc
 * MIT Licensed
 */

import com.falkonry.helper.models.*;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TestEntityModel {

  @Test
  public void eventbufferModel()throws Exception{
    Eventbuffer eb = new Eventbuffer();
    eb.setName("Test-EB");
    eb.setTimeIdentifier("time");
    eb.setTimeFormat("iso_8601");
    eb.setValueColumn("value");
    eb.setSignalsDelimiter("_");
    eb.setSignalsLocation("prefix");
    eb.setSignalsTagField("tag");

    Assert.assertEquals(eb.getName(),"Test-EB");
    Assert.assertEquals("time", eb.getTimeIdentifier());
    Assert.assertEquals("iso_8601", eb.getTimeFormat());

    Assert.assertEquals("value", eb.getValueColumn());
    Assert.assertEquals("_", eb.getSignalsDelimiter());
    Assert.assertEquals("tag", eb.getSignalsTagField());
    Assert.assertEquals("prefix", eb.getSignalsLocation());
  }

  @Test
  public void pipelineModelWithSingleThingWithDefaults(){
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

    Interval interval = new Interval();
    interval.setDuration("PT1S");

    Pipeline pipeline = new Pipeline();
    pipeline.setName("Motor Health");
    pipeline.setEventbuffer("eventbuffer-id");
    pipeline.setInputList(signals);
    pipeline.setAssessmentList(assessments);
    pipeline.setInterval(interval);

    Assert.assertEquals(pipeline.getName(),"Motor Health");
    Assert.assertEquals(pipeline.getEventbuffer(),"eventbuffer-id");
    Assert.assertEquals(pipeline.getInputList().size(),1);
    Assert.assertEquals(pipeline.getAssessmentList().size(),1);
    Assert.assertEquals(pipeline.getInterval().getDuration(),"PT1S");

  }

  @Test
  public void pipelineModelWithMultipleThingsWithOverrides(){
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

    Interval interval = new Interval();
    interval.setDuration("PT1S");

    Pipeline pipeline = new Pipeline();
    pipeline.setName("Motor Health");
    pipeline.setEventbuffer("eventbuffer-id");
    pipeline.setInputList(signals);
    pipeline.setAssessmentList(assessments);
    pipeline.setInterval(interval);

    Assert.assertEquals(pipeline.getName(),"Motor Health");
    Assert.assertEquals(pipeline.getEventbuffer(),"eventbuffer-id");
    Assert.assertEquals(pipeline.getInputList().size(),1);
    Assert.assertEquals(pipeline.getAssessmentList().size(),1);
    Assert.assertEquals(pipeline.getInterval().getDuration(),"PT1S");
  }

  @Test
  public void signalModel(){
    Signal signal = new Signal();
    signal.setName("current").setValueType(new ValueType().setType("Numeric"))
        .setEventType(new EventType().setType("Samples"));

    Assert.assertEquals(signal.getName(),"current");
    Assert.assertEquals(signal.getValueType().getType(),"Numeric");
    Assert.assertEquals(signal.getEventType().getType(),"Samples");
  }

  @Test
  public void assessmentModel(){
    List<String> inputList = new ArrayList<String>();
    inputList.add("current");
    inputList.add("vibration");
    inputList.add("state");

    Assessment assessment = new Assessment();
    assessment.setName("Health");
    assessment.setInputList(inputList);

    Assert.assertEquals(assessment.getName(),"Health");
    Assert.assertEquals(assessment.getInputList().size(),3);

  }

  @Test
  public void subscriptionModel()
  {
    Subscription sub = new Subscription();
    sub.setType("MQTT")
        .setPath("mqtt://test.mosquito.com")
        .setTopic("falkonry-eb-1-test")
        .setUsername("test-user")
        .setPassword("test");

    Assert.assertEquals(sub.getType(),"MQTT");
    Assert.assertEquals(sub.getTopic(),"falkonry-eb-1-test");
    Assert.assertEquals(sub.getPath(),"mqtt://test.mosquito.com");
    Assert.assertEquals(sub.getUsername(),"test-user");

  }

  @Test
  public void publicationModel(){
    Publication publication = new Publication();
    publication.setType("MQTT")
        .setPath("mqtt://test.mosquito.com")
        .setTopic("falkonry-eb-1-test")
        .setUsername("test-user")
        .setPassword("test")
        .setContentType("application/json");

    Assert.assertEquals(publication.getType(),"MQTT");
    Assert.assertEquals(publication.getPath(),"mqtt://test.mosquito.com");
    Assert.assertEquals(publication.getTopic(),"falkonry-eb-1-test");
    Assert.assertEquals(publication.getUsername(),"test-user");
    Assert.assertEquals(publication.getContentType(),"application/json");
  }
}
