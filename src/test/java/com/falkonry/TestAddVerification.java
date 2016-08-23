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

public class TestAddVerification {
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
    public void createPipelineWithCsvVerification() throws Exception {

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
        pipeline.setName(name)
                .setEventbuffer(eventbuffer.getId())
                .setInputList(signals)
                .setAssessmentList(assessments)
                .setInterval(interval);
        Pipeline pl = falkonry.createPipeline(pipeline);

        data = "time,end,thing,Health\n2011-03-31T00:00:00Z,2011-04-01T00:00:00Z,thing1,Normal\n2011-03-31T00:00:00Z,2011-04-01T00:00:00Z,thing1,Normal";
        String response = falkonry.addVerification(pl.getId(), data, null);
        Assert.assertEquals(response,"{\"message\":\"Data submitted successfully\"}");
        falkonry.deletePipeline(pl.getId());
    }

    //@Test
    public void createPipelineWithWideCsvVerification() throws Exception {

        Eventbuffer eb = new Eventbuffer();
        eb.setName("Test-EB-"+Math.random());
        eb.setTimeIdentifier("time");
        eb.setTimeFormat("iso_8601");
        eb.setThingIdentifier("thing");

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

        String data = "time, tag, thing, signal1, signal2, signal3\n2016-03-01 01:01:01, signal1_thing1, thing1, 3.4, 4.8, 8.3";
        falkonry.addInput(eventbuffer.getId(), data, options);

        Interval interval = new Interval();
        interval.setDuration("PT1S");

        Pipeline pipeline = new Pipeline();
        String name = "Test-PL-" + Math.random();
        pipeline.setName(name)
                .setEventbuffer(eventbuffer.getId())
                .setInputList(signals)
                .setAssessmentList(assessments)
                .setInterval(interval);
        Pipeline pl = falkonry.createPipeline(pipeline);

        data = "time,end,thing,Health\n2011-03-31T00:00:00Z,2011-04-01T00:00:00Z,thing1,Normal\n2011-03-31T00:00:00Z,2011-04-01T00:00:00Z,thing1,Normal";
        String response = falkonry.addVerification(pl.getId(), data, null);
        Assert.assertEquals(response,"{\"message\":\"Data submitted successfully\"}");
        falkonry.deletePipeline(pl.getId());
    }

    //@Test
    public void createPipelineWithJsonVerification() throws Exception {
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

        String data = "{\"time\" : \"2016-03-01 01:01:01\", \"tag\" : \"signal1_thing1\", \"value\" : 3.4}";
        falkonry.addInput(eventbuffer.getId(), data, options);

        Interval interval = new Interval();
        interval.setDuration("PT1S");

        Pipeline pipeline = new Pipeline();
        String name = "Test-PL-" + Math.random();
        pipeline.setName(name)
                .setEventbuffer(eventbuffer.getId())
                .setInputList(signals)
                .setAssessmentList(assessments)
                .setInterval(interval);
        Pipeline pl = falkonry.createPipeline(pipeline);

        data = "{\"time\" : \"2011-03-26T12:00:00Z\", \"thing\" : \"thing1\", \"end\" : \"2012-06-01T00:00:00Z\", \"Health\" : \"Normal\"}";
        String response = falkonry.addVerification(pl.getId(), data, null);
        String response_id = response.split("(:)|(,)")[1];
        Assert.assertNotEquals(response_id,null);
        Assert.assertEquals(pl.getName(),pipeline.getName());
        falkonry.deletePipeline(pl.getId());
    }

    //@Test
    public void createPipelineWithWideJsonVerification() throws Exception {
        Eventbuffer eb = new Eventbuffer();
        eb.setName("Test-EB-"+Math.random());
        eb.setTimeIdentifier("time");
        eb.setTimeFormat("millis");
        eb.setThingIdentifier("thing");

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

        String data = "{\"time\":1467729675422,\"thing\":\"thing1\",\"signal1\":41.11,\"signal2\":82.34,\"signal3\":74.63,\"signal4\":4.8,\"signal5\":72.01}";
        falkonry.addInput(eventbuffer.getId(), data, options);

        Interval interval = new Interval();
        interval.setDuration("PT1S");

        Pipeline pipeline = new Pipeline();
        String name = "Test-PL-" + Math.random();
        pipeline.setName(name)
                .setEventbuffer(eventbuffer.getId())
                .setInputList(signals)
                .setAssessmentList(assessments)
                .setInterval(interval);
        Pipeline pl = falkonry.createPipeline(pipeline);

        data = "{\"time\" : \"2011-03-26T12:00:00Z\", \"thing\" : \"thing1\", \"end\" : \"2012-06-01T00:00:00Z\", \"Health\" : \"Normal\"}";
        String response = falkonry.addVerification(pl.getId(), data, null);
        String response_id = response.split("(:)|(,)")[1];
        Assert.assertNotEquals(response_id,null);
        Assert.assertEquals(pl.getName(),pipeline.getName());
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

