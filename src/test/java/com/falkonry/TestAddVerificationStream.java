package com.falkonry;

import com.falkonry.client.Falkonry;
import com.falkonry.helper.models.*;
import org.apache.commons.io.FileUtils;
import org.junit.*;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.*;

/*!
 * falkonry-java-client
 * Copyright(c) 2016 Falkonry Inc
 * MIT Licensed
 */

public class TestAddVerificationStream {
    Falkonry falkonry = null;
    String host = "http://localhost:8080";
    String token = "";             //auth token
    List<Eventbuffer> eventbuffers = new ArrayList<Eventbuffer>();
    List<Pipeline> pipelines = new ArrayList<Pipeline>();

    @Before
    public void setUp() throws Exception {
        falkonry = new Falkonry(host, token);
    }

    //@Test
    public void createPipelineWithCsvVerificationStream() throws Exception {
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

        File file = new File("res/verificationData.csv");
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(FileUtils.readFileToByteArray(file));
        String response = falkonry.addVerificationStream(pl.getId(),byteArrayInputStream, null);
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

        File file = new File("res/verificationData.json");
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(FileUtils.readFileToByteArray(file));
        String response = falkonry.addVerificationStream(pl.getId(),byteArrayInputStream, null);
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

