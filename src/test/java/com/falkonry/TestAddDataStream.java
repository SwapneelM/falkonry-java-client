package com.falkonry;

import com.falkonry.client.Falkonry;
import com.falkonry.helper.models.Eventbuffer;
import com.falkonry.helper.models.InputStatus;
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


public class TestAddDataStream {
  Falkonry falkonry = null;
  String host = "http://localhost:8080";
  String token = "";      //auth token
  List<Eventbuffer> eventbuffers = new ArrayList<Eventbuffer>();

  @Before
  public void setUp() throws Exception {
    falkonry = new Falkonry(host, token);
  }

  //@Test
  public void addDataJsonStream() throws Exception {
    Eventbuffer eb = new Eventbuffer();
    eb.setName("Test-EB-"+Math.random());
    eb.setTimeIdentifier("time");
    eb.setTimeFormat("iso_8601");
    eb.setValueColumn("value");
    eb.setSignalsDelimiter("_");
    eb.setSignalsLocation("prefix");
    eb.setSignalsTagField("tag");
    Map<String, String> options = new HashMap<String, String>();
    Eventbuffer eventbuffer = falkonry.createEventbuffer(eb);
    eventbuffers.add(eventbuffer);
    File file = new File("res/data.json");
    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(FileUtils.readFileToByteArray(file));
    InputStatus inputStatus = falkonry.addInputStream(eventbuffer.getId(),byteArrayInputStream,options);
    eventbuffer = falkonry.getUpdatedEventbuffer(eventbuffer.getId());
    Assert.assertEquals(1,eventbuffer.getSchemaList().size());
  }

  //@Test
  public void addWideDataJsonStream() throws Exception {
    Eventbuffer eb = new Eventbuffer();
    eb.setName("Test-EB-"+Math.random());
    eb.setTimeIdentifier("time");
    eb.setTimeFormat("millis");
    eb.setThingIdentifier("thing");
    Map<String, String> options = new HashMap<String, String>();
    Eventbuffer eventbuffer = falkonry.createEventbuffer(eb);
    eventbuffers.add(eventbuffer);
    File file = new File("res/data_wide.json");
    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(FileUtils.readFileToByteArray(file));
    InputStatus inputStatus = falkonry.addInputStream(eventbuffer.getId(),byteArrayInputStream,options);
    eventbuffer = falkonry.getUpdatedEventbuffer(eventbuffer.getId());
    Assert.assertEquals(1,eventbuffer.getSchemaList().size());
  }

  //@Test
  public void addDataCsvStream() throws Exception {
    Eventbuffer eb = new Eventbuffer();
    eb.setName("Test-EB-"+Math.random());
    eb.setTimeIdentifier("time");
    eb.setTimeFormat("iso_8601");
    eb.setValueColumn("value");
    eb.setSignalsDelimiter("_");
    eb.setSignalsLocation("prefix");
    eb.setSignalsTagField("tag");
    Map<String, String> options = new HashMap<String, String>();
    Eventbuffer eventbuffer = falkonry.createEventbuffer(eb);
    eventbuffers.add(eventbuffer);
    File file = new File("res/data.csv");
    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(FileUtils.readFileToByteArray(file));
    InputStatus inputStatus = falkonry.addInputStream(eventbuffer.getId(),byteArrayInputStream,options);
    eventbuffer = falkonry.getUpdatedEventbuffer(eventbuffer.getId());
    Assert.assertEquals(1,eventbuffer.getSchemaList().size());
  }

  //@Test
  public void addWideDataCsvStream() throws Exception {
    Eventbuffer eb = new Eventbuffer();
    eb.setName("Test-EB-"+Math.random());
    eb.setTimeIdentifier("time");
    eb.setTimeFormat("millis");
    eb.setThingIdentifier("thing");
    Map<String, String> options = new HashMap<String, String>();
    Eventbuffer eventbuffer = falkonry.createEventbuffer(eb);
    eventbuffers.add(eventbuffer);
    File file = new File("res/data_wide.csv");
    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(FileUtils.readFileToByteArray(file));
    InputStatus inputStatus = falkonry.addInputStream(eventbuffer.getId(),byteArrayInputStream,options);
    eventbuffer = falkonry.getUpdatedEventbuffer(eventbuffer.getId());
    Assert.assertEquals(1,eventbuffer.getSchemaList().size());
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
