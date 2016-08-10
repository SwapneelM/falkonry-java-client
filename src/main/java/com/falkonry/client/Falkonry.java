package com.falkonry.client;

/*!
 * falkonry-java-client
 * Copyright(c) 2016 Falkonry Inc
 * MIT Licensed
 */

import com.falkonry.client.service.FalkonryService;
import com.falkonry.helper.models.*;

import javax.security.auth.callback.Callback;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.Map;
import java.util.Observer;


public class Falkonry {

  private FalkonryService falkonryService;

  public Falkonry(String host, String token) throws Exception {
    this.falkonryService = new FalkonryService(host, token);
  }

  public Eventbuffer createEventbuffer(Eventbuffer eventbuffer) throws Exception {
    return falkonryService.createEventbuffer(eventbuffer);
}

  public List<Eventbuffer> getEventbuffers() throws Exception {
    return falkonryService.getEventbuffers();
  }

  public Eventbuffer getUpdatedEventbuffer(String id) throws Exception{
    return falkonryService.getUpdatedEventbuffer(id);
  }

  public void deleteEventbuffer(String eventbuffer) throws Exception {
    falkonryService.deleteEventbuffer(eventbuffer);
  }

  public Pipeline createPipeline(Pipeline pipeline) throws Exception {
    return falkonryService.createPipeline(pipeline);
  }

  public List<Pipeline> getPipelines() throws Exception {
    return falkonryService.getPipelines();
  }

  public void deletePipeline(String pipeline) throws Exception {
    this.falkonryService.deletePipeline(pipeline);
  }

  public InputStatus addInput(String eventbuffer, String data, Map<String, String> options) throws Exception {
    return this.falkonryService.addInputData(eventbuffer, data, options);
  }

  public String addVerification(String pipeline,  String data, Map<String, String> options) throws Exception{
    return this.falkonryService.addVerification(pipeline, data, options);
  }

  public InputStatus addInputStream(String eventbuffer, ByteArrayInputStream stream, Map<String, String> options) throws Exception {
    return this.falkonryService.addInputFromStream(eventbuffer, stream, options);
  }

  public String addVerificationStream(String pipeline, ByteArrayInputStream stream, Map<String, String> options) throws Exception{
    return this.falkonryService.addVerificationStream(pipeline,stream,options);
  }

  public BufferedReader getOutput(String pipeline, Long start, Long end) throws Exception {
    return this.falkonryService.getOutput(pipeline, start, end);
  }

  public Object streamOutput(String pipeline, Long start, Class obj) throws Exception {
    return this.falkonryService.streamOutput(pipeline, start, obj);
  }

  public Subscription createSubscription(String eventbuffer, Subscription subscription) throws Exception {
    return falkonryService.createSubscription(eventbuffer, subscription);
  }

  public Subscription updateSubscription(String eventbuffer, Subscription subscription) throws Exception {
    return falkonryService.updateSubscription(eventbuffer, subscription);
  }

  public void deleteSubscription(String eventbuffer, String subscription) throws Exception {
    falkonryService.deleteSubscription(eventbuffer, subscription);
  }

  public Publication createPublication(String pipeline, Publication publication) throws Exception {
    return falkonryService.createPublication(pipeline, publication);
  }

  public Publication updatePublication(String pipeline, Publication publication) throws Exception {
    return falkonryService.updatePublication(pipeline, publication);
  }

  public void deletePublication(String pipeline, String publication) throws Exception {
    falkonryService.deletePublication(pipeline, publication);
  }
}
