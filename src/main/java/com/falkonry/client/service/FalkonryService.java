package com.falkonry.client.service;

/*!
 * falkonry-java-client
 * Copyright(c) 2016 Falkonry Inc
 * MIT Licensed
 */

import com.falkonry.helper.models.*;
import javafx.util.Callback;
import org.apache.commons.io.IOUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FalkonryService {
  private HttpService httpService;

  public FalkonryService (String host, String token) throws Exception {
    this.httpService = new HttpService(host, token);
  }

  public Eventbuffer createEventbuffer(Eventbuffer eventbuffer) throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    Eventbuffer eb = new Eventbuffer();

    eb.setName(eventbuffer.getName());
    if(eventbuffer.getThingIdentifier()!=null)
    eb.setThingIdentifier(eventbuffer.getThingIdentifier());
    if(eventbuffer.getTimeFormat()!=null)
      eb.setTimeFormat(eventbuffer.getTimeFormat());
    if(eventbuffer.getTimeIdentifier()!=null)
      eb.setTimeIdentifier(eventbuffer.getTimeIdentifier());
    if(eventbuffer.getSignalsTagField()!=null)
      eb.setSignalsTagField(eventbuffer.getSignalsTagField());
    if(eventbuffer.getSignalsDelimiter()!=null)
      eb.setSignalsDelimiter(eventbuffer.getSignalsDelimiter());
    if(eventbuffer.getValueColumn()!=null)
      eb.setValueColumn(eventbuffer.getValueColumn());
    if(eventbuffer.getSignalsLocation()!=null)
      eb.setSignalsLocation(eventbuffer.getSignalsLocation());

    String eventbuffer_json = httpService.post("/eventbuffer", mapper.writeValueAsString(eb));
    return mapper.readValue(eventbuffer_json, Eventbuffer.class);
  }

  public List<Eventbuffer> getEventbuffers() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    String eventbuffer_json = httpService.get("/eventbuffer");
    return mapper.readValue(eventbuffer_json, new TypeReference<List<Eventbuffer>>(){});
  }

  public Eventbuffer getUpdatedEventbuffer(String id) throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    String url = "/eventbuffer/" + id;
    String eventbuffer_json = httpService.get(url);
    return mapper.readValue(eventbuffer_json,Eventbuffer.class);
  }

  public void deleteEventbuffer(String eventbuffer) throws Exception {
    httpService.delete("/eventbuffer/" + eventbuffer);
  }

  public Pipeline createPipeline(Pipeline pipeline) throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    PipelineRequest pipelineRequest = new PipelineRequest();
    List<Signal> signalList;
    List<SignalRequest> signalRequestList = new ArrayList<SignalRequest>();
    int len_input_list = pipeline.getInputList().size();
    signalList = pipeline.getInputList();
    for(int i = 0; i < len_input_list; i++)
    {
      SignalRequest signalRequest = new SignalRequest();
      signalRequest.setName(signalList.get(i).getName());
      signalRequest.setEventType(signalList.get(i).getEventType());
      signalRequest.setValueType(signalList.get(i).getValueType());
      signalRequestList.add(signalRequest);
    }
    int len_assessment_list = pipeline.getAssessmentList().size();
    List<Assessment> assessmentList = pipeline.getAssessmentList();
    List<AssessmentRequest> assessmentRequestList = new ArrayList<AssessmentRequest>();
    for(int i = 0; i < len_assessment_list; i++)
    {
      AssessmentRequest assessmentRequest = new AssessmentRequest();
      assessmentRequest.setName(assessmentList.get(i).getName());
      assessmentRequest.setInputList(assessmentList.get(i).getInputList());
      assessmentRequest.setAprioriConditionList(assessmentList.get(i).getAprioriConditionList());
      assessmentRequestList.add(assessmentRequest);
    }
    pipelineRequest.setName(pipeline.getName())
        .setThingIdentifier(pipeline.getThingIdentifier())
        .setInterval(pipeline.getInterval())
        .setEventbuffer(pipeline.getEventbuffer())
        .setInputList(signalRequestList)
        .setAssessmentList(assessmentRequestList)
        .setThingName(pipeline.getThingName());
    String pipeline_json = httpService.post("/pipeline", mapper.writeValueAsString(pipelineRequest));
    return mapper.readValue(pipeline_json, Pipeline.class);
  }

  public List<Pipeline> getPipelines() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    String pipeline_json = httpService.get("/pipeline");
    return mapper.readValue(pipeline_json, new TypeReference<List<Pipeline>>(){});
  }

  public void deletePipeline(String pipeline) throws Exception {
    httpService.delete("/pipeline/"+pipeline);
  }

  public InputStatus addInputData(String eventbuffer, String data, Map<String, String> options) throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    Map<String, String> ops = new HashMap<String, String>();
    String url = "/eventbuffer/"+eventbuffer;
    if(options.containsKey("subscription")){
      url += "?subscriptionKey="+options.get("subscription");
    }
    byte[] data_bytes = data.getBytes(Charset.forName("UTF-8"));
    InputStream stream = new ByteArrayInputStream(data_bytes);
    String status = this.httpService.postData(url, data);
    return mapper.readValue(status, InputStatus.class);
  }

  public String addVerification(String pipeline, String data, Map<String, String > options) throws Exception{
    String url = "/pipeline/" + pipeline + "/verification";
    return this.httpService.postData(url, data);
  }

  public InputStatus addInputFromStream(String eventbuffer, ByteArrayInputStream stream, Map<String, String> options) throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    String url = "/eventbuffer/"+eventbuffer;
    if(options.containsKey("subscription")){
      url += "?subscriptionKey="+options.get("subscription");
    }
    byte[] data_bytes = IOUtils.toByteArray(stream);
    String status = this.httpService.upstream(url, data_bytes);
    return mapper.readValue(status, InputStatus.class);
  }

  public String addVerificationStream(String pipeline, ByteArrayInputStream stream, Map<String, String> options) throws Exception{
    String url = "/pipeline/" + pipeline + "/verification";
    byte[] data_bytes = IOUtils.toByteArray(stream);
    return this.httpService.upstream(url, data_bytes);
  }

  public BufferedReader getOutput(String pipeline, Long start, Long end) throws Exception {
    String url = "/pipeline/"+pipeline+"/output?";
    if(end != null) {
      url += "lastTime=" + end;
      if(start != null)
        url += "&startTime="+start;
    }
    else {
      if(start != null)
        url += "startTime="+start;
    }
    return this.httpService.downstream(url);
  }


  private class StreamingThread implements Runnable {

    String pipeline = "";
    Long start = 0l;
    BufferedReader data;
    Boolean awaitingResponse = false;
    Boolean dataUpdated = false;
    Callback callback;

    private StreamingThread (String pipeline, Long start, Callback myCallback) throws Exception {
      this.pipeline = pipeline;
      this.start = start;
      this.callback = myCallback;
    }
    public void run() {
      try {
        while (true) {
          if (!awaitingResponse) {
            System.out.println("Checking for new data . . . ");
            data = null;
            awaitingResponse = true;
            data = outflowData(pipeline);
          }
          if (data != null) {
            System.out.println("Found new data");
            String line = null;
            String message = new String();
            final StringBuffer buffer = new StringBuffer(2048);
            while ((line = data.readLine()) != null) {
              //buffer.append(line);
              message += line;
            }
            System.out.println("Data : " + data);
            callback.call(message);
          }
          Thread.sleep(4000);
        }
      } catch (Exception e) {
        System.out.println("Exception : " + e);
      }
    }

    private BufferedReader outflowData (String pipeline) {
      try {
        System.out.println("Checking if pipeline is open . . . ");
        if(pipelineOpen()) {
          System.out.println("Pipeline is open - Start : " + start);
          String url = "/pipeline/" + pipeline + "/output?startTime=" + start;
          awaitingResponse = false;
          return httpService.downstream(url);
        } else {
          System.out.println("Pipeline not open");
          awaitingResponse = false;
        }
      } catch (Exception e) {
        System.out.println("Error : " + e);
      }
      return null;
    }

    private boolean pipelineOpen() throws Exception {
      String url = "/pipeline/" + pipeline;
      String pipeline_json = httpService.get(url);
      JSONObject outflowStatus = new JSONObject(pipeline_json);
      System.out.println("Pipeline details : " + pipeline_json + "\n"
              + "Outflow Status : " + outflowStatus.get("outflowStatus"));
      return (outflowStatus.get("outflowStatus").equals("OPEN"));
    }

    public void closeThread (Thread t){
      t.stop();
    }

  }

  private class Initiator {
    StreamingThread streamer;
    Thread streamingThread;

    private Initiator (String pipeline, Long start, Callback callback) {
      try {
        streamer = new StreamingThread(pipeline, start, callback);
        streamingThread = new Thread(streamer);
      } catch (Exception e) {
        System.out.println("Exception creating thread : " + e);
      }
    }

    private Thread startThread () {
      streamingThread.start();
      return streamingThread;
    }

    private void closeThread (Thread t) {
      t.stop();
    }
  }

  public Object streamOutput(String pipeline, Long start, Callback callback) {
    String data;
    Boolean streaming = true;
    try {
      Initiator initiator = new Initiator(pipeline, start, callback);
      Thread t = initiator.startThread();
      Thread.sleep(15000);
    } catch (Exception e) {
      System.out.println("Error instantiating streamingThread : " + e);
    }
    return null;
  }

  public Subscription createSubscription(String eventbuffer, Subscription subscription) throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    String subscription_json = httpService.post("/eventbuffer/"+eventbuffer+"/subscription", mapper.writeValueAsString(subscription));
    return mapper.readValue(subscription_json, Subscription.class);
  }

  public Subscription updateSubscription(String eventbuffer, Subscription subscription) throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    String subscription_json = httpService.put("/eventbuffer/"+eventbuffer+"/subscription/"+subscription.getKey(), mapper.writeValueAsString(subscription));
    return mapper.readValue(subscription_json, Subscription.class);
  }

  public void deleteSubscription(String eventbuffer, String subscription) throws Exception {
    httpService.delete("/eventbuffer/"+eventbuffer+"/subscription/"+subscription);
  }

  public Publication createPublication(String pipeline, Publication publication) throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    String publication_json = httpService.post("/pipeline/"+pipeline+"/publication", mapper.writeValueAsString(publication));
    return mapper.readValue(publication_json, Publication.class);
  }

  public Publication updatePublication(String pipeline, Publication publication) throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    String publication_json = httpService.put("/pipeline"+pipeline+"/publication/"+publication.getKey(), mapper.writeValueAsString(publication));
    return mapper.readValue(publication_json, Publication.class);
  }

  public void deletePublication(String pipeline, String publication) throws Exception {
    httpService.delete("/pipeline/"+pipeline+"/publication/"+publication);
  }
}
