package com.falkonry;

/*!
 * falkonry-java-client
 * Copyright(c) 2016 Falkonry Inc
 * MIT Licensed
 */

import com.falkonry.client.Falkonry;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.security.auth.callback.Callback;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.Observer;

public class TestStreamOutput {
  Falkonry falkonry = null;
  String host = "http://localhost:8080";
  String token = "";      //auth token
  String pipeline = "";

  public class OutflowCallback implements javafx.util.Callback<String, String> {
    public String call (String result) {
      System.out.println("Callback Result :" + result + "\n");
      return result;
    }
  }

  @Before
  public void setUp() throws Exception {
    falkonry = new Falkonry(host, token);
  }

  @Test
  public void streamOutput() throws Exception{

    try {
      Long start = 123456l;
      javafx.util.Callback<String, String> streamRunner= falkonry.streamOutput(pipeline, start, new OutflowCallback());
      long now = System.currentTimeMillis();
      long end = now + 2*1000;
      while (System.currentTimeMillis() < end) {}
      try {
        System.out.println("Called pause : " + streamRunner.call("pause"));
      } catch (Exception e) {}
      now = System.currentTimeMillis();
      end = now + 10*1000;
      while (System.currentTimeMillis() < end) {}
      System.out.println("Called resume : " + streamRunner.call("resume"));

      now = System.currentTimeMillis();
      end = now + 20*1000;
      while (System.currentTimeMillis() < end) {}
      try {
        System.out.println("Called close : " + streamRunner.call("close"));
      } catch (Exception e) {}
    }

    catch (Exception e){
      System.out.println(e.toString()+"\nError in getting output");
      Assert.assertEquals(0,1);
    }
  }
}
