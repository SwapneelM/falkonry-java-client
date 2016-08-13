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
  String host = "https://dev.falkonry.io";
  String token = "dg5th0r2rj4rywg3rv90egu3krswpw50";
  String pipeline = "auvkkw8boml4l2";

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
      Object streamerUser = falkonry.streamOutput(pipeline, start, new OutflowCallback());
    }
    catch (Exception e){
      System.out.println(e.toString()+"\nError in getting output");
      Assert.assertEquals(0,1);
    }
  }
}
