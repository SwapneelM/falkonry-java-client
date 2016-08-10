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
  String token = "";

  class OutflowCallback implements javafx.util.Callback<String, String> {
    public String call (String result) {
      System.out.println("Result :" + result + "\n");
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
      String pipeline = "zmusfprsf7zspf";
      Long start = 123456l;
      Object streamerUser = falkonry.streamOutput(pipeline, start, new OutflowCallback());
    }
    catch (Exception e){
      System.out.println(e.toString()+"\nError in getting output");
      Assert.assertEquals(0,1);
    }
  }
}
