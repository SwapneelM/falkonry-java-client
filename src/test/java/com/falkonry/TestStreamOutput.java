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
  String token = "tqrm0aqb0y24vuzeybqp218h8evjaday";    //auth token
  String pipeline = "jff0ktfdu3t38u";

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
      System.out.println("Calling pause : " + streamRunner.call("pause"));
      System.out.println("Calling resume : " + streamRunner.call("resume"));
      //wait();
    }
    catch (Exception e){
      System.out.println(e.toString()+"\nError in getting output");
      Assert.assertEquals(0,1);
    }
  }
}
