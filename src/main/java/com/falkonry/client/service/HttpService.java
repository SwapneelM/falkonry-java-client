package com.falkonry.client.service;

/*!
 * falkonry-java-client
 * Copyright(c) 2016 Falkonry Inc
 * MIT Licensed
 */

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;
import java.util.Map;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

public class HttpService {

  private String host;
  private String token;
  private String user_agent = "falkonry/java-client";

  public HttpService(String host, String token) throws Exception {
    this.host = (host == null) ? "https://service.falkonry.io" : host;
    try {
      this.token = Base64.getEncoder().encodeToString(token.getBytes("UTF-8"));
    } catch (Exception e) {
      throw new Exception("Invalid token");
    }
  }

  public String get(String path) throws Exception {
    String url = this.host + path;
    URL obj = new URL(url);
    HttpURLConnection con = (HttpURLConnection) obj.openConnection();
    con.setRequestMethod("GET");
    con.setRequestProperty("User-Agent", this.user_agent);
    con.setRequestProperty("Authorization", "Token "+this.token);
    int responseCode = con.getResponseCode();
    BufferedReader in = new BufferedReader(
        new InputStreamReader(con.getInputStream()));

    String inputLine;
    StringBuffer response = new StringBuffer();

    while ((inputLine = in.readLine()) != null) {
      response.append(inputLine);
    }

    in.close();

    if(responseCode == 401)
      throw new Exception("Unauthorized : Invalid token");
    else if(responseCode >= 400)
      throw new Exception(response.toString());
    else
      return response.toString();
  }

  public String post(String path, String data) throws Exception {
    String url = this.host + path;

    URL obj = new URL(url);
    HttpURLConnection con = (HttpURLConnection) obj.openConnection();
    con.setRequestMethod("POST");
    con.setRequestProperty("User-Agent", this.user_agent);
    con.setRequestProperty("Content-Type", "application/json");
    con.setRequestProperty("Authorization", "Token "+this.token);
    con.setDoOutput(true);
    DataOutputStream wr = new DataOutputStream(con.getOutputStream());
    wr.writeBytes(data);
    wr.flush();
    wr.close();
    int responseCode = con.getResponseCode();
    InputStream is = con.getInputStream();
    BufferedReader in = new BufferedReader(
            new InputStreamReader(is));

    String inputLine;
    StringBuffer response = new StringBuffer();

    while ((inputLine = in.readLine()) != null) {
      response.append(inputLine);
    }

    in.close();


    if(responseCode == 401)
      throw new Exception("Unauthorized : Invalid token");
    else if(responseCode >= 400)
      throw new Exception(response.toString());
    else
      return response.toString();

  }


  public String postData(String path, String data) throws Exception {
    String url = this.host + path;
    URL obj = new URL(url);
    HttpURLConnection con = (HttpURLConnection) obj.openConnection();
    con.setRequestMethod("POST");
    con.setRequestProperty("User-Agent", this.user_agent);
    con.setRequestProperty("Content-Type", "text/plain");
    con.setRequestProperty("Authorization", "Token "+this.token);
    con.setDoOutput(true);
    DataOutputStream wr = new DataOutputStream(con.getOutputStream());
    wr.writeBytes(data);
    wr.flush();
    wr.close();
    int responseCode = con.getResponseCode();
    InputStream is = con.getInputStream();
    BufferedReader in = new BufferedReader(
        new InputStreamReader(is));

    String inputLine;
    StringBuffer response = new StringBuffer();

    while ((inputLine = in.readLine()) != null) {
      response.append(inputLine);
    }

    in.close();

    if(responseCode == 401)
      throw new Exception("Unauthorized : Invalid token");
    else if(responseCode >= 400)
      throw new Exception(response.toString());
    else
      return response.toString();

  }

  public String put(String path, String data) throws Exception {
    String url = this.host + path;
    URL obj = new URL(url);
    HttpURLConnection con = (HttpURLConnection) obj.openConnection();
    con.setRequestMethod("PUT");
    con.setRequestProperty("User-Agent", this.user_agent);
    con.setRequestProperty("Content-Type", "application/json");
    con.setRequestProperty("Authorization", "Token "+this.token);
    con.setDoOutput(true);
    DataOutputStream wr = new DataOutputStream(con.getOutputStream());
    wr.writeBytes(data);
    wr.flush();
    wr.close();

    int responseCode = con.getResponseCode();
    BufferedReader in = new BufferedReader(
        new InputStreamReader(con.getInputStream()));

    String inputLine;
    StringBuffer response = new StringBuffer();

    while ((inputLine = in.readLine()) != null) {
      response.append(inputLine);
    }

    in.close();

    if(responseCode == 401)
      throw new Exception("Unauthorized : Invalid token");
    else if(responseCode >= 400)
      throw new Exception(response.toString());
    else
      return response.toString();
  }

  public String delete(String path) throws Exception {
    String url = this.host + path;
    CloseableHttpClient httpClient = HttpClients.createDefault();
    HttpDelete httpDelete = new HttpDelete(url);
    httpDelete.addHeader("User-Agent", this.user_agent);
    httpDelete.addHeader("Authorization", "Token "+this.token);
    httpDelete.addHeader("Content-Type", "application/json");
    CloseableHttpResponse closeableHttpResponse = httpClient.execute(httpDelete);
    int responseCode = closeableHttpResponse.getStatusLine().getStatusCode();
    if(responseCode == 401)
      throw new Exception("Unauthorized : Invalid token");
    else if(responseCode >= 400)
      throw new Exception("Error:"+responseCode);
    else
      return "Success";//response.toString();
  }

  public String sfpost(String path, Map<String, String> params, InputStream stream) throws Exception {
    String url = this.host + path;
    String tempFileName;
    CloseableHttpClient httpClient = HttpClients.createDefault();
    HttpPost httpPost = new HttpPost(url);
    httpPost.addHeader("User-Agent", this.user_agent);
    httpPost.addHeader("Authorization", "Token "+this.token);

    MultipartEntityBuilder builder = MultipartEntityBuilder.create();
    builder.addTextBody("name",params.get("name"));
    builder.addTextBody("timeIdentifier",params.get("timeIdentifier"));
    builder.addTextBody("timeFormat",params.get("timeFormat"));

    if(stream != null)
    {
      tempFileName = "input-" + Math.random() + "." + params.get("fileFormat");
      String content_type = "text/" + params.get("fileFormat");
      builder.addBinaryBody("data",stream,ContentType.create(content_type),tempFileName);
    }

    HttpEntity multipart = builder.build();
    httpPost.setEntity(multipart);

    CloseableHttpResponse closeableHttpResponse = httpClient.execute(httpPost);
    int responseCode = closeableHttpResponse.getStatusLine().getStatusCode();
    HttpEntity responseEntity = closeableHttpResponse.getEntity();

    InputStream is = responseEntity.getContent();
    BufferedReader in = new BufferedReader(
        new InputStreamReader(is));

    String inputLine;
    StringBuffer response = new StringBuffer();

    while ((inputLine = in.readLine()) != null) {
      response.append(inputLine);
    }

    in.close();

    if(responseCode == 401)
      throw new Exception("Unauthorized : Invalid token");
    else if(responseCode >= 400)
      throw new Exception(response.toString());
    else
      return response.toString();

  }

  public String upstream(String path, byte[] data) throws Exception {
    String url = this.host + path;
    URL obj = new URL(url);
    HttpURLConnection con = (HttpURLConnection) obj.openConnection();
    con.setRequestMethod("POST");
    con.setRequestProperty("User-Agent", this.user_agent);
    con.setRequestProperty("Content-Type", "text/plain");
    con.setRequestProperty("Authorization", "Token "+this.token);
    con.setDoOutput(true);
    DataOutputStream wr = new DataOutputStream(con.getOutputStream());
    wr.write(data);
    wr.flush();
    wr.close();

    int responseCode = con.getResponseCode();
    BufferedReader in = new BufferedReader(
        new InputStreamReader(con.getInputStream()));

    String inputLine;
    StringBuffer response = new StringBuffer();

    while ((inputLine = in.readLine()) != null) {
      response.append(inputLine);
    }

    in.close();

    if(responseCode == 401)
      throw new Exception("Unauthorized : Invalid token");
    else if(responseCode >= 400)
      throw new Exception(response.toString());
    else
      return response.toString();
  }

  public BufferedReader downstream(String path) throws Exception {
    String url = this.host + path;
    URL obj = new URL(url);
    HttpURLConnection con = (HttpURLConnection) obj.openConnection();
    con.setRequestMethod("GET");
    con.setRequestProperty("User-Agent", this.user_agent);
    con.setRequestProperty("Authorization", "Token "+this.token);
    int responseCode = con.getResponseCode();
    BufferedReader in = new BufferedReader(
        new InputStreamReader(con.getInputStream()));

    if(responseCode == 401)
      throw new Exception("Unauthorized : Invalid token");
    else if(responseCode >= 400) {
      String inputLine;
      StringBuffer response = new StringBuffer();

      while ((inputLine = in.readLine()) != null) {
        response.append(inputLine);
      }
      in.close();
      throw new Exception(response.toString());
    }
    else
      return in;
  }

  public HttpURLConnection downstreamRequest(String path) throws Exception {
    String url = this.host + path;
      URL obj = new URL(url);
    HttpURLConnection con = (HttpURLConnection) obj.openConnection();
    con.setRequestMethod("GET");
      con.setRequestProperty("Accept", "application/json");
      con.setRequestProperty("Content-Type", "application/x-json-stream");
    con.setRequestProperty("Authorization", "Token "+this.token);
    int responseCode = con.getResponseCode();
    BufferedReader in = new BufferedReader(
            new InputStreamReader(con.getInputStream()));
      in.close();
    if(responseCode == 401)
      throw new Exception("Unauthorized : Invalid token");
    else if(responseCode >= 400) {

      StringBuffer response = new StringBuffer();
        String inputLine;
        while ((inputLine = in.readLine()) != null) {
            response.append(inputLine);
        }
      throw new Exception(response.toString());
    }
    else
      return con;
  }
}
