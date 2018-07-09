package com.tjacob.io.sparkMqttClient.mqtt;

import org.eclipse.paho.client.mqttv3.MqttConnectOptions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ConnectionParams implements Serializable {

  private String brokerUrl;
  private String topic;
  private String subscribingTopic;
  private String publishingTopic;
  private String username;
  private char[] password;
  private boolean cleanSession;
  private boolean useSSL;
  private int qos;
  private int connectionTimeout;
  private int keepAliveInterval;
  private int mqttVersion;
  private String trustedCertificatesFile;
  private String clientCertificateFile;
  private String clientSSLKey;
  private String clientSSLPassword;

  public String getSubscribingTopic() {
    return subscribingTopic;
  }

  public String getPublishingTopic() {
    return publishingTopic;
  }

  public String getTopic() {
    return topic;
  }

  public int getQos() {
    return qos;
  }

  public String getBrokerUrl() {
    return brokerUrl;
  }

  public String getUsername() {
    return username;
  }

  public char[] getPassword() {
    return password;
  }

  public boolean isCleanSession() {
    return cleanSession;
  }

  public int getConnectionTimeout() {
    return connectionTimeout;
  }

  public int getKeepAliveInterval() {
    return keepAliveInterval;
  }

  public int getMqttVersion() {
    return mqttVersion;
  }

  public ConnectionParams(String brokerUrl,
                          String publishingTopic,
                          String subscribingTopic,
                          int qos,
                          MqttConnectOptions options) {
    this(brokerUrl, publishingTopic, subscribingTopic, qos, options, null, null, null, null);
    this.useSSL = false;
  }

  public ConnectionParams(String brokerUrl,
                          String publishingTopic,
                          String subscribingTopic,
                          int qos,
                          MqttConnectOptions options,
                          String trustedCertificatesPath,
                          String clientCertificatePath,
                          String clientPrivateKey,
                          String clientPassword){
    this.brokerUrl = brokerUrl;
    this.publishingTopic = publishingTopic;
    this.subscribingTopic = subscribingTopic;
    this.qos = qos;
    this.username = options.getUserName();
    this.password = options.getPassword();
    this.connectionTimeout = options.getConnectionTimeout();
    this.keepAliveInterval = options.getKeepAliveInterval();
    this.mqttVersion = options.getMqttVersion();
    this.cleanSession = options.isCleanSession();
    this.trustedCertificatesFile = trustedCertificatesPath;
    this.clientCertificateFile = clientCertificatePath;
    this.clientSSLKey = clientPrivateKey;
    this.clientSSLPassword = clientPassword;
    this.useSSL = true;
  }

  public boolean isUseSSL() {
    return useSSL;
  }

  public String getTrustedCertificatesFile() {
    return trustedCertificatesFile;
  }

  public String getClientCertificateFile() {
    return clientCertificateFile;
  }

  public String getClientSSLKey() {
    return clientSSLKey;
  }

  public String getClientSSLPassword() {
    return clientSSLPassword;
  }

  public static MqttConnectOptions initOptions(ConnectionParams params){
    MqttConnectOptions options = new MqttConnectOptions();

    options.setMqttVersion(params.getMqttVersion());
    options.setCleanSession(params.isCleanSession());
    options.setUserName(params.getUsername());
    options.setPassword(params.getPassword());
    options.setKeepAliveInterval(params.getKeepAliveInterval());
    options.setConnectionTimeout(params.getConnectionTimeout());

    if (params.useSSL && params.getBrokerUrl().startsWith("ssl://")){
      try {
        options.setSocketFactory(SSLSocketBuilder.getSocketFactory(
                params.getTrustedCertificatesFile(),
                params.getClientCertificateFile(),
                params.getClientSSLKey(),
                params.getClientSSLPassword()));
      } catch(Exception e){
        e.printStackTrace();
      }
    }

    return options;
  }

}
