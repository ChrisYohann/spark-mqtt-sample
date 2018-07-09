package com.tjacob.io.sparkMqttClient;

import com.tjacob.io.sparkMqttClient.mqtt.ConnectionParams;
import com.tjacob.io.sparkMqttClient.obd.FireBrakeEventParams;
import com.tjacob.io.sparkMqttClient.obd.OBDReceiver;
import org.apache.commons.cli.*;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.StreamingLinearRegressionWithSGD;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import org.apache.spark.SparkConf;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class App
{

  public static final String DEFAULT_MQTT_BROKER_URL = "tcp://localhost:1883";

  public static double[] INITIAL_WEIGHTS = {0.5, 0.4, 0.1};

  public static String getOptionKey(String prefix, String key){
    return String.format("%s_%s", prefix, key);
  }

  public static MqttConnectOptions getMqttOptions(Properties properties, String prefix){

    MqttConnectOptions options = new MqttConnectOptions();

    String usernameKey = getOptionKey(prefix, "username");
    String passwordKey = getOptionKey(prefix, "password");
    String cleanSessionKey = getOptionKey(prefix, "useCleanSession");
    String keepAliveIntervalKey = getOptionKey(prefix, "keepAliveInterval");
    String mqttVersionKey = getOptionKey(prefix, "mqttVersion");

    if (properties.containsKey(usernameKey) && properties.containsKey(passwordKey)){
      options.setUserName(properties.getProperty(usernameKey));
      options.setPassword(properties.getProperty(passwordKey).toCharArray());
    }

    options.setCleanSession(true);

    if (properties.containsKey(cleanSessionKey)) {
      options.setCleanSession(Boolean.getBoolean(cleanSessionKey));
    } else {
      options.setCleanSession(true);
    }

    if (properties.containsKey(mqttVersionKey)){
      options.setMqttVersion(Integer.parseInt(properties.getProperty(mqttVersionKey)));
    }

    return options;
  }

  public static ConnectionParams getConnectionParams(Properties properties, String prefix){
    String trustedCertificates;
    String clientCertificate = null;
    String clientSSLKey = null;
    String clientSSLPassword = "";

    String brokerUrlKey = getOptionKey(prefix, "brokerUrl");
    String publishTopicKey = getOptionKey(prefix, "topic");
    String subscribeTopicKey = getOptionKey(prefix, "topic");
    String qosKey = getOptionKey(prefix, "qos");

    String sslCertificatesKey = getOptionKey(prefix, "certificates");
    String sslClientCertificateKey = getOptionKey(prefix, "crt");
    String sslClientPrivateKeyKey = getOptionKey(prefix, "ssl_key");
    String sslClientPasswordKey = getOptionKey(prefix, "ssl_password");

    String brokerUrl = properties.getProperty(brokerUrlKey, DEFAULT_MQTT_BROKER_URL);
    String publishTopic = properties.getProperty(publishTopicKey, "/#");
    String subscribeTopic = properties.getProperty(subscribeTopicKey, "/#");
    int qos = Integer.parseInt(properties.getProperty(qosKey, "0"));

    MqttConnectOptions options = getMqttOptions(properties, prefix);

    if (properties.containsKey(sslCertificatesKey)){
      trustedCertificates = properties.getProperty(sslCertificatesKey);
      if (properties.containsKey(sslClientCertificateKey) && properties.containsKey(sslClientPrivateKeyKey)){
        clientCertificate = properties.getProperty(sslClientCertificateKey);
        clientSSLKey = properties.getProperty(sslClientPrivateKeyKey);
        if (properties.contains(sslClientPasswordKey)){
         clientSSLPassword = properties.getProperty(sslClientPasswordKey);
        }
      }
      return new ConnectionParams(brokerUrl, publishTopic, subscribeTopic, qos, options,
              trustedCertificates, clientCertificate, clientSSLKey, clientSSLPassword);
    }

    return new ConnectionParams(brokerUrl, publishTopic, subscribeTopic, qos, options);
  }

  public static void main( String[] args ) throws Exception
  {
    SparkConf sparkConf = new SparkConf()
            .setAppName("MQTT Streaming Suscriber")
            .setMaster("local[2]");
    JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(5000));

    Options firstOptions = AppOptions.helpParameters();
    Options options = AppOptions.configParameters(firstOptions);

    CommandLineParser parser = new BasicParser();
    CommandLine firstLine = parser.parse(firstOptions, args, true);

    if (firstLine.hasOption("help")) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("Spark MQTT Sample", options, true);
      System.exit(0);
    }

    Properties defaultProperties = new Properties();
    defaultProperties.load(App.class.getResourceAsStream("/default_settings.properties"));

    CommandLine line = parser.parse(options, args);
    Properties commandLineProperties = AppOptions.getPropertiesFromCmdline(line.iterator());

    Properties filePassedProperties = new Properties();

    if (commandLineProperties.containsKey(AppOptions.PROPERTIES_FILE)){
      filePassedProperties.load(new FileInputStream(new File(commandLineProperties.getProperty(AppOptions.PROPERTIES_FILE))));
    }

    Properties mergedProperties = new Properties();

    defaultProperties.forEach((key, value) -> {
      mergedProperties.merge(key, value, (value1, value2) -> value2);
    });

    filePassedProperties.forEach((key, value) -> {
      mergedProperties.merge(key, value, (value1, value2) -> value2);
    });

    commandLineProperties.forEach((key, value) -> {
      mergedProperties.merge(key, value, (value1, value2) -> value2);
    });

    mergedProperties.stringPropertyNames().forEach((property) -> {
      System.out.println(String.format("%s : %s", property, mergedProperties.getProperty(property)));
    });

    ConnectionParams publishParams = getConnectionParams(mergedProperties, "publish");
    ConnectionParams subscribeParams = getConnectionParams(mergedProperties, "subscribe");

    StreamingLinearRegressionWithSGD model = new StreamingLinearRegressionWithSGD()
            .setInitialWeights(Vectors.dense(INITIAL_WEIGHTS));

    OBDReceiver receiver = new OBDReceiver(
            MqttClient.generateClientId(),
            subscribeParams,
            new FireBrakeEventParams()
    );

    JavaDStream<Vector> obdConsumerStream = ssc.receiverStream(receiver);

    model.predictOn(obdConsumerStream)
            .foreachRDD((rdd) -> {
      rdd.foreachPartition((doubleIterator -> {
        MqttClient mqttClient = new MqttClient(
                publishParams.getBrokerUrl(),
                MqttClient.generateClientId(),
                new MemoryPersistence());
        mqttClient.connect(ConnectionParams.initOptions(publishParams));
        doubleIterator.forEachRemaining((proba) -> {
          if (proba >= 0.5){
            String message = String.format(
                    "{\"status\":\"emergency braking\", \"proba\": %1.2f}",
                    proba
            );
            MqttMessage mqttMessage = new MqttMessage(message.getBytes(StandardCharsets.UTF_8));
            try {
              mqttClient.publish(publishParams.getPublishingTopic(), mqttMessage);
              mqttClient.disconnect();
              mqttClient.close();
            } catch (MqttException e) {
              e.printStackTrace();
            }
          }
        });
      }));
    });

    ssc.start();
    ssc.awaitTermination();
  }
}
