package com.tjacob.io.sparkMqttClient.obd;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tjacob.io.sparkMqttClient.mqtt.ConnectionParams;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.mllib.linalg.DenseVector;
import org.apache.spark.streaming.receiver.Receiver;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;


public class OBDReceiver extends Receiver<Vector> implements Serializable {

  public final String SPEED = "Speed";

  private String clientId;

  private MqttClient mqttClient;

  private ConnectionParams connectionParams;

  public MqttClient getMqttClient() {
    return mqttClient;
  }

  public void setMqttClient(MqttClient mqttClient) {
    this.mqttClient = mqttClient;
  }

  private double lastSpeedValue = 0;
  private CarStatus currentCarStatus = CarStatus.NORMAL;
  private int currentEventsSinceFirstBrake = 0;
  private FireBrakeEventParams fireBrakeEventsParams;

  public OBDReceiver(String clientId,
                     ConnectionParams connectionParams,
                     FireBrakeEventParams brakingParams){
    super(StorageLevel.MEMORY_AND_DISK());
    this.clientId = clientId;
    this.connectionParams = connectionParams;
    this.fireBrakeEventsParams = brakingParams;
    this.lastSpeedValue = 0;
  }

  @Override
  public void onStart() {
    try {
      MemoryPersistence persistence = new MemoryPersistence();

      String brokerUrl = connectionParams.getBrokerUrl();
      String subscribingTopic = connectionParams.getSubscribingTopic();
      String publishingTopic = connectionParams.getPublishingTopic();

      MqttConnectOptions options = ConnectionParams.initOptions(connectionParams);

      mqttClient = new MqttClient(brokerUrl, clientId, persistence);

      MqttCallback callback = new MqttCallback() {
        @Override
        public void connectionLost(Throwable cause) {
          restart("Connection Lost", cause);
        }

        @Override
        public void messageArrived(String topic, MqttMessage message) throws Exception {
          if (topic.equals("OBD2/cardata")){
            String messageToPublish = null;
            String messageAsString = new String(message.getPayload(), StandardCharsets.UTF_8);
            System.out.println("Message Arrived :"+messageAsString + "Last speed : "+lastSpeedValue);
            try {
              HashMap<String,Object> result =
                      new ObjectMapper().readValue(messageAsString, HashMap.class);
              Object jsonSpeed = result.get(SPEED);
              double newSpeed;
              if (jsonSpeed instanceof Integer){
                newSpeed = Double.valueOf((Integer) jsonSpeed);
              } else if (jsonSpeed instanceof Double){
                newSpeed = Double.valueOf((Double) jsonSpeed);
              } else {
                newSpeed = Double.valueOf((String) jsonSpeed);
              }
              CarStatus newStatus = updateStatus(
                      newSpeed,
                      lastSpeedValue,
                      currentEventsSinceFirstBrake,
                      currentCarStatus,
                      fireBrakeEventsParams
              );

              lastSpeedValue = newSpeed;

              System.out.println(newStatus);
              currentCarStatus = newStatus;
              store(buildVectorFeatures(currentCarStatus));

              switch (newStatus){
                case BRAKING:
                  currentEventsSinceFirstBrake += 1;
                  System.out.println("Car Speed Dropped By 10. Sending Emergency Braking with probability 0.5");
                  break;

                case STILL_BRAKING:
                  currentEventsSinceFirstBrake += 1;
                  System.out.println("Car Speed Dropped By 10 again. Sending Emergency Braking with probability 0.9");
                  break;

                case STOPPED:
                  currentEventsSinceFirstBrake = 0;
                  System.out.println("Car Stopped");
                  break;

                case NORMAL:
                  currentEventsSinceFirstBrake = 0;
                  break;
              }
            } catch (Exception e){
              e.printStackTrace();
            }
          }
        }

        @Override
        public void deliveryComplete(IMqttDeliveryToken token) {

        }
      };

      mqttClient.setCallback(callback);
      mqttClient.connect(options);
      mqttClient.subscribe(subscribingTopic, connectionParams.getQos());


    } catch (Exception e){
      e.printStackTrace();
    }
  }

  @Override
  public void onStop() {
    restart("Stop to Stream data");
  }

  static String buildMessage(CarStatus carStatus){
    ObjectMapper mapper = new ObjectMapper();
    String result = "";

    Map<String, Object> map = new HashMap<String, Object>();
    double proba;

    switch (carStatus){
      case BRAKING:
        proba = 0.5;
        break;
      case STILL_BRAKING:
        proba = 0.9;
        break;
      case STOPPED:
        proba = 1.0;
        break;
      default:
        proba = 0;
        break;
    }
    map.put("status", "emergency braking");
    map.put("probability", proba);

    // convert map to JSON string
    try {
      result = mapper.writeValueAsString(map);
    } catch (JsonProcessingException e){
      result = "{}";
    }

    return result;

  }



  static CarStatus updateStatus(double actualSpeed,
                                double lastSpeed,
                                int currentEventsBeforeStops,
                                CarStatus currentStatus,
                                FireBrakeEventParams params){
    double minDecreasingThrehsold = params.getDecreasingSpeedThreshold();
    double diff = lastSpeed - actualSpeed;

    if (actualSpeed == 0){
      if(diff == 0){
        return CarStatus.NORMAL;
      }
      if (currentEventsBeforeStops <= params.getWindowSurveySize()){
        return CarStatus.STOPPED;
      }
    }

    if (diff >= minDecreasingThrehsold){
      switch (currentStatus){
        case NORMAL:
          return CarStatus.BRAKING;
        case BRAKING:
          return CarStatus.STILL_BRAKING;
        case STILL_BRAKING:
          return CarStatus.STILL_BRAKING;
      }
    }

    return CarStatus.NORMAL;
  }

  static Vector buildVectorFeatures(CarStatus carStatus){
    double[] features = new double[3];
    Arrays.fill(features, 0);

    if (!carStatus.equals(CarStatus.NORMAL)){

      features[0] = 1;

      if (carStatus.equals(CarStatus.BRAKING)){
        return new DenseVector(features);
      }

      features[1] = 1;

      if (carStatus.equals(CarStatus.STILL_BRAKING)){
        return new DenseVector(features);
      }

      features[2] = 1;
    }

    return new DenseVector(features);
  }

  public enum CarStatus {
    NORMAL,
    BRAKING,
    STILL_BRAKING,
    STOPPED
  }


}
