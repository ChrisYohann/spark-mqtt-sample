package com.tjacob.io.sparkMqttClient.obd;

import java.io.Serializable;

public class FireBrakeEventParams implements Serializable {

  public static final int DEFAULT_DECREASING_THRESHOLD = 10;
  public static final int DEFAULT_WINDOW_SURVEY_SIZE = 5;
  public static final String EMERGENCY_BRAKING_STATUS = "emergency braking";

  private int decreasingSpeedThreshold;
  private int windowSurveySize;

    public FireBrakeEventParams(int decreasingThreshold, int windowSize){
        this.decreasingSpeedThreshold = decreasingThreshold;
        this.windowSurveySize = windowSize;
    }

    public FireBrakeEventParams(){
      this.decreasingSpeedThreshold = DEFAULT_DECREASING_THRESHOLD;
      this.windowSurveySize = DEFAULT_WINDOW_SURVEY_SIZE;
    }

    public int getDecreasingSpeedThreshold() {
        return decreasingSpeedThreshold;
    }

    public int getWindowSurveySize() {
        return windowSurveySize;
    }
}
