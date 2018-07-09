package com.tjacob.io.sparkMqttClient;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

public class AppOptions {

  public static final String PROPERTIES_FILE = "propertiesFile";

  public static final String SUBSCRIBE_BROKER_URL = "subscribe_brokerUrl";
  public static final String SUBSCRIBE_TOPIC = "subscribe_topic";
  public static final String SUBSCRIBE_QOS = "subscribe_option";
  public static final String SUBSCRIBE_USER = "subscribe_username";
  public static final String SUBSCRIBE_PASSWORD = "subscribe_password";
  public static final String SUBSCRIBE_SSL_TRUSTED_CERTIFICATES = "subscribe_certificates";
  public static final String SUBSCRIBE_SSL_CERTIFICATE = "subscribe_crt";
  public static final String SUBSCRIBE_SSL_KEY = "subscribe_ssl_key";
  public static final String SUBSCRIBE_SSL_PASSWORD = "subscribe_ssl_password";

  public static final String PUBLISH_BROKER_URL = "publish_brokerUrl";
  public static final String PUBLISH_TOPIC = "publish_topic";
  public static final String PUBLISH_QOS = "publish_option";
  public static final String PUBLISH_USER = "publish_username";
  public static final String PUBLISH_PASSWORD = "publish_password";
  public static final String PUBLISH_SSL_TRUSTED_CERTIFICATES = "publish_certificates";
  public static final String PUBLISH_SSL_CERTIFICATE = "publish_crt";
  public static final String PUBLISH_SSL_KEY = "publish_ssl_key";
  public static final String PUBLISH_SSL_PASSWORD = "publish_ssl_password";


  public static Options helpParameters() {
    Option helpFileOption = OptionBuilder
            .withLongOpt("help")
            .withDescription("Show Application Usage")
            .create("h");

    final Options firstOptions = new Options();

    firstOptions.addOption(helpFileOption);

    return firstOptions;
  }

  public static Options configParameters(final Options firstOptions) {

    final Option propertiesFileOption = OptionBuilder
            .withLongOpt(PROPERTIES_FILE)
            .withDescription("Properties File where we can pass all the options can be passed into this file")
            .hasArg(true)
            .isRequired(false)
            .create("f");

    final Option subscribeBrokerUrlOption = OptionBuilder
            .withLongOpt(SUBSCRIBE_BROKER_URL)
            .withDescription("URL of MQTT Broker to consume messages from.")
            .hasArg(true)
            .withArgName("Subscribe Broker URL")
            .isRequired(false)
            .create();

    final Option subscribeTopicOption = OptionBuilder
            .withLongOpt(SUBSCRIBE_TOPIC)
            .withDescription("Topic to consume messages from.")
            .hasArg(true)
            .withArgName("Subscribe topic")
            .isRequired(false)
            .create();

    final Option subscribeQosOption = OptionBuilder
            .withLongOpt(SUBSCRIBE_QOS)
            .withDescription("Desired QOS for subscribe broker. {0|1|2} ")
            .hasArg(true)
            .withType(Integer.class)
            .isRequired(false)
            .create();

    final Option subscribeUserOption = OptionBuilder
            .withLongOpt(SUBSCRIBE_USER)
            .withDescription("User to connect to subscribe broker")
            .hasArg(true)
            .isRequired(false)
            .create();

    final Option subscribePasswordOption = OptionBuilder
            .withLongOpt(SUBSCRIBE_PASSWORD)
            .withDescription("Password to connect to subscribe mqttBroker")
            .hasArg(true)
            .isRequired(false)
            .create();

    final Option publishBrokerUrlOption = OptionBuilder
            .withLongOpt(PUBLISH_BROKER_URL)
            .withDescription("URL of MQTT Broker to publish messages to.")
            .hasArg(true)
            .withArgName("publish Broker URL")
            .isRequired(false)
            .create();

    final Option publishTopicOption = OptionBuilder
            .withLongOpt(PUBLISH_TOPIC)
            .withDescription("Topic to publish messages to.")
            .hasArg(true)
            .withArgName("publish topic")
            .isRequired(false)
            .create();

    final Option publishQosOption = OptionBuilder
            .withLongOpt(PUBLISH_QOS)
            .withDescription("Desired QOS for subscribe broker. {0|1|2}")
            .hasArg(true)
            .withType(Integer.class)
            .isRequired(false)
            .create();

    final Option publishUserOption = OptionBuilder
            .withLongOpt(PUBLISH_USER)
            .withDescription("User to connect to mqttBroker publisher")
            .hasArg(true)
            .isRequired(false)
            .create();

    final Option publishPasswordOption = OptionBuilder
            .withLongOpt(PUBLISH_PASSWORD)
            .withDescription("Password to connect to mqttBroker publisher")
            .hasArg(true)
            .isRequired(false)
            .create();

    final Option subscribeSSLCertificatesOption = OptionBuilder
            .withLongOpt(SUBSCRIBE_SSL_TRUSTED_CERTIFICATES)
            .withDescription("Trusted Certificates for connection with the broker")
            .hasArg(true)
            .isRequired(false)
            .create();

    final Option subscribeSSLClientCertificateOption = OptionBuilder
            .withLongOpt(SUBSCRIBE_SSL_CERTIFICATE)
            .withDescription("SSL Certificate of the client")
            .hasArg(true)
            .isRequired(false)
            .create();

    final Option subscribeSSLPrivateKeyOption = OptionBuilder
            .withLongOpt(SUBSCRIBE_SSL_KEY)
            .withDescription("Private Key of The client")
            .hasArg(true)
            .isRequired(false)
            .create();

    final Option subscribeSSLPasswordOption = OptionBuilder
            .withLongOpt(SUBSCRIBE_SSL_PASSWORD)
            .withDescription("Password if key is encrypted")
            .hasArg(true)
            .isRequired(false)
            .create();

    final Option publishSSLCertificatesOption = OptionBuilder
            .withLongOpt(PUBLISH_SSL_TRUSTED_CERTIFICATES)
            .withDescription("Trusted Certificates for connection with the broker")
            .hasArg(true)
            .isRequired(false)
            .create();

    final Option publishSSLClientCertificateOption = OptionBuilder
            .withLongOpt(PUBLISH_SSL_CERTIFICATE)
            .withDescription("SSL Certificate of the client")
            .hasArg(true)
            .isRequired(false)
            .create();

    final Option publishSSLPrivateKeyOption = OptionBuilder
            .withLongOpt(PUBLISH_SSL_KEY)
            .withDescription("Private Key of The client")
            .hasArg(true)
            .isRequired(false)
            .create();

    final Option publishSSLPasswordOption = OptionBuilder
            .withLongOpt(PUBLISH_SSL_PASSWORD)
            .withDescription("Password if key is encrypted")
            .hasArg(true)
            .isRequired(false)
            .create();



    Options options = new Options();

    for (Object fo : firstOptions.getOptions()) {
      options.addOption((Option)fo);
    }

    options.addOption(propertiesFileOption);

    options.addOption(subscribeTopicOption);
    options.addOption(subscribeBrokerUrlOption);
    options.addOption(subscribeQosOption);
    options.addOption(subscribeUserOption);
    options.addOption(subscribePasswordOption);
    options.addOption(subscribeSSLCertificatesOption);
    options.addOption(subscribeSSLClientCertificateOption);
    options.addOption(subscribeSSLPrivateKeyOption);
    options.addOption(subscribeSSLPasswordOption);

    options.addOption(publishTopicOption);
    options.addOption(publishBrokerUrlOption);
    options.addOption(publishQosOption);
    options.addOption(publishUserOption);
    options.addOption(publishPasswordOption);
    options.addOption(publishSSLCertificatesOption);
    options.addOption(publishSSLClientCertificateOption);
    options.addOption(publishSSLPrivateKeyOption);
    options.addOption(publishSSLPasswordOption);

    return options;
  }

  public static Properties getPropertiesFromCmdline(Iterator<Option> optionsIterator){
    Properties properties = new Properties();

    optionsIterator.forEachRemaining((option) -> {
      properties.put(option.getLongOpt(), option.getValue());
    });
    return properties;
  }


}
