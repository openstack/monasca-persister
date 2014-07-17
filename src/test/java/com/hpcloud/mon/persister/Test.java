package com.hpcloud.mon.persister;

import com.hpcloud.mon.persister.configuration.MonPersisterConfiguration;
import com.hpcloud.mon.persister.consumer.KafkaConsumer;
import com.hpcloud.util.config.ConfigurationException;
import com.hpcloud.util.config.ConfigurationFactory;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class Test extends KafkaConsumer {
  private static final Logger logger = LoggerFactory.getLogger(Test.class);

  private static final String TOPIC = "Test";

  public Test(MonPersisterConfiguration configuration) {
    super(configuration);
  }

  public static void main(String[] args) throws IOException, ConfigurationException {
    final MonPersisterConfiguration config = createConfig(args[0]);
    config.getKafkaConfiguration();
    final Test test = new Test(config);
    test.run();
  }

  private static MonPersisterConfiguration createConfig(String configFileName) throws IOException,
      ConfigurationException {
    return ConfigurationFactory
        .<MonPersisterConfiguration>forClass(MonPersisterConfiguration.class).build(
            new File(configFileName));
  }

  @Override
  protected Runnable createRunnable(KafkaStream<byte[], byte[]> stream, int threadNumber) {
    logger.info("Created KafkaReader for {}", threadNumber);
    return new KafkaReader(stream, threadNumber);
  }

  @Override
  protected String getStreamName() {
    return TOPIC;
  }

  protected class KafkaReader implements Runnable {

    private final KafkaStream<byte[], byte[]> stream;
    private final int threadNumber;

    public KafkaReader(KafkaStream<byte[], byte[]> stream, int threadNumber) {
      this.threadNumber = threadNumber;
      this.stream = stream;
    }


    public void run() {
      ConsumerIterator<byte[], byte[]> it = stream.iterator();
      while (it.hasNext()) {

        final String s = new String(it.next().message());

        logger.debug("Thread {}: {}", threadNumber, s);
      }
      logger.debug("Shutting down Thread: " + threadNumber);
    }
  }

}
