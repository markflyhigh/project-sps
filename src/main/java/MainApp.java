import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MainApp {

  private static final Logger LOG = LoggerFactory.getLogger(MainApp.class);

  private static final String CONFIG_FILE_NAME = "config.properties";
  private static final String SOURCE_URL_PROPERTY = "source_url";
  private static final String EVENT_QUEUE_SIZE_PROPERTY = "event_queue_size";

  public static void main(String[] args) {
    // 1. Initialization
    Properties configuration = loadConfigFromFile();
    String sourceUrl = configuration.getProperty(SOURCE_URL_PROPERTY);
    int eventQueueSize = Integer.parseInt(configuration.getProperty(EVENT_QUEUE_SIZE_PROPERTY));

    BlockingQueue<String> eventQueue = new ArrayBlockingQueue<>(eventQueueSize);
    EventInjector injector = new EventInjector(sourceUrl, eventQueue);

    // 2. Start the event injection
    Thread injectionThread = new Thread(injector);
    injectionThread.start();

    // 3. Start the Start-per-second aggregation service to continously process
    // events
    SPSAggregationService service = new SPSAggregationService(configuration);
    service.run(eventQueue);
  }

  public static Properties loadConfigFromFile() {
    InputStream in = MainApp.class.getClassLoader().getResourceAsStream(CONFIG_FILE_NAME);
    Properties configuration = new Properties();
    try {
      configuration.load(in);
      in.close();
    } catch (IOException e) {
      LOG.error("Error while loading configuration from file.", e);
    }
    return configuration;
  }
}
