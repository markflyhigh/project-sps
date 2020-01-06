import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventInjector implements Runnable{

  private static final Logger LOG = LoggerFactory.getLogger(EventInjector.class);

  private BlockingQueue<String> eventQueue;
  private final String streamSourceUrl;

  public EventInjector(String streamSourceUrl, BlockingQueue<String> queue) {
    this.streamSourceUrl = streamSourceUrl;
    this.eventQueue = queue;
  }
  
  @Override
  public void run() {
    LOG.info("Start injecting streaming data to event queue from {}", streamSourceUrl);

    try {
      URL url = new URL(streamSourceUrl);
      BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream()));
      String inputLine;

      while ((inputLine = reader.readLine()) != null) {
        inputLine = inputLine.trim();
        if (!inputLine.isEmpty()) {
          eventQueue.put(inputLine);
        }
      }

      reader.close();

    } catch (MalformedURLException me) {
      LOG.error("MalformedURLException: ", me);
    } catch (IOException ioe) {
      LOG.error("IOException: ", ioe);
    } catch (InterruptedException ie) {
      LOG.error("InterruptedException: ", ie);
    }
  }
}
