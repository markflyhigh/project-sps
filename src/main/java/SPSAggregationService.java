import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.LongAdder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

public class SPSAggregationService {

  private static final Logger LOG = LoggerFactory.getLogger(SPSAggregationService.class);

  private final String WINDOW_SIZE_MILLIS_PROPERTY = "window_size_millis";
  private final String WORKER_THREAD_POOL_SIZE_PROPERTY = "worker_thread_pool_size";
  private final String OUTPUT_FILE_PATH_PROPERTY = "output_file_path";
  private final String ENABLE_CONSOLE_OUTPUT_PROPERTY = "enable_console_output";
  private final String ENABLE_FILE_OUTPUT_PROPERTY = "enable_file_output";

  private final int windowSizeMillis;
  private final int WorkerThreadPoolSize;
  private final String outputFilePath;
  private final boolean enableConsoleOutput;
  private final boolean enableFileOutput;

  private ConcurrentMap<Long, ConcurrentMap<RawEvent, LongAdder>> eventFrequencyPerSecond;
  private ThreadPoolExecutor executor;

  public SPSAggregationService(Properties configuration) {
    this.windowSizeMillis = Integer.parseInt(configuration.getProperty(WINDOW_SIZE_MILLIS_PROPERTY));
    this.WorkerThreadPoolSize = Integer.parseInt(configuration.getProperty(WORKER_THREAD_POOL_SIZE_PROPERTY));
    this.outputFilePath = configuration.getProperty(OUTPUT_FILE_PATH_PROPERTY);
    this.enableConsoleOutput = Boolean.parseBoolean(configuration.getProperty(ENABLE_CONSOLE_OUTPUT_PROPERTY));
    this.enableFileOutput = Boolean.parseBoolean(configuration.getProperty(ENABLE_FILE_OUTPUT_PROPERTY));

    this.eventFrequencyPerSecond = new ConcurrentHashMap<>();
    this.executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(WorkerThreadPoolSize);
  }

  /**
   * Run service
   * 
   * @param eventQueue
   */
  public void run(BlockingQueue<String> eventQueue) {
    LOG.info("Start running the Starts Per Second Aggregation Service.");

    // 1. Generate a list of process threads
    List<WorkerThread> pThreads = generateProcessThreads(eventQueue, WorkerThreadPoolSize);
    for (WorkerThread pThread : pThreads) {
      executor.execute(pThread);
    }

    try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFilePath))) {
      while (true) {
        monitor();

        // 2. Process events in each time window
        List<SpsEvent> completedEvents = processCompletedWindows();

        // 3. Output Starts-Per-Second events
        output(completedEvents, writer);

        // 4. Wait for one second
        Thread.sleep(1000);
      }
    } catch (InterruptedException ie) {
      LOG.error("InterruptedException: ", ie);
    } catch (IOException ioe) {
      LOG.error("IOException: ", ioe);
    } finally {
      stop();
    }
  }

  /**
   * Terminate the service
   */
  public void stop() {
    LOG.info("Terminate the Starts Per Second Aggregation Service.");
    executor.shutdownNow();
  }

  private List<WorkerThread> generateProcessThreads(BlockingQueue<String> eventQueue, int size) {
    List<WorkerThread> list = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      list.add(new WorkerThread(eventQueue, eventFrequencyPerSecond, windowSizeMillis));
    }
    return list;
  }

  private List<SpsEvent> processCompletedWindows() {
    List<SpsEvent> completedEvents = new ArrayList<>();

    long latestWindowTimestamp = 0;
    for (long oldWindowTimestamp : eventFrequencyPerSecond.keySet()) {
      latestWindowTimestamp = Math.max(latestWindowTimestamp, oldWindowTimestamp);
    }

    for (long oldWindowTimestamp : eventFrequencyPerSecond.keySet()) {
      if (oldWindowTimestamp + windowSizeMillis < latestWindowTimestamp) {
        Map<RawEvent, LongAdder> removedEvents = eventFrequencyPerSecond.remove(oldWindowTimestamp);
        completedEvents.addAll(format(removedEvents));
      }
    }

    Collections.sort(completedEvents, (e1, e2) -> Long.compare(e1.windowTime, e2.windowTime));
    
    return completedEvents;
  }

  private void monitor() {
    LOG.info("[monitor] frequency map size: {}", eventFrequencyPerSecond.size());
    LOG.info("[monitor] [{}/{}] Active: {}, Completed: {}, Task: {}, isShutdown: {}, isTerminated: {}",
            executor.getPoolSize(), executor.getCorePoolSize(), executor.getActiveCount(),
            executor.getCompletedTaskCount(), executor.getTaskCount(), executor.isShutdown(), executor.isTerminated());
  }

  private Set<SpsEvent> format(Map<RawEvent, LongAdder> spsEvents) {
    Set<SpsEvent> jsonEvents = new HashSet<>();
    for (RawEvent re : spsEvents.keySet()) {
      SpsEvent se = EventUtils.buildSpsEvent(re, (int) spsEvents.get(re).longValue());
      jsonEvents.add(se);
    }

    return jsonEvents;
  }

  private void output(List<SpsEvent> events, BufferedWriter writer) {
    List<String> outputs = new ArrayList<>();
    ObjectMapper mapper = new ObjectMapper();
    for (SpsEvent e : events) {
      outputs.add(EventUtils.spsEventToJson(mapper, e));
    }

    if (enableConsoleOutput) {
      outputToConsoleOutput(outputs);
    }
    if (enableFileOutput) {
      outputToFile(outputs, writer);
    }
  }

  private void outputToFile(List<String> data, BufferedWriter writer) {
    try {
      String content = String.join("\n", data);
      writer.write(content);
      writer.write("\n");
      writer.flush();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void outputToConsoleOutput(List<String> data) {
    LOG.info(String.join("\n", data));
  }
}
