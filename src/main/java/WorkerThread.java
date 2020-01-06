import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

public class WorkerThread implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(WorkerThread.class);

  private static final String INVALID_EVENT_TITLE = "busted data";

  private BlockingQueue<String> eventQueue;
  private ConcurrentMap<Long, ConcurrentMap<RawEvent, LongAdder>> windows;
  private ObjectMapper mapper;
  private final int windowSize;

  public WorkerThread(BlockingQueue<String> eventQueue,
      ConcurrentMap<Long, ConcurrentMap<RawEvent, LongAdder>> countMap, int windowSize) {
    this.eventQueue = eventQueue;
    this.windows = countMap;
    this.windowSize = windowSize;
    this.mapper = new ObjectMapper();
  }

  @Override
  public void run() {
    try {
      while (true) {
        // 1. Parse json inputs
        String rawInput = eventQueue.take();
        String jsonEvent = EventUtils.parseRawJson(rawInput);
        RawEvent event = EventUtils.jsonToRawEvent(mapper, jsonEvent);

        // 2. Filter invalid data
        if (!isValidAndSuccess(event))
          continue;

        // 3. Aggregate events
        event.windowTime = event.time - event.time % windowSize;
        windows.putIfAbsent(event.windowTime, new ConcurrentHashMap<RawEvent, LongAdder>());

        ConcurrentMap<RawEvent, LongAdder> countMap = windows.get(event.windowTime);
        countMap.computeIfAbsent(event, k -> new LongAdder()).increment();
      }
    } catch (InterruptedException e) {
      LOG.error("InterruptedException: ", e);
    }
  }

  private boolean isValidAndSuccess(RawEvent e) {
    if (e == null || e.title.startsWith(INVALID_EVENT_TITLE)) {
      return false;
    }
    return e.sev.equals("success");
  }

}
