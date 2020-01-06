import java.util.Objects;
import java.util.regex.*;

import com.fasterxml.jackson.databind.ObjectMapper;


class RawEvent {
  public String device;
  public String sev;
  public String title;
  public String country;
  public long time;
  public long windowTime;

  @Override
  public boolean equals(Object o) {
    if (o == this)
      return true;
    if (!(o instanceof RawEvent)) {
      return false;
    }
    RawEvent event = (RawEvent) o;
    return Objects.equals(device, event.device) &&
           Objects.equals(title, event.title) &&
           Objects.equals(country, event.country);
    }

  @Override
  public int hashCode() {
    return Objects.hash(device, title, country);
  }
}


class SpsEvent {
  public String device;
  public int sps;
  public String title;
  public String country;
  public long windowTime;
}


class EventUtils {

  private final static String PATTERN = "^data[^{]+(.*})";
  private static Pattern r = Pattern.compile(PATTERN);

  public static String parseRawJson(String rawJson) {
    if (rawJson == null) {
      return null;
    }
    rawJson = rawJson.trim();
    if (rawJson.isEmpty())
      return "";

    Matcher matcher = r.matcher(rawJson);
    String jsonResult = null;
    if (matcher.find()) {
      jsonResult = matcher.group(1);
    }
    return jsonResult;
  }

  public static RawEvent jsonToRawEvent(ObjectMapper mapper, String jsonInput) {
    if (jsonInput == null || jsonInput.isEmpty()) {
      return null;
    }

    try {
      return mapper.readValue(jsonInput, RawEvent.class);
    } catch (Exception e) {
      // Unsupported json input
      throw new RuntimeException(e);
    }
  }

  public static String spsEventToJson(ObjectMapper mapper, SpsEvent event) {
    if (event == null) {
      return null;
    }

    try {
      return mapper.writeValueAsString(event);
    } catch (Exception e) {
      // Unsupported event input
      throw new RuntimeException(e);
    }
  }

  public static SpsEvent buildSpsEvent(RawEvent e, int sps) {
    if (e == null) {
      return null;
    }

    SpsEvent event = new SpsEvent();
    event.device = e.device;
    event.title = e.title;
    event.country = e.country;
    event.windowTime = e.windowTime;
    event.sps = sps;

    return event;
  }
}
