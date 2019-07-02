package ru.tinkoff.zeppelin.engine.forms;

import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class FormsProcessor {

  public static class InjectResponse {
    private final String payload;
    private final int cursorPosition;

    public InjectResponse(final String payload, final int cursorPosition) {
      this.payload = payload;
      this.cursorPosition = cursorPosition;
    }

    public String getPayload() {
      return payload;
    }

    public int getCursorPosition() {
      return cursorPosition;
    }
  }

  private static final Pattern FORM_BLOCK_PATTERN =
          Pattern.compile("%FORM(.*)%FORM[\n ]*", Pattern.DOTALL);

  public static String injectFormValues(final String payload, final Map<String, Object> formValues) {
    String text = FORM_BLOCK_PATTERN.matcher(payload).replaceAll("");

    if (formValues.isEmpty()) {
      return text;
    }
    for (Map.Entry<String, Object> form : formValues.entrySet()) {
      Object value = form.getValue();
      if (value instanceof List) {
        value = String.join(",", (List) value);
      }
      text = text.replaceAll("#" + form.getKey() + "#", value.toString());
    }
    return text;
  }

  public static InjectResponse injectFormValues(final String payload, int cursorPosition, final Map<String, Object> formValues) {

    final String marker = "<<<CURSOR_POSITION>>>";
    final String preparedPayload =
            payload.substring(0, cursorPosition) + marker + payload.substring(cursorPosition);

    final String afterInject = injectFormValues(preparedPayload, formValues);
    final int resultCursorPosition = afterInject.indexOf(marker);
    final String resultPayload = afterInject.replace(marker, StringUtils.EMPTY);

    return new InjectResponse(resultPayload, resultCursorPosition);
  }


}
