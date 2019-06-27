package ru.tinkoff.zeppelin.core.content;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

public class TableContent extends Content {

  private enum Fields {
    TABLE_COLUMNS(new TypeToken<ArrayList<String>>() {}.getType()),
    ;

    private final Type type;

    Fields(final Type type) {
      this.type = type;
    }

    public Type getType() {
      return type;
    }
  }

  TableContent(final Content content) {
    super(content.getNoteId(), content.getType(), content.getDescription(), content.getLocation(), content.getParams());
  }

  public List<String> getColumns() {
    return new Gson().fromJson(getParams().get(Fields.TABLE_COLUMNS.name()), Fields.TABLE_COLUMNS.getType());
  }

}
