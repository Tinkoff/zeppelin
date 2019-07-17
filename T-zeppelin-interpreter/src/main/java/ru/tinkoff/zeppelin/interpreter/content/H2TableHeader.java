package ru.tinkoff.zeppelin.interpreter.content;

public class H2TableHeader {

  private final String name;
  private final H2TableType type;

  public H2TableHeader(final String name, final H2TableType type) {
    this.name = name;
    this.type = type;
  }

  public String getName() {
    return name;
  }

  public String getTypeName() {
    return type.getSchemaName();
  }
}
