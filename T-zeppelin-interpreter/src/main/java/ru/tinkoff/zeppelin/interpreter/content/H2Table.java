package ru.tinkoff.zeppelin.interpreter.content;

import java.util.ArrayList;

public class H2Table<T> {
  private H2TableHeader header;
  private H2TableMetadata metadata;
  private ArrayList<ArrayList<Object>> table;

  public H2Table(final H2TableHeader header, final H2TableMetadata metadata, final ArrayList<ArrayList<Object>> table) {
    this.header = header;
    this.metadata = metadata;
    this.table = table;
  }

  public H2TableHeader getHeader() {
    return header;
  }

  public void setHeader(final H2TableHeader header) {
    this.header = header;
  }

  public H2TableMetadata getMetadata() {
    return metadata;
  }

  public void setMetadata(final H2TableMetadata metadata) {
    this.metadata = metadata;
  }

  public ArrayList<ArrayList<Object>> getTable() {
    return table;
  }

  public void setTable(final ArrayList<ArrayList<Object>> table) {
    this.table = table;
  }
}
