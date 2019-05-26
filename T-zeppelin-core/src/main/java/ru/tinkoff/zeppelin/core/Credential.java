package ru.tinkoff.zeppelin.core;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Credential is user-information, available in interpolation.
 */
public class Credential implements Serializable {

  private long id;

  @Nonnull
  private final String key;

  @Nonnull
  private String value;

  @Nullable
  private String description;

  /**
   * Users who could delete/edit.
   */
  @Nonnull
  private final Set<String> owners;

  /**
   * Users who could use it in context.
   */
  @Nonnull
  private final Set<String> readers;

  public Credential(@Nonnull final String key,
                    @Nonnull final String value,
                    @Nullable final String description) {
    this.key = key;
    this.value = value;
    this.description = description;
    this.owners = new HashSet<>();
    this.readers = new HashSet<>();
  }

  public void setId(final long id) {
    this.id = id;
  }

  public long getId() {
    return id;
  }

  @Nonnull
  public String getKey() {
    return key;
  }

  @Nonnull
  public String getValue() {
    return value;
  }

  public void setValue(@Nonnull final String value) {
    this.value = value;
  }

  @Nullable
  public String getDescription() {
    return description;
  }

  public void setDescription(@Nullable final String description) {
    this.description = description;
  }

  @Nonnull
  public Set<String> getOwners() {
    return owners;
  }

  @Nonnull
  public Set<String> getReaders() {
    return readers;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", "{", "}")
        .add("id='" + id + "'")
        .add("key='" + key + "'")
        .add("value='" + value + "'")
        .add("description='" + description + "'")
        .add("owners=" + owners)
        .add("readers=" + readers)
        .toString();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Credential)) {
      return false;
    }
    final Credential that = (Credential) o;
    return id == that.id &&
        key.equals(that.key) &&
        value.equals(that.value) &&
        Objects.equals(description, that.description) &&
        owners.equals(that.owners) &&
        readers.equals(that.readers);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, key, value, description, owners, readers);
  }
}
