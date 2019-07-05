/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ru.tinkoff.zeppelin.interpreter;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Base64;
import java.util.LinkedList;
import java.util.List;

/**
 * Interpreter result template.
 */
public class InterpreterResult implements Serializable {

  private static final Logger LOGGER = LoggerFactory.getLogger(InterpreterResult.class);

  /**
   *  Type of result after code execution.
   */
  public enum Code {
    SUCCESS,
    ABORTED,
    ERROR
  }


  private Code code;
  private List<Message> msg = new LinkedList<>();

  public InterpreterResult(final Code code) {
    this.code = code;
  }

  public InterpreterResult(final Code code, final List<Message> msgs) {
    this.code = code;
    msg.addAll(msgs);
  }

  public InterpreterResult(final Code code, final Message msgs) {
    this.code = code;
    msg.add(msgs);
  }

  public InterpreterResult add(final Message message) {
    msg.add(message);
    return this;
  }

  public Code code() {
    return code;
  }

  public List<Message> message() {
    return msg;
  }

  public static class Message implements Serializable {

    public enum Type {
      TEXT,
      TEXT_TEMP,
      HTML,
      ANGULAR,
      TABLE,
      IMG,
      SVG,
      NULL,
      NETWORK,
      CONTENT_FILE
    }

    Type type;
    String data;
    String contentFilePath;

    public Message(final Type type, final String data) {
      this.type = type;
      this.data = data;
    }

    public static Message createContentMessage(final String contentFilePath) {
      final Message message = new Message(Type.CONTENT_FILE, null);
      message.contentFilePath = contentFilePath;
      return message;
    }

    public Type getType() {
      return type;
    }

    public String getData() {
      if (data == null && !StringUtils.isEmpty(contentFilePath)) {
        data = loadContentData();
      }

      return data;
    }

    public String getContentFilePath() {
      return contentFilePath;
    }

    private String loadContentData() {
      final File file = new File(contentFilePath);
      try {
        if (type == InterpreterResult.Message.Type.IMG) {
          final String extension = FilenameUtils.getExtension(file.getName()).toLowerCase();
          return String.format(
              "<div style='width:auto;height:auto'>" +
                  "<img src=data:image/%s;base64,%s  style='width=auto;height:auto'/>" +
                  "</div>",
              extension,
              new String(Base64.getEncoder().encode(FileUtils.readFileToByteArray(file))));
        }

        return FileUtils.readFileToString(file, "UTF-8");
      } catch (final IOException e) {
        LOGGER.error("Can't load content file {}", contentFilePath, e);
        contentFilePath = null;
        return "!!!FAILED_THAN_LOAD_CONTENT_FILE!!!";
      }
    }

    public String toString() {
      return "%" + type.name().toLowerCase() + " " + data;
    }
  }
}
