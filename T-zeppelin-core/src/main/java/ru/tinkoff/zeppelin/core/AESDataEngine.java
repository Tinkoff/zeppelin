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

package ru.tinkoff.zeppelin.core;

import org.apache.commons.lang3.exception.ExceptionUtils;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.spec.KeySpec;
import java.util.Base64;

public class AESDataEngine {

  private final Cipher cipherEncrypt;
  private final Cipher cipherDecrypt;

  private static volatile AESDataEngine instance;

  public static void init(final String secret,
                          final String salt,
                          final byte[] iv) throws GeneralSecurityException {

    synchronized (AESDataEngine.class) {
      if (instance == null) {
        instance = new AESDataEngine(secret, salt, iv);
      } else {
        throw new GeneralSecurityException("AESDataEngine already init");
      }
    }
  }

  public static AESDataEngine getInstance() {
    synchronized (AESDataEngine.class) {
      if (instance == null) {
        throw new RuntimeException("AESDataEngine not init");
      } else {
        return instance;
      }
    }
  }

  private AESDataEngine(final String secret,
                        final String salt,
                        final byte[] iv)
          throws GeneralSecurityException {
    IvParameterSpec ivspec = new IvParameterSpec(iv);

    SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");
    KeySpec spec = new PBEKeySpec(
            secret.toCharArray(),
            salt.getBytes(),
            65536,
            256
    );
    SecretKey tmp = factory.generateSecret(spec);
    SecretKeySpec secretKey = new SecretKeySpec(tmp.getEncoded(), "AES");

    cipherEncrypt = Cipher.getInstance("AES/CBC/PKCS5Padding");
    cipherEncrypt.init(Cipher.ENCRYPT_MODE, secretKey, ivspec);

    cipherDecrypt = Cipher.getInstance("AES/CBC/PKCS5Padding");
    cipherDecrypt.init(Cipher.DECRYPT_MODE, secretKey, ivspec);
  }

  public String encrypt(final String strToEncrypt) {
    try {
      return Base64.getEncoder().encodeToString(cipherEncrypt.doFinal(strToEncrypt.getBytes(StandardCharsets.UTF_8)));
    } catch (Exception e) {
      throw new RuntimeException(ExceptionUtils.getMessage(e) + " :  " + ExceptionUtils.getStackTrace(e));
    }
  }

  public String decrypt(final String strToDecrypt) {
    try {
      return new String(cipherDecrypt.doFinal(Base64.getDecoder().decode(strToDecrypt)));
    } catch (Exception e) {
      throw new RuntimeException(ExceptionUtils.getMessage(e) + " :  " + ExceptionUtils.getStackTrace(e));
    }
  }
}
