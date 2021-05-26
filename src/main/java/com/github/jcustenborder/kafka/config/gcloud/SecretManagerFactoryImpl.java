/**
 * Copyright © 2021 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.config.gcloud;

import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.google.cloud.secretmanager.v1.SecretManagerServiceSettings;
import org.apache.kafka.common.config.ConfigException;

import java.io.IOException;

class SecretManagerFactoryImpl implements SecretManagerFactory {
  @Override
  public SecretManagerServiceClient create(SecretManagerConfigProviderConfig config) {
    try {
      SecretManagerServiceSettings settings = SecretManagerServiceSettings.newBuilder()
          .setCredentialsProvider(config.credentialsProvider())
          .build();
      return SecretManagerServiceClient.create(settings);
    } catch (IOException ex) {
      ConfigException exception = new ConfigException("Exception during configuration");
      exception.initCause(exception);
      throw exception;
    }
  }
}
