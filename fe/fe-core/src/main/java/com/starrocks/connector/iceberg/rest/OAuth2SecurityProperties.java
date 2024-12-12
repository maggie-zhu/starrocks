// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.connector.iceberg.rest;

import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.auth.OAuth2Properties;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class OAuth2SecurityProperties {
    private final Map<String, String> securityProperties;

    public OAuth2SecurityProperties(OAuth2SecurityConfig securityConfig) {
        requireNonNull(securityConfig, "securityConfig is null");

        ImmutableMap.Builder<String, String> propertiesBuilder = ImmutableMap.builder();
        if (securityConfig.getSecurity() == SecurityEnum.OAUTH2) {
            securityConfig.getCredential().ifPresent(
                    credential -> {
                        propertiesBuilder.put(OAuth2Properties.CREDENTIAL, credential);
                        securityConfig.getScope()
                                .ifPresent(scope -> propertiesBuilder.put(OAuth2Properties.SCOPE, scope));
                    });
            securityConfig.getToken().ifPresent(
                    value -> propertiesBuilder.put(OAuth2Properties.TOKEN, value));
            securityConfig.getServerUri().ifPresent(
                    value -> propertiesBuilder.put(OAuth2Properties.OAUTH2_SERVER_URI, value.toString()));
        }

        this.securityProperties = propertiesBuilder.buildOrThrow();
    }

    public Map<String, String> get() {
        return securityProperties;
    }
}