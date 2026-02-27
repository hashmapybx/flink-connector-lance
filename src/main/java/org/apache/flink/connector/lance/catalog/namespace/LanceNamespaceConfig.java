/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.lance.catalog.namespace;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Configuration for Lance Namespace integration.
 * 
 * Supports:
 * - Namespace implementation selection (dir, rest, custom)
 * - Implementation-specific parameters (root path, REST URI)
 * - Extra level configuration (for Spark compatibility)
 * - Parent prefix support (for Hive 3 compatibility)
 */
public class LanceNamespaceConfig {
    
    // Configuration keys
    public static final String KEY_IMPL = "impl";
    public static final String KEY_ROOT = "root";
    public static final String KEY_URI = "uri";
    public static final String KEY_EXTRA_LEVEL = "extra_level";
    public static final String KEY_PARENT = "parent";
    public static final String KEY_PARENT_DELIMITER = "parent_delimiter";
    
    private final String impl;
    private final Map<String, String> properties;
    private final Optional<String> extraLevel;
    private final Optional<String> parent;
    private final String parentDelimiter;
    
    /**
     * Create configuration from properties map.
     */
    public static LanceNamespaceConfig from(Map<String, String> properties) {
        return new LanceNamespaceConfig(properties);
    }
    
    /**
     * Create builder for configuration.
     */
    public static Builder builder() {
        return new Builder();
    }
    
    /**
     * Private constructor.
     */
    private LanceNamespaceConfig(Map<String, String> properties) {
        this.properties = new HashMap<>(Objects.requireNonNull(properties, "Properties cannot be null"));
        
        // Extract required impl
        this.impl = properties.get(KEY_IMPL);
        if (this.impl == null || this.impl.isEmpty()) {
            throw new IllegalArgumentException("Missing required configuration: " + KEY_IMPL);
        }
        
        // Extract optional extra level
        String extraLevelValue = properties.get(KEY_EXTRA_LEVEL);
        this.extraLevel = extraLevelValue != null && !extraLevelValue.isEmpty() ?
                Optional.of(extraLevelValue) : Optional.empty();
        
        // Extract optional parent prefix
        String parentValue = properties.get(KEY_PARENT);
        this.parent = parentValue != null && !parentValue.isEmpty() ?
                Optional.of(parentValue) : Optional.empty();
        
        // Extract parent delimiter
        this.parentDelimiter = properties.getOrDefault(KEY_PARENT_DELIMITER, ".");
    }
    
    /**
     * Get namespace implementation type.
     */
    public String getImpl() {
        return impl;
    }
    
    /**
     * Get all configuration properties.
     */
    public Map<String, String> getProperties() {
        return Collections.unmodifiableMap(properties);
    }
    
    /**
     * Get root path for directory namespace implementation.
     */
    public Optional<String> getRoot() {
        return Optional.ofNullable(properties.get(KEY_ROOT));
    }
    
    /**
     * Get URI for REST namespace implementation.
     */
    public Optional<String> getUri() {
        return Optional.ofNullable(properties.get(KEY_URI));
    }
    
    /**
     * Get extra level configuration (for Spark compatibility).
     */
    public Optional<String> getExtraLevel() {
        return extraLevel;
    }
    
    /**
     * Get parent prefix configuration (for Hive 3 compatibility).
     */
    public Optional<String> getParent() {
        return parent;
    }
    
    /**
     * Get parent delimiter.
     */
    public String getParentDelimiter() {
        return parentDelimiter;
    }
    
    /**
     * Get parent prefix as array.
     */
    public Optional<String[]> getParentArray() {
        return parent.map(p -> p.split(java.util.regex.Pattern.quote(parentDelimiter)));
    }
    
    /**
     * Check if directory namespace implementation.
     */
    public boolean isDirectoryNamespace() {
        return "dir".equals(impl);
    }
    
    /**
     * Check if REST namespace implementation.
     */
    public boolean isRestNamespace() {
        return "rest".equals(impl);
    }
    
    /**
     * Check if extra level should be automatically configured.
     */
    public boolean shouldAutoConfigureExtraLevel() {
        return !extraLevel.isPresent() && isDirectoryNamespace();
    }
    
    @Override
    public String toString() {
        return "LanceNamespaceConfig{" +
                "impl='" + impl + '\'' +
                ", extraLevel=" + extraLevel +
                ", parent=" + parent +
                ", properties=" + properties +
                '}';
    }
    
    /**
     * Builder for LanceNamespaceConfig.
     */
    public static class Builder {
        private final Map<String, String> properties = new HashMap<>();
        
        public Builder impl(String impl) {
            properties.put(KEY_IMPL, impl);
            return this;
        }
        
        public Builder root(String root) {
            properties.put(KEY_ROOT, root);
            return this;
        }
        
        public Builder uri(String uri) {
            properties.put(KEY_URI, uri);
            return this;
        }
        
        public Builder extraLevel(String extraLevel) {
            properties.put(KEY_EXTRA_LEVEL, extraLevel);
            return this;
        }
        
        public Builder parent(String parent) {
            properties.put(KEY_PARENT, parent);
            return this;
        }
        
        public Builder parentDelimiter(String delimiter) {
            properties.put(KEY_PARENT_DELIMITER, delimiter);
            return this;
        }
        
        public Builder property(String key, String value) {
            properties.put(key, value);
            return this;
        }
        
        public Builder properties(Map<String, String> props) {
            properties.putAll(props);
            return this;
        }
        
        public LanceNamespaceConfig build() {
            return new LanceNamespaceConfig(properties);
        }
    }
}
