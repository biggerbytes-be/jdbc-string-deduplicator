/*
 * Copyright 2013-2018 Biggerbytes.be
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
 *
 */

package be.biggerbytes.jdbc.deduplicator;

import java.util.Properties;

public class DedupSettings {
    public static final int _1MB = 1024 * 1024;
    public static final String DEDUP_INTERN_STRINGS = "DEDUP_INTERN_STRINGS";
    public static final String DEDUP_LARGE_STRING = "DEDUP_LARGE_STRING";
    public static final String DEDUP_MAX_RECENT = "DEDUP_MAX_RECENT";
    public static final String DEDUP_ENABLE = "DEDUP_ENABLE";

    protected boolean interningStrings = true;
    protected int largeString = _1MB;
    private int maxRecent = 100;
    protected boolean enabled = true;

    public DedupSettings(Properties properties) {
        String v;
        v = properties.getProperty(DEDUP_INTERN_STRINGS);
        if (v != null) {
            interningStrings = Boolean.parseBoolean(v);
        }

        //
        v = properties.getProperty(DEDUP_LARGE_STRING);
        if (v != null) {
            largeString = Integer.valueOf(v);
        }

        //
        v = properties.getProperty(DEDUP_MAX_RECENT);
        if (v != null) {
            maxRecent = Integer.valueOf(v);
        }

        v = properties.getProperty(DEDUP_ENABLE);
        if (v != null) {
            enabled= Boolean.valueOf(v);
        }
    }

    public boolean isInterningStrings() {
        return interningStrings;
    }

    public int getLargeString() {
        return largeString;
    }

    public void setInterningStrings(boolean interningStrings) {
        this.interningStrings = interningStrings;
    }

    public void setLargeString(int largeString) {
        this.largeString = largeString;
    }

    public int getMaxRecent() {
        return maxRecent;
    }

    public void setMaxRecent(int maxRecent) {
        this.maxRecent = maxRecent;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }
}
