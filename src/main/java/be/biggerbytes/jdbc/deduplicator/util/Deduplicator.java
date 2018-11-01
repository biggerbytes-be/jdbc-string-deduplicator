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

package be.biggerbytes.jdbc.deduplicator.util;

import be.biggerbytes.jdbc.deduplicator.DedupSettings;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * String deduplication happens in this helper class.
 */
public class Deduplicator {
    private final DedupSettings settings;
    private final DedupInfo mbean;

    /**
     * Recently encountered instances are kept in this size limited Map. The entries
     * in this Map are sorted based on their access order
     */
    Map<String, String> recent;


    public Deduplicator(DedupContext context) {
        this.mbean = context.dedupMBean;
        this.settings = context.settings;
        recent = create(settings.getMaxRecent());
        init();
    }

    /**
     * Initialize if needed
     */
    protected void init() {

    }

    /**
     * Creates a Map from String to String with limited size and using a LRU eviction policy
     *
     * @param max
     * @return
     */
    private Map<String, String> create(final int max) {
        return new LinkedHashMap<String, String>(max, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
                return size() > max;
            }
        };
    }

    /**
     * Deduplicates String instance by looking for an equal (as in equals() == true) instance in a Map of
     * recently encountered instances. The Map is:
     * <ul>
     *     <li>Limited in size (can be configured {@link DedupSettings#getMaxRecent()})</li>
     *     <li>Is a LRU map updated based on the access order</li>
     *     <li>Large String instances are not put in the map (can be configured {@link DedupSettings#getLargeString()}) </li>
     *     <li>String tnstances are interned before being put in the map (can be configured {@link DedupSettings#isInterningStrings()} ()})</li>
     * </ul>
     *
     * The different parameters can be adjusted by passing properties to jdbc driver.
     * See {@link DedupSettings#DEDUP_INTERN_STRINGS}, {@link DedupSettings#DEDUP_LARGE_STRING} and
     * {@link DedupSettings#DEDUP_MAX_RECENT}
     *
     * @param s the String to deduplicate
     * @return the same String s if not previously encountered or a previous instance which represents the same string
     */
    public String dedup(String s) {
        if (!settings.isEnabled())
            return s;

        if (s == null) {
            return null;
        }

        mbean.incString();

        // Don't hang on to too large strings
        if (s.length() > settings.getLargeString()) {
            return s;
        }

        String prev = recent.get(s);
        if (prev != null) {
            // Reference comparison here!
            // we don't want to report deduplications if the driver already
            // did the work
            if (prev != s)
                mbean.inc();

            return prev;
        }

        // Use the JVM wide cache of String instances
        // If other Deduplicator instance already deduplicated the string
        // then the instance may very well be already in there
        //
        // Older JVMs didn't GC these instances which ended up hurting memory
        // Disable string interning in that case
        //
        if (settings.isInterningStrings())
            s = s.intern();

        recent.put(s, s);
        return s;
    }

    /**
     *
     */
    public void optimize() {
        recent.clear();
    }
}
