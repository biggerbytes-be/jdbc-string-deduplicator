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

import java.text.NumberFormat;
import java.util.concurrent.atomic.AtomicInteger;

public class DedupInfo implements DedupInfoMBean {
    protected final AtomicInteger dedupCounter = new AtomicInteger(0);
    protected final AtomicInteger stringCounter = new AtomicInteger(0);

    protected NumberFormat format = NumberFormat.getPercentInstance();

    @Override
    public boolean isEnabled() {
        return settings.isEnabled();
    }

    @Override
    public void setEnabled(boolean b) {
        settings.setEnabled(b);
    }

    public void setInternStrings(boolean internStrings) {
        settings.setInterningStrings(internStrings);
    }

    public void setLargeString(int largeString) {
        settings.setLargeString(largeString);
    }

    public boolean isInternStrings() {
        return settings.isInterningStrings();
    }

    public int getLargeString() {
        return settings.getLargeString();
    }

    private final DedupSettings settings;

    public DedupInfo(DedupSettings settings) {
        this.settings = settings;
    }

    @Override
    public int getDedups() {
        return dedupCounter.get();
    }

    @Override
    public int getStrings() {
        return stringCounter.get();
    }

    @Override
    public String getDedupRatio() {
        int s = getStrings();
        int d = getDedups();
        double r = (s > 0) ? ((double) d) / s : 0.0;

        return format.format(r);
    }

    @Override
    public void reset() {
        dedupCounter.set(0);
        stringCounter.set(0);
    }

    public void inc() {
        dedupCounter.incrementAndGet();
    }

    @Override
    public void incString() {
        stringCounter.incrementAndGet();
    }
}
