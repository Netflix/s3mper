/*
 *
 *  Copyright 2013 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */


package com.netflix.bdp.s3mper.alert.impl;

import java.util.List;

/**
 * Message that describes an inconsistent listing.
 * 
 * @author dweeks
 */
public class S3ConsistencyMessage extends AbstractMessage {

    private List<String> paths;
    private boolean truncated;
    private boolean recovered;
    private int missingFiles;

    public S3ConsistencyMessage() {
        super();
    }

    public List<String> getPaths() {
        return paths;
    }

    public void setPaths(List<String> paths) {
        this.paths = paths;
    }

    public boolean isRecovered() {
        return recovered;
    }

    public void setRecovered(boolean recovered) {
        this.recovered = recovered;
    }

    public boolean isTruncated() {
        return truncated;
    }

    public void setTruncated(boolean truncated) {
        this.truncated = truncated;
    }

    public int getMissingFiles() {
        return missingFiles;
    }

    public void setMissingFiles(int missingFiles) {
        this.missingFiles = missingFiles;
    }
}
