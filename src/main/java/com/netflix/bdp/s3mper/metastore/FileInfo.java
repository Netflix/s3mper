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


package com.netflix.bdp.s3mper.metastore;

import org.apache.hadoop.fs.Path;

/**
 * Provides basic file metadata for the metastore
 *
 * @author dweeks
 */
public class FileInfo {

    private Path path;
    private boolean deleted;
    private boolean directory;

    public FileInfo(Path path) {
        this.path = path;
        this.deleted = false;

        this.directory = false;
    }

    public FileInfo(Path path, boolean deleted, boolean directory) {
        this.path = path;
        this.deleted = deleted;
        this.directory = directory;
    }

    public Path getPath() {
        return path;
    }

    public void setPath(Path path) {
        this.path = path;
    }

    public boolean isDeleted() {
        return deleted;
    }

    public void setDeleted(boolean deleted) {
        this.deleted = deleted;
    }

    public boolean isDirectory() {
        return directory;
    }

    public void setDirectory(boolean directory) {
        this.directory = directory;
    }

    public String toString() {
        return "FileInfo(" + path + "," + deleted + "," + directory+")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FileInfo fileInfo = (FileInfo) o;

        if (deleted != fileInfo.deleted) return false;
        if (directory != fileInfo.directory) return false;
        return path != null ? path.equals(fileInfo.path) : fileInfo.path == null;

    }

    @Override
    public int hashCode() {
        int result = path != null ? path.hashCode() : 0;
        result = 31 * result + (deleted ? 1 : 0);
        result = 31 * result + (directory ? 1 : 0);
        return result;
    }
}
