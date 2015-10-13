/**
 * Licensed to DigitalPebble Ltd under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * DigitalPebble licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.digitalpebble.storm.crawler.spout;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.Scheme;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;

/**
 * Reads the lines from a UTF-8 file and use them as a spout. Load the entire
 * content into memory
 */
@SuppressWarnings("serial")
public class FileSpout extends BaseRichSpout {

    private SpoutOutputCollector _collector;
    private String[] _inputFiles;
    private Scheme _scheme;
    private int _inputPos = -1;
    private LineIterator iterator = null;

    private LinkedList<byte[]> toPut = new LinkedList<byte[]>();
    private boolean active;

    public static final Logger LOG = LoggerFactory.getLogger(FileSpout.class);

    public FileSpout(String dir, String filter, Scheme scheme) {
        Path pdir = Paths.get(dir);
        List<String> f = new LinkedList<String>();
        try (DirectoryStream<Path> stream = Files
                .newDirectoryStream(pdir, filter)) {
            for (Path entry : stream) {
                String inputFile = entry.toAbsolutePath().toString();
                f.add(inputFile);
                LOG.info("Input : {}", inputFile);
            }
        } catch (IOException ioe) {
            LOG.error("IOException: %s%n", ioe);
        }
        _inputFiles = f.toArray(new String[f.size()]);
        _scheme = scheme;
    }

    public FileSpout(String file, Scheme scheme) {
        this(scheme, file);
    }

    public FileSpout(Scheme scheme, String... files) {
        if (files.length == 0) {
            throw new IllegalArgumentException(
                    "Must configure at least one inputFile");
        }
        _scheme = scheme;
        _inputFiles = files;
    }

    private LineIterator updateIterator() {
        _inputPos++; // move to the next file

        try {
            iterator = FileUtils
                    .lineIterator(Paths.get(_inputFiles[_inputPos]).toFile(),
                            "UTF-8");

            return iterator;
        } catch (IOException e) {
            // This file couldn't be processed
            System.err.format("IOException: %s%n", e);
        }

        return null;
    }

    private String nextLine() {
        try {
            String line;

            while ((line = iterator.nextLine()) != null && StringUtils
                    .isBlank(line))
                continue;

            return line;
        } catch (NoSuchElementException e) {
            if (_inputPos < _inputFiles.length - 1) {
                updateIterator();
            } else {
                return null; // no more lines or files
            }
        }

        return null;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void open(Map conf, TopologyContext context,
            SpoutOutputCollector collector) {
        _collector = collector;

        updateIterator();
    }

    @Override
    public void nextTuple() {
        if (!active)
            return;

        String line = nextLine();

        if (StringUtils.isBlank(line)) {
            return;
        }

        List<Object> fields = this._scheme.deserialize(line.getBytes());
        this._collector.emit(fields, fields.get(0).toString());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(_scheme.getOutputFields());
    }

    @Override
    public void close() {
    }

    @Override
    public void activate() {
        super.activate();
        active = true;
    }

    @Override
    public void deactivate() {
        super.deactivate();
        active = false;
    }
}
