/*
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

package com.addthis.ccompressor;

import javax.annotation.concurrent.GuardedBy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.io.DataChannelCodec;
import com.addthis.bundle.value.ValueObject;

import com.google.common.collect.Iterables;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is thread safe in the sense that multiple threads calling write should
 * not both be able to actually modify the output stream they passed in at the same time.
 * This is regardless of whether or not they are the same output stream object. It is not
 * okay to modify the columnList after constructing the object though.
 */
public class ColumnRowCompressor implements RowWriter {

    private static final Logger logger = LoggerFactory.getLogger(ColumnRowCompressor.class);
    private static final byte VERSION = 1;

    private final List<Column> columnList;
    private final int blockSize;
    private final BlockingQueue<Bundle> rowBuffer = new LinkedBlockingQueue<>();
    private final Lock flushLock = new ReentrantLock();

    public ColumnRowCompressor(List<Column> columnMetaDataList, int blockSize) {
        this.columnList = columnMetaDataList;
        this.blockSize = blockSize;
    }

    @Override
    public boolean write(Iterable<Bundle> rows, OutputStream outputStream) throws IOException {
        assert (rows != null);
        Iterables.addAll(rowBuffer, rows);
        return maybeFlush(outputStream);
    }

    @Override
    public boolean write(Bundle row, OutputStream outputStream) throws IOException {
        if (row == null) {
            return false;
        }
        rowBuffer.add(row);
        return maybeFlush(outputStream);
    }

    @Override
    public void flush(OutputStream outputStream) throws IOException {
        flushLock.lock();
        try {
            while (!rowBuffer.isEmpty()) {
                singleFlush(outputStream);
            }
        } finally {
            flushLock.unlock();
        }
    }

    private boolean maybeFlush(OutputStream out) throws IOException {
        boolean flushed = false;
        if (rowBuffer.size() >= blockSize) {
            if (flushLock.tryLock()) {
                try {
                    while (rowBuffer.size() >= blockSize) {
                        singleFlush(out);
                        flushed = true;
                    }
                } finally {
                    flushLock.unlock();
                }
            }
        }
        return flushed;
    }

    @GuardedBy("flushLock")
    private void singleFlush(OutputStream outputStream) throws IOException {
        List<Bundle> blockBundles = new ArrayList<>();
        rowBuffer.drainTo(blockBundles, blockSize);
        ByteArrayOutputStream valueOutputStream = new ByteArrayOutputStream();
        DataChannelCodec.ClassIndexMap classIndexMap = DataChannelCodec.createClassIndexMap();
        for (Bundle blockBundle : blockBundles) {
            for (Column column : columnList) {
                ValueObject val = column.getValue(blockBundle);
                switch (column.getColumnType()) {
                    case RAW:
                        DataChannelCodec.encodeValue(val, valueOutputStream, classIndexMap);
                        column.push(valueOutputStream.toByteArray());
                        valueOutputStream.reset();
                        break;
                    case TEXT255:
                    case RUNLENGTH:
                        column.push(val == null ? new byte[0] : val.asString().toString().getBytes());
                        break;
                    case DELTAINT:
                        column.push(val == null ? 0 : ((int) val.asLong().getLong()));
                        break;
                    case DELTALONG:
                        column.push(val == null ? 0L : val.asLong().getLong());
                        break;
                    default:
                        logger.error("Unknown column type {}", column.getColumnType());
                        throw new IOException("Unknown column type " + column.getColumnType());
                }

            }
        }
        int totalValueBytes = 0;
        List<byte[]> columnByteList = new ArrayList<>();
        for (Column column : columnList) {
            byte[] bytes = column.flush();
            totalValueBytes += bytes.length;
            columnByteList.add(bytes);
        }

        // write header
        BlockHeader.writeHeader(VERSION, totalValueBytes, columnList, outputStream);
        // write payload
        for (byte[] bytes : columnByteList) {
            outputStream.write(bytes);
        }
    }
}
