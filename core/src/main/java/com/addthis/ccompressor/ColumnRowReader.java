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

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.addthis.basis.util.Bytes;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleFactory;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.core.BundlePrinter;
import com.addthis.bundle.core.list.ListBundle;
import com.addthis.bundle.io.DataChannelCodec;
import com.addthis.bundle.value.ValueFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO: fix up concurrency for reading bundles
 * TODO: fix up bundle field use to not perform a bundle field lookup per block (minor)
 */
public class ColumnRowReader implements RowReader {

    private static final Logger logger = LoggerFactory.getLogger(ColumnRowReader.class);

    private final InputStream inputStream;
    private final Set<String> fieldFilterSet;
    private final BundleFactory bundleFactory;

    public ColumnRowReader(InputStream inputStream, String[] filterFields, BundleFactory bundleFactory) {
        if (filterFields != null && filterFields.length > 0) {
            fieldFilterSet = new HashSet<>(filterFields.length);
            Collections.addAll(fieldFilterSet, filterFields);
        } else {
            fieldFilterSet = null;
        }
        this.inputStream = inputStream;
        this.bundleFactory = bundleFactory;
    }

    public ColumnRowReader(InputStream inputStream, String[] filterFields) {
        this(inputStream, filterFields, new ListBundle());
    }

    private class RowIterator implements Iterator<Bundle> {

        private final BlockingQueue<Bundle> bundleQueue;

        public RowIterator() {
            bundleQueue = new LinkedBlockingQueue<>();
        }

        @Override
        public boolean hasNext() {
            if (bundleQueue.isEmpty()) {
                readNextBlock();
            }
            return !bundleQueue.isEmpty();
        }

        @Override
        public Bundle next() {
            if (bundleQueue.isEmpty()) {
                readNextBlock();
            }
            return bundleQueue.poll();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Can't remove from underlying source.");
        }

        public synchronized void readNextBlock() {
            try {
                BlockHeader blockHeader = BlockHeader.readHeader(inputStream);
                List<Bundle> bundles = new ArrayList<>(blockHeader.getLength());
                if (blockHeader != null) {
                    List<List<?>> columnByteList = getColumnBytes(blockHeader);
                    if (columnByteList != null && columnByteList.size() > 0) {
                        boolean firstColumn = true;
                        int column = 0;
                        DataChannelCodec.ClassIndexMap classIndexMap = DataChannelCodec.createClassIndexMap();
                        for (List<? extends Object> bytes : columnByteList) {
                            Column columnObj;
                            int rowIndex = 0;
                            while (true) {
                                columnObj = blockHeader.getColumnList().get(column++);
                                if (fieldFilterSet != null && !fieldFilterSet.contains(columnObj.getName())) {
                                    continue;
                                }
                                break;
                            }
                            for (Object columnBytes : bytes) {
                                Bundle bundle;
                                if (firstColumn) {
                                    bundle = bundleFactory.createBundle();
                                    bundles.add(bundle);
                                } else {
                                    bundle = bundles.get(rowIndex++);
                                }
                                BundleField newField = bundle.getFormat().getField(columnObj.getName());
                                switch (columnObj.getColumnType()) {
                                    case RAW:
                                        byte[] val = (byte[]) columnBytes;
                                        if (val.length < 2) {
                                            bundle.setValue(newField, null);
                                        } else {
                                            try {
                                                bundle.setValue(newField, DataChannelCodec.decodeValue(
                                                        new ByteArrayInputStream(val), classIndexMap));
                                            } catch (IOException ex) {
                                                logger.error("Encountered IOException at column \"" +
                                                        columnObj.getName() +
                                                        "\" length of column bytes is " + val.length +
                                                        " at row " + rowIndex + " and column " + column +
                                                        " and bundle contents are " + BundlePrinter.printBundle(bundle));
                                                throw ex;
                                            }
                                        }
                                        break;
                                    case RUNLENGTH:
                                    case TEXT255:
                                        bundle.setValue(newField, ValueFactory.create((String) columnBytes));
                                        break;
                                    case DELTAINT:
                                        bundle.setValue(newField, ValueFactory.create((Integer) columnBytes));
                                        break;
                                    case DELTALONG:
                                        bundle.setValue(newField, ValueFactory.create((Long) columnBytes));
                                        break;
                                    default:
                                        logger.error("Unexpected column type: " + columnObj.getColumnType());
                                        throw new IOException("Unexpected column type: " + columnObj.getColumnType());
                                }
                            }
                            firstColumn = false;
                        }
                        bundleQueue.addAll(bundles);
                    }
                }
            } catch (EOFException eof) {
                // expected
            } catch (IOException e) {
                logger.error("IOException filling cache", e);
            } catch (Exception e) {
                logger.error("QueueSize: + " + bundleQueue.size() + " Unexpected exception", e);
            }
        }

        private List<List<?>> getColumnBytes(BlockHeader blockHeader) throws IOException {
            List<List<?>> columnByteList = new ArrayList<>();
            for (Column column : blockHeader.getColumnList()) {
                ColumnHeader header = AbstractColumn.readHeader(inputStream);
                if (fieldFilterSet != null && !fieldFilterSet.contains(column.getName())) {
                    // need to advance to next block
                    long skippedTotal = 0;
                    while (skippedTotal != header.length) {
                        long skipped = inputStream.skip(header.length - skippedTotal);
                        assert (skipped >= 0);
                        skippedTotal += skipped;
                        if (skipped == 0) {
                            break;
                        }
                    }
                    continue;
                }
                switch (header.columnType) {
                    case RAW:
                        int length = header.length;
                        byte[] bytes = Bytes.readBytes(inputStream, length);
                        columnByteList.add(DefaultColumn.decodeBytes(new ByteArrayInputStream(bytes)));
                        break;
                    case TEXT255:
                        columnByteList.add(Text255Column.readColumnValues(inputStream, header.length));
                        break;
                    case RUNLENGTH:
                        columnByteList.add(RunLengthColumn.readColumnValues(inputStream, header.length));
                        break;
                    case DELTAINT:
                        columnByteList.add(DeltaIntColumn.readColumnValues(inputStream, header.length));
                        break;
                    case DELTALONG:
                        columnByteList.add(DeltaLongColumn.readColumnValues(inputStream, header.length));
                        break;
                    default:
                        logger.error("Unexpected column type: " + header.columnType);
                        throw new IOException("Unexpected column type: " + header.columnType);
                }
            }
            return columnByteList;
        }
    }

    @Override
    public Iterator<Bundle> iterator() {
        return new RowIterator();
    }

    @Override
    public void close() throws IOException {
        if (inputStream != null) {
            inputStream.close();
        }
    }
}
