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
package com.addthis.ccompressor.core;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import com.addthis.ccompressor.Column;
import com.addthis.ccompressor.ColumnRowCompressor;
import com.addthis.ccompressor.ColumnRowReader;
import com.addthis.ccompressor.DefaultColumn;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.core.list.ListBundle;
import com.addthis.bundle.core.list.ListBundleFormat;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DefaultColumnTest {

    @Test
    public void testColumnCompress() throws IOException {
        ListBundleFormat format = new ListBundleFormat();
        BundleField bundleField = format.getField("foo");
        DefaultColumn defaultColumn = new DefaultColumn(bundleField.getName());
        byte[] helloBytes = ValueFactory.create("hello world").asBytes().asNative();
        defaultColumn.push(helloBytes);
        defaultColumn.push(helloBytes);
        defaultColumn.push(helloBytes);
        defaultColumn.push(helloBytes);

        byte[] compressed = defaultColumn.flush();
        ByteArrayInputStream bis = new ByteArrayInputStream(compressed);
        bis.read(new byte[9], 0, 9);
        List<byte[]> columnValues = defaultColumn.decodeBytes(bis);
        for (byte[] value : columnValues) {
            assertArrayEquals(value, helloBytes);
        }
    }

    @Test
    public void testWrite() throws Exception {
        ListBundleFormat format = new ListBundleFormat();
        List<Column> columns = new ArrayList<>();
        columns.add(new DefaultColumn("default"));
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        int blockSize = 1000;
        ColumnRowCompressor columnRowCompressor = new ColumnRowCompressor(columns, blockSize);
        int bosSize = 0;
        int bundles = 10005;
        List<String> values = Arrays.asList("5", "123342", "4", "", "55", null, "24234", "32242");
        for (int i = 0; i < bundles; i++) {
            Bundle b = new ListBundle(format);
            b.setValue(format.getField("default"), ValueFactory.create(values.get(i % values.size())));
            columnRowCompressor.write(b, dos);
            if (i % blockSize == 0 && i != 0) {
                assertTrue(dos.size() > bosSize);
                bosSize = dos.size();
            }
        }
        columnRowCompressor.flush(dos);
        byte[] bytes = bos.toByteArray();
        ColumnRowReader reader = new ColumnRowReader(new ByteArrayInputStream(bytes), new String[]{"default"});
        Iterator<Bundle> results = reader.iterator();
        int resultCount = 0;
        while (results.hasNext()) {
            Bundle b = results.next();
            ValueObject value = b.getValue(b.getFormat().getField("default"));
            String valueString = (value == null) ? null : value.asString().asNative();
            assertEquals(valueString, values.get(resultCount % values.size()));
            resultCount++;
        }
        assertEquals(bundles, resultCount);
    }
}
