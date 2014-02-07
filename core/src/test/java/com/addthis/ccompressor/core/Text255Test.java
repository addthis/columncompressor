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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import com.addthis.ccompressor.Column;
import com.addthis.ccompressor.ColumnRowCompressor;
import com.addthis.ccompressor.ColumnRowReader;
import com.addthis.ccompressor.Text255Column;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.list.ListBundle;
import com.addthis.bundle.core.list.ListBundleFormat;
import com.addthis.bundle.value.ValueFactory;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class Text255Test {

    @Test
    public void testWrite() throws Exception {
        ListBundleFormat format = new ListBundleFormat();
        List<Column> columns = new ArrayList<>();
        columns.add(new Text255Column("textColumn"));
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        int blockSize = 1000;
        ColumnRowCompressor columnRowCompressor = new ColumnRowCompressor(columns, blockSize);
        int bosSize = 0;
        int bundles = 10005;
        List<String> values = Arrays.asList("5", "123342", "4", "55", "24234", "32242");
        for (int i = 0; i < bundles; i++) {
            Bundle b = new ListBundle(format);
            b.setValue(format.getField("textColumn"), ValueFactory.create(values.get(i % values.size())));
            columnRowCompressor.write(b, dos);
            if (i % blockSize == 0 && i != 0) {
                assertTrue(dos.size() > bosSize);
                bosSize = dos.size();
            }
        }
        columnRowCompressor.flush(dos);
        byte[] bytes = bos.toByteArray();
        ColumnRowReader reader = new ColumnRowReader(new ByteArrayInputStream(bytes), new String[]{"textColumn"});
        Iterator<Bundle> results = reader.iterator();
        int resultCount = 0;
        while (results.hasNext()) {
            Bundle b = results.next();
            assertEquals(b.getValue(b.getFormat().getField("textColumn")).asString().toString(), values.get(resultCount % values.size()));
            resultCount++;
        }
        assertEquals(bundles, resultCount);
    }
}
