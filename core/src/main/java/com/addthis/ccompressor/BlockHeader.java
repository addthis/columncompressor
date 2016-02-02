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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import java.util.ArrayList;
import java.util.List;

import java.nio.ByteBuffer;

import com.addthis.basis.util.LessBytes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlockHeader {

    private static final Logger logger = LoggerFactory.getLogger(BlockHeader.class);

    private final int length;
    private final List<Column> columnList;

    private BlockHeader(int length, List<Column> columnList) {
        this.length = length;
        this.columnList = columnList;
    }

    public List<Column> getColumnList() {
        return columnList;
    }

    public int getLength() {
        return length;
    }

    public static BlockHeader readHeader(InputStream inputStream) throws IOException {

        byte[] blockHeader = LessBytes.readBytes(inputStream, 9);
        ByteBuffer bb = ByteBuffer.wrap(blockHeader);
        // not currently used but may be in the future
        byte version = bb.get();
        int length = bb.getInt(1);
        List<Column> columnList = new ArrayList<>();
        int numColumns = bb.getInt(5);
        for (int i = 0; i < numColumns; i++) {
            String fieldName = LessBytes.readString(inputStream);
            int columnInt = LessBytes.readInt(inputStream);
            ColumnType columnType = ColumnType.get(columnInt);
            Column c;
            if (fieldName == null) {
                throw new IOException("Field name for column at offset " +
                        i + " is null.");
            } else if (columnType == null) {
                throw new IOException("Column type for column at offset " +
                        i + " is null. Field name is \"" + fieldName + "\" " +
                        " and column integer code is " + columnInt + ".");
            }
            c = AbstractColumn.createColumn(fieldName, columnType);
            columnList.add(c);
        }
        return new BlockHeader(length, columnList);
    }

    public static void writeHeader(byte VERSION, int totalValueBytes,
                                   List<Column> columnList, OutputStream outputStream) throws IOException {
        outputStream.write(VERSION);
        outputStream.write((totalValueBytes >>> 24) & 0xFF);
        outputStream.write((totalValueBytes >>> 16) & 0xFF);
        outputStream.write((totalValueBytes >>> 8) & 0xFF);
        outputStream.write(totalValueBytes & 0xFF);
        // write field info
        LessBytes.writeInt(columnList.size(), outputStream);
        for (Column column : columnList) {
            LessBytes.writeString(column.getName(), outputStream);
            LessBytes.writeInt(column.getColumnType().getId(), outputStream);
        }
    }
}
