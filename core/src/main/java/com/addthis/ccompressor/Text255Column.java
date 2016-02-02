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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import com.addthis.basis.util.LessBytes;

public class Text255Column extends AbstractColumn<byte[]> {

    private final Map<ByteArrayWrapper, Integer> wordCountMap = new HashMap<>();
    private final List<ByteArrayWrapper> queue = new ArrayList<>();

    public Text255Column(String name) {
        super(name);
    }

    @Override
    public ColumnType getColumnType() {
        return ColumnType.TEXT255;
    }

    @Override
    public void push(byte[] columnValue) throws IOException {
        ByteArrayWrapper value = new ByteArrayWrapper(columnValue);
        Integer count = wordCountMap.get(value);
        if (count == null) {
            wordCountMap.put(value, 1);
        } else {
            wordCountMap.put(value, count + 1);
        }
        queue.add(value);
    }

    @Override
    public byte[] flush() throws IOException {
        SortedSet<Map.Entry<ByteArrayWrapper, Integer>> sortedSet = entriesSortedByValues(wordCountMap);
        int index = 0;
        Map<ByteArrayWrapper, Integer> indexMap = new HashMap<>();
        ByteArrayOutputStream keyStream = new ByteArrayOutputStream();
        for (Map.Entry<ByteArrayWrapper, Integer> entry : sortedSet) {
            if (entry.getValue() > 2 && entry.getKey() != null && entry.getKey().length > 0) {
                indexMap.put(entry.getKey(), index);
                ByteArrayWrapper value = entry.getKey();
                keyStream.write(VarInt.writeUnsignedVarInt(index++));
                keyStream.write(VarInt.writeUnsignedVarInt(value.length));
                keyStream.write(value.getData());
                if (index++ > 254) {
                    break;
                }
            }
        }

        // now write to output stream
        for (ByteArrayWrapper value : queue) {
            if (indexMap.containsKey(value)) {
                Integer indexVal = indexMap.get(value);
                byteArrayOutputStream.write(VarInt.writeUnsignedVarInt(1));
                byteArrayOutputStream.write(LessBytes.toBytes(indexVal)[3]);
            } else {
                writeColumnChunk(value.getData());
            }
        }

        queue.clear();
        byte[] textBytes = keyStream.toByteArray();
        byte[] outBytes = byteArrayOutputStream.toByteArray();
        byte[] payloadLength = VarInt.writeUnsignedVarInt(outBytes.length);
        // bytes = payload + header + key map length + keymap string
        int totalLength = byteArrayOutputStream.size() + textBytes.length + 1 + payloadLength.length;
        byte[] columnBlockBytes = new byte[totalLength + HEADER_SIZE];
        writeColumnHeader(columnBlockBytes, VERSION, getColumnType(), totalLength);
        columnBlockBytes[9] = LessBytes.toBytes(indexMap.size())[3];
        System.arraycopy(textBytes, 0, columnBlockBytes, 9 + 1, textBytes.length);
        System.arraycopy(payloadLength, 0, columnBlockBytes, 9 + 1 + textBytes.length, payloadLength.length);
        System.arraycopy(outBytes, 0, columnBlockBytes, 9 + 1 + textBytes.length + payloadLength.length, outBytes.length);
        byteArrayOutputStream.reset();
        return columnBlockBytes;
    }

    static <K, V extends Comparable<? super V>>
    SortedSet<Map.Entry<K, V>> entriesSortedByValues(Map<K, V> map) {
        SortedSet<Map.Entry<K, V>> sortedEntries = new TreeSet<>(
                new Comparator<Map.Entry<K, V>>() {
                    @Override
                    public int compare(Map.Entry<K, V> e1, Map.Entry<K, V> e2) {
                        return e2.getValue().compareTo(e1.getValue());
                    }
                }
        );
        sortedEntries.addAll(map.entrySet());
        return sortedEntries;
    }

    public static List<String> convert(List<byte[]> columnValues, Map<Integer, ByteArrayWrapper> indexMap) {
        List<String> values = new ArrayList<>();
        for (byte[] columnValue : columnValues) {
            String value;
            if (indexMap != null && indexMap.size() > 0 && columnValue.length > 0) {
                int index = columnValue[0];
                if (indexMap.containsKey(index)) {
                    value = new String(indexMap.get(index).getData());
                } else {
                    value = new String(columnValue);
                }
            } else {
                value = new String(columnValue);
            }
            values.add(value);
        }
        return values;
    }


    public static List<String> readColumnValues(InputStream inputStream, int payloadLength) throws IOException {
        Map<Integer, ByteArrayWrapper> keyMap = new HashMap<>();
        int totalKeys = LessBytes.readBytes(inputStream, 1)[0] & 0xFF;
        for (int i = 0; i < totalKeys; i++) {
            int index = VarInt.readUnsignedVarInt(inputStream);
            int length = VarInt.readUnsignedVarInt(inputStream);
            byte[] value = LessBytes.readBytes(inputStream, length);
            keyMap.put(index, new ByteArrayWrapper(value));
        }

        int dataLength = VarInt.readUnsignedVarInt(inputStream);
        byte[] bytes = LessBytes.readBytes(inputStream, dataLength);
        List<byte[]> columnValues = Text255Column.decodeBytes(new ByteArrayInputStream(bytes));
        return Text255Column.convert(columnValues, keyMap);
    }


}
