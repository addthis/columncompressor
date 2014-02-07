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

public enum ColumnType {
    RAW(0),
    VARLENGTH(1),
    BYTE_DICTIONARY(2),
    RUNLENGTH(3),
    TEXT255(4),
    DELTAINT(5),
    DELTALONG(6);


    private final int id;

    private ColumnType(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public static ColumnType get(int index) {
        switch (index) {
            case 0:
                return RAW;
            case 1:
                return VARLENGTH;
            case 2:
                return BYTE_DICTIONARY;
            case 3:
                return RUNLENGTH;
            case 4:
                return TEXT255;
            case 5:
                return DELTAINT;
            case 6:
                return DELTALONG;
            default:
                return null;
        }
    }
}
