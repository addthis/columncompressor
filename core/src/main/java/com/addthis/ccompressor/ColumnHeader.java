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

public class ColumnHeader {

    public final byte VERSION;
    public final int length;
    public final ColumnType columnType;

    public ColumnHeader(byte VERSION, ColumnType columnType, int length) {
        this.VERSION = VERSION;
        this.length = length;
        this.columnType = columnType;
    }
}
