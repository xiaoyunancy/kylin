/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.gridtable;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.cube.gridtable.CuboidToGridTableMapping;
import org.apache.kylin.dimension.AbstractDateDimEnc;
import org.apache.kylin.dimension.DictionaryDimEnc;
import org.apache.kylin.dimension.DimensionEncoding;
import org.apache.kylin.dimension.FixedLenDimEnc;
import org.apache.kylin.dimension.IntDimEnc;
import org.apache.kylin.dimension.IntegerDimEnc;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;
import org.apache.kylin.metadata.expression.TupleExpression;
import org.apache.kylin.metadata.expression.TupleExpressionSerializer;
import org.apache.kylin.metadata.filter.ColumnTupleFilter;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.ConstantTupleFilter;
import org.apache.kylin.metadata.filter.FilterOptimizeTransformer;
import org.apache.kylin.metadata.filter.IFilterCodeSystem;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter.FilterOperatorEnum;
import org.apache.kylin.metadata.filter.TupleFilterSerializer;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import static org.apache.kylin.metadata.filter.ConstantTupleFilter.TRUE;

public class GTUtil {

    private GTUtil(){}

    static final TableDesc MOCKUP_TABLE = TableDesc.mockup("GT_MOCKUP_TABLE");

    static TblColRef tblColRef(int col, String datatype) {
        return TblColRef.mockup(MOCKUP_TABLE, col + 1, "" + col, datatype);
    }

    public static byte[] serializeGTFilter(TupleFilter gtFilter, GTInfo info) {
        IFilterCodeSystem<ByteArray> filterCodeSystem = wrap(info.codeSystem.getComparator());
        return TupleFilterSerializer.serialize(gtFilter, filterCodeSystem);
    }

    public static TupleFilter deserializeGTFilter(byte[] bytes, GTInfo info) {
        IFilterCodeSystem<ByteArray> filterCodeSystem = wrap(info.codeSystem.getComparator());
        return TupleFilterSerializer.deserialize(bytes, filterCodeSystem);
    }

    public static TupleFilter convertFilterUnevaluatable(TupleFilter rootFilter, GTInfo info, //
            Set<TblColRef> unevaluatableColumnCollector) {
        return convertFilter(rootFilter, info, null, false, unevaluatableColumnCollector);
    }

    public static TupleFilter convertFilterColumnsAndConstants(TupleFilter rootFilter, GTInfo info, //
            List<TblColRef> colMapping, Set<TblColRef> unevaluatableColumnCollector) {
        Map<TblColRef, Integer> map = colListToMap(colMapping);
        return convertFilterColumnsAndConstants(rootFilter, info, map, unevaluatableColumnCollector);
    }

    public static TupleFilter convertFilterColumnsAndConstants(TupleFilter rootFilter, GTInfo info, //
                                                               Map<TblColRef, Integer> colMapping, Set<TblColRef> unevaluatableColumnCollector) {
        return convertFilterColumnsAndConstants(rootFilter, info, colMapping, unevaluatableColumnCollector, false);
    }

    public static TupleFilter convertFilterColumnsAndConstants(TupleFilter rootFilter, GTInfo info, //
            Map<TblColRef, Integer> colMapping, Set<TblColRef> unevaluatableColumnCollector, boolean isParquet) {
        if (rootFilter == null) {
            return null;
        }

        TupleFilter filter;
        if (isParquet) {
            filter = convertFilter(rootFilter, new GTConvertDecoratorParquet(unevaluatableColumnCollector, colMapping, info, true));
        } else {
            filter = convertFilter(rootFilter, info, colMapping, true, unevaluatableColumnCollector);
        }

        // optimize the filter: after translating with dictionary, some filters become determined
        // e.g.
        // ( a = 'value_in_dict' OR a = 'value_not_in_dict') will become (a = 'value_in_dict' OR ConstantTupleFilter.FALSE)
        // use the following to further trim the filter to (a = 'value_in_dict')
        // The goal is to avoid too many children after flatten filter step
        filter = new FilterOptimizeTransformer().transform(filter);
        return filter;
    }

    protected static Map<TblColRef, Integer> colListToMap(List<TblColRef> colMapping) {
        Map<TblColRef, Integer> map = Maps.newHashMap();
        for (int i = 0; i < colMapping.size(); i++) {
            map.put(colMapping.get(i), i);
        }
        return map;
    }

    // converts TblColRef to GridTable column, encode constants, drop unEvaluatable parts
    private static TupleFilter convertFilter(TupleFilter rootFilter, final GTInfo info, //
            final Map<TblColRef, Integer> colMapping, final boolean encodeConstants, //
            final Set<TblColRef> unevaluatableColumnCollector) {

        IFilterCodeSystem<ByteArray> filterCodeSystem = wrap(info.codeSystem.getComparator());

        GTConvertDecorator decorator = new GTConvertDecorator(unevaluatableColumnCollector, colMapping, info,
                encodeConstants);

        byte[] bytes = TupleFilterSerializer.serialize(rootFilter, decorator, filterCodeSystem);
        return TupleFilterSerializer.deserialize(bytes, filterCodeSystem);
    }

    private static TupleFilter convertFilter(TupleFilter rootFilter, GTConvertDecorator decorator) {
        rootFilter = decorator.onSerialize(rootFilter);
        List<TupleFilter> newChildren = Lists.newArrayListWithCapacity(rootFilter.getChildren().size());
        if (rootFilter.hasChildren()) {
            for (TupleFilter childFilter : rootFilter.getChildren()) {
                newChildren.add(convertFilter(childFilter, decorator));

            }
            rootFilter.removeAllChildren();
            rootFilter.addChildren(newChildren);
        }
        return rootFilter;
    }

    public static TupleExpression convertFilterColumnsAndConstants(TupleExpression rootExpression, GTInfo info,
            CuboidToGridTableMapping mapping, Set<TblColRef> unevaluatableColumnCollector) {
        Map<TblColRef, FunctionDesc> innerFuncMap = Maps.newHashMap();
        return convertFilterColumnsAndConstants(rootExpression, info, mapping, innerFuncMap,
                unevaluatableColumnCollector);
    }

    public static TupleExpression convertFilterColumnsAndConstants(TupleExpression rootExpression, GTInfo info,
            CuboidToGridTableMapping mapping, //
            Map<TblColRef, FunctionDesc> innerFuncMap, Set<TblColRef> unevaluatableColumnCollector) {
        return convertExpression(rootExpression, info, mapping, innerFuncMap, true, unevaluatableColumnCollector);
    }

    private static TupleExpression convertExpression(TupleExpression rootExpression, final GTInfo info,
            final CuboidToGridTableMapping mapping, //
            final Map<TblColRef, FunctionDesc> innerFuncMap, final boolean encodeConstants,
            final Set<TblColRef> unevaluatableColumnCollector) {
        IFilterCodeSystem<ByteArray> filterCodeSystem = wrap(info.codeSystem.getComparator());

        final TupleExpressionSerializer.Decorator decorator = new TupleExpressionSerializer.Decorator() {
            @Override
            public TupleFilter convertInnerFilter(TupleFilter filter) {
                return convertFilter(filter, info, mapping.getDim2gt(), encodeConstants, unevaluatableColumnCollector);
            }

            @Override
            public TupleExpression convertInnerExpression(TupleExpression expression) {
                return convertFilterColumnsAndConstants(expression, info, mapping, innerFuncMap,
                        unevaluatableColumnCollector);
            }

            @Override
            public TblColRef mapCol(TblColRef col) {
                int gtColIdx = mapping.getIndexOf(col);
                if (gtColIdx < 0 && innerFuncMap.get(col) != null) {
                    gtColIdx = mapping.getIndexOf(innerFuncMap.get(col));
                }
                return info.colRef(gtColIdx);
            }
        };

        byte[] bytes = TupleExpressionSerializer.serialize(rootExpression, decorator, filterCodeSystem);
        return TupleExpressionSerializer.deserialize(bytes, filterCodeSystem);
    }

    public static IFilterCodeSystem<ByteArray> wrap(final IGTComparator comp) {
        return new IFilterCodeSystem<ByteArray>() {

            @Override
            public int compare(ByteArray o1, ByteArray o2) {
                return comp.compare(o1, o2);
            }

            @Override
            public boolean isNull(ByteArray code) {
                return comp.isNull(code);
            }

            @Override
            public void serialize(ByteArray code, ByteBuffer buffer) {
                if (code == null)
                    BytesUtil.writeByteArray(null, 0, 0, buffer);
                else
                    BytesUtil.writeByteArray(code.array(), code.offset(), code.length(), buffer);
            }

            @Override
            public ByteArray deserialize(ByteBuffer buffer) {
                return new ByteArray(BytesUtil.readByteArray(buffer));
            }
        };
    }

    private static class GTConvertDecoratorParquet extends GTConvertDecorator {
        public GTConvertDecoratorParquet(Set<TblColRef> unevaluatableColumnCollector, Map<TblColRef, Integer> colMapping,
                                         GTInfo info, boolean encodeConstants) {
            super(unevaluatableColumnCollector, colMapping, info, encodeConstants);
        }

        @Override
        protected TupleFilter convertColumnFilter(ColumnTupleFilter columnFilter) {
            return columnFilter;
        }

        @Override
        protected Object translate(int col, Object value, int roundingFlag) {
            try {
                buf.clear();
                DimensionEncoding dimEnc = info.codeSystem.getDimEnc(col);
                info.codeSystem.encodeColumnValue(col, value, roundingFlag, buf);
                DataTypeSerializer serializer = dimEnc.asDataTypeSerializer();
                buf.flip();
                if (dimEnc instanceof DictionaryDimEnc) {
                    int id = BytesUtil.readUnsigned(buf, dimEnc.getLengthOfEncoding());
                    return id;
                } else if (dimEnc instanceof AbstractDateDimEnc) {
                    return Long.valueOf((String)serializer.deserialize(buf));
                } else if (dimEnc instanceof FixedLenDimEnc) {
                    return serializer.deserialize(buf);
                } else if (dimEnc instanceof IntegerDimEnc || dimEnc instanceof IntDimEnc) {
                    return Integer.valueOf((String)serializer.deserialize(buf));
                } else {
                    return value;
                }
            } catch (IllegalArgumentException ex) {
                return null;
            }
        }
    }

    protected static class GTConvertDecorator implements TupleFilterSerializer.Decorator {
        protected final Set<TblColRef> unevaluatableColumnCollector;
        protected final Map<TblColRef, Integer> colMapping;
        protected final GTInfo info;
        protected final boolean useEncodeConstants;

        public GTConvertDecorator(Set<TblColRef> unevaluatableColumnCollector, Map<TblColRef, Integer> colMapping,
                                  GTInfo info, boolean encodeConstants) {
            this.unevaluatableColumnCollector = unevaluatableColumnCollector;
            this.colMapping = colMapping;
            this.info = info;
            this.useEncodeConstants = encodeConstants;
            buf = ByteBuffer.allocate(info.getMaxColumnLength());
        }

        protected int mapCol(TblColRef col) {
            Integer i = colMapping.get(col);
            return i == null ? -1 : i;
        }

        @Override
        public TupleFilter onSerialize(TupleFilter filter) {
            if (filter == null)
                return null;

            // In case of NOT(unEvaluatableFilter), we should immediately replace it as TRUE,
            // Otherwise, unEvaluatableFilter will later be replace with TRUE and NOT(unEvaluatableFilter)
            // will always return FALSE.
            if (filter.getOperator() == FilterOperatorEnum.NOT && !TupleFilter.isEvaluableRecursively(filter)) {
                TupleFilter.collectColumns(filter, unevaluatableColumnCollector);
                return TRUE;
            }

            // shortcut for unEvaluatable filter
            if (!filter.isEvaluable()) {
                TupleFilter.collectColumns(filter, unevaluatableColumnCollector);
                return TRUE;
            }

            // map to column onto grid table
            if (colMapping != null && filter instanceof ColumnTupleFilter) {
                return convertColumnFilter((ColumnTupleFilter) filter);
            }

            // encode constants
            if (useEncodeConstants && filter instanceof CompareTupleFilter) {
                return encodeConstants((CompareTupleFilter) filter);
            }

            return filter;
        }

        protected TupleFilter convertColumnFilter(ColumnTupleFilter columnFilter) {
            int gtColIdx = mapCol(columnFilter.getColumn());
            return new ColumnTupleFilter(info.colRef(gtColIdx));
        }

        protected TupleFilter encodeConstants(CompareTupleFilter oldCompareFilter) {
            // extract ColumnFilter & ConstantFilter
            TblColRef externalCol = oldCompareFilter.getColumn();

            if (externalCol == null) {
                return oldCompareFilter;
            }

            Collection constValues = oldCompareFilter.getValues();
            if (constValues == null || constValues.isEmpty()) {
                return oldCompareFilter;
            }

            //CompareTupleFilter containing BuiltInFunctionTupleFilter will not reach here caz it will be transformed by BuiltInFunctionTransformer
            CompareTupleFilter newCompareFilter = new CompareTupleFilter(oldCompareFilter.getOperator());
            newCompareFilter.addChild(new ColumnTupleFilter(externalCol));

            //for CompareTupleFilter containing dynamicVariables, the below codes will actually replace dynamicVariables
            //with normal ConstantTupleFilter

            Object firstValue = constValues.iterator().next();
            int col = colMapping == null ? externalCol.getColumnDesc().getZeroBasedIndex() : mapCol(externalCol);

            TupleFilter result;
            Object code;

            // translate constant into code
            switch (newCompareFilter.getOperator()) {
            case EQ:
            case IN:
                Set newValues = Sets.newHashSet();
                for (Object value : constValues) {
                    code = translate(col, value, 0);
                    if (code != null)
                        newValues.add(code);
                }
                if (newValues.isEmpty()) {
                    result = ConstantTupleFilter.FALSE;
                } else {
                    newCompareFilter.addChild(new ConstantTupleFilter(newValues));
                    result = newCompareFilter;
                }
                break;
            case NOTIN:
                Set notInValues = Sets.newHashSet();
                for (Object value : constValues) {
                    code = translate(col, value, 0);
                    if (code != null)
                        notInValues.add(code);
                }
                if (notInValues.isEmpty()) {
                    result = ConstantTupleFilter.TRUE;
                } else {
                    newCompareFilter.addChild(new ConstantTupleFilter(notInValues));
                    result = newCompareFilter;
                }
                break;
            case NEQ:
                code = translate(col, firstValue, 0);
                if (code == null) {
                    result = newCompareFilter(TupleFilter.FilterOperatorEnum.ISNOTNULL, externalCol);
                } else {
                    newCompareFilter.addChild(new ConstantTupleFilter(code));
                    result = newCompareFilter;
                }
                break;
            case LT:
                code = translate(col, firstValue, 0);
                if (code == null) {
                    code = translate(col, firstValue, -1);
                    if (code == null)
                        result = newCompareFilter(TupleFilter.FilterOperatorEnum.ISNOTNULL, externalCol);
                    else
                        result = newCompareFilter(FilterOperatorEnum.LTE, externalCol, code);
                } else {
                    newCompareFilter.addChild(new ConstantTupleFilter(code));
                    result = newCompareFilter;
                }
                break;
            case LTE:
                code = translate(col, firstValue, -1);
                if (code == null) {
                    result = ConstantTupleFilter.FALSE;
                } else {
                    newCompareFilter.addChild(new ConstantTupleFilter(code));
                    result = newCompareFilter;
                }
                break;
            case GT:
                code = translate(col, firstValue, 0);
                if (code == null) {
                    code = translate(col, firstValue, 1);
                    if (code == null)
                        result = newCompareFilter(TupleFilter.FilterOperatorEnum.ISNOTNULL, externalCol);
                    else
                        result = newCompareFilter(FilterOperatorEnum.GTE, externalCol, code);
                } else {
                    newCompareFilter.addChild(new ConstantTupleFilter(code));
                    result = newCompareFilter;
                }
                break;
            case GTE:
                code = translate(col, firstValue, 1);
                if (code == null) {
                    result = ConstantTupleFilter.FALSE;
                } else {
                    newCompareFilter.addChild(new ConstantTupleFilter(code));
                    result = newCompareFilter;
                }
                break;
            default:
                throw new IllegalStateException("Cannot handle operator " + newCompareFilter.getOperator());
            }
            return result;
        }

        private TupleFilter newCompareFilter(FilterOperatorEnum op, TblColRef col, Object code) {
            CompareTupleFilter r = new CompareTupleFilter(op);
            r.addChild(new ColumnTupleFilter(col));
            r.addChild(new ConstantTupleFilter(code));
            return r;
        }

        private TupleFilter newCompareFilter(TupleFilter.FilterOperatorEnum op, TblColRef col) {
            CompareTupleFilter r = new CompareTupleFilter(op);
            r.addChild(new ColumnTupleFilter(col));
            return r;
        }

        transient ByteBuffer buf;

        protected Object translate(int col, Object value, int roundingFlag) {
            try {
                buf.clear();
                info.codeSystem.encodeColumnValue(col, value, roundingFlag, buf);
                return ByteArray.copyOf(buf.array(), 0, buf.position());
            } catch (IllegalArgumentException ex) {
                return null;
            }
        }
    }

    /** set record to the codes of specified values, reuse given space to hold the codes */
    public static GTRecord setValuesParquet(GTRecord record, ByteBuffer buf, Map<Integer, Integer> dictCols,
            Map<Integer, Integer> binaryCols, Map<Integer, Integer> otherCols, Object[] values) {

        int pos = buf.position();
        int i, c;
        for (Map.Entry<Integer, Integer> entry : dictCols.entrySet()) {
            i = entry.getKey();
            c = entry.getValue();
            DictionaryDimEnc.DictionarySerializer serializer = (DictionaryDimEnc.DictionarySerializer) record.info.codeSystem
                    .getSerializer(c);
            int len = serializer.peekLength(buf);
            BytesUtil.writeUnsigned((Integer) values[i], len, buf);
            int newPos = buf.position();
            record.cols[c].reset(buf.array(), buf.arrayOffset() + pos, newPos - pos);
            pos = newPos;
        }

        for (Map.Entry<Integer, Integer> entry : binaryCols.entrySet()) {
            i = entry.getKey();
            c = entry.getValue();
            record.cols[c].reset((byte[]) values[i], 0, ((byte[]) values[i]).length);
        }

        for (Map.Entry<Integer, Integer> entry : otherCols.entrySet()) {
            i = entry.getKey();
            c = entry.getValue();
            record.info.codeSystem.encodeColumnValue(c, values[i], buf);
            int newPos = buf.position();
            record.cols[c].reset(buf.array(), buf.arrayOffset() + pos, newPos - pos);
            pos = newPos;
        }

        return record;
    }
}
