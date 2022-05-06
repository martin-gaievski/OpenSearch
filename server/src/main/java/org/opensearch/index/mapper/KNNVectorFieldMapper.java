/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.codecs.lucene91.Lucene91HnswVectorsFormat;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.KnnVectorField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.Query;
import org.opensearch.common.Explicit;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.support.XContentMapValues;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.QueryShardException;
import org.opensearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Field Mapper for KNN vector type.
 *
 * Extends ParametrizedFieldMapper in order to easily configure mapping parameters.
 */
public class KNNVectorFieldMapper extends ParametrizedFieldMapper {

    public static final String CONTENT_TYPE = "knn_vector";


    /**
     * Define the max dimension a knn_vector mapping can have. This limit is somewhat arbitrary. In the future, we
     * should make this configurable.
     */
    public static final int MAX_DIMENSION = 10000;

    public static final int MAX_CONN_DEFAULT_VALUE = Lucene91HnswVectorsFormat.DEFAULT_MAX_CONN;
    public static final int MAX_CONN_MAX_VALUE = 16;
    public static final int BEAM_WIDTH_DEFAULT_VALUE = Lucene91HnswVectorsFormat.DEFAULT_BEAM_WIDTH;
    public static final int BEAM_WIDTH_MAX_VALUE = 512;

    private static KNNVectorFieldMapper toType(FieldMapper in) {
        return (KNNVectorFieldMapper) in;
    }

    /**
     * Builder for KNNVectorFieldMapper. This class defines the set of parameters that can be applied to the knn_vector
     * field type
     */
    public static class Builder extends ParametrizedFieldMapper.Builder {

        protected final Parameter<Integer> dimension = new Parameter<>("dimension", false,
            () -> -1,
            (n, c, o) -> {
                if (o == null) {
                    throw new IllegalArgumentException("Dimension cannot be null");
                }
                int value = XContentMapValues.nodeIntegerValue(o);
                if (value > MAX_DIMENSION) {
                    throw new IllegalArgumentException("Dimension value cannot be greater than " +
                        MAX_DIMENSION + " for vector: " + name);
                }

                if (value <= 0) {
                    throw new IllegalArgumentException("Dimension value must be greater than 0 " +
                        "for vector: " + name);
                }
                return value;
            }, m -> toType(m).dimension);

        protected final Parameter<Integer> maxConnections = new Parameter<>("max_connections", false,
            () -> MAX_CONN_DEFAULT_VALUE,
            (n, c, o) -> {
                if (o == null) {
                    throw new IllegalArgumentException("Max connections cannot be null");
                }
                int value = XContentMapValues.nodeIntegerValue(o);
                if (value > MAX_CONN_MAX_VALUE) {
                    throw new IllegalArgumentException("Max connections value cannot be greater than " +
                        MAX_CONN_MAX_VALUE + " for vector: " + name);
                }

                if (value <= 0) {
                    throw new IllegalArgumentException("Max connections value must be greater than 0 " +
                        "for vector: " + name);
                }
                return value;
            }, maxConnections -> toType(maxConnections).maxConnections);

        protected final Parameter<Integer> beamWidth = new Parameter<>("beam_width", false,
            () -> BEAM_WIDTH_DEFAULT_VALUE,
            (n, c, o) -> {
                if (o == null) {
                    throw new IllegalArgumentException("Beam width cannot be null");
                }
                int value = XContentMapValues.nodeIntegerValue(o);
                if (value > BEAM_WIDTH_MAX_VALUE) {
                    throw new IllegalArgumentException("Beam width connections value cannot be greater than " +
                        BEAM_WIDTH_MAX_VALUE + " for vector: " + name);
                }

                if (value <= 0) {
                    throw new IllegalArgumentException("Beam width value must be greater than 0 for vector: " + name);
                }
                return value;
            }, beamWidth -> toType(beamWidth).beamWidth);

        public Builder(String name) {
            super(name);
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return List.of(dimension, maxConnections, beamWidth);
        }

        @Override
        public KNNVectorFieldMapper build(BuilderContext context) {
            return new KNNVectorFieldMapper(
                buildFullName(context),
                new KNNVectorFieldType(buildFullName(context), Collections.emptyMap(), dimension.get(),
                    maxConnections.get(), beamWidth.get()),
                multiFieldsBuilder.build(this, context),
                copyTo.build()
            );
        }
    }

    public static class TypeParser implements Mapper.TypeParser {

        @Override
        public Mapper.Builder<?> parse(String name, Map<String, Object> node, ParserContext parserContext)
            throws MapperParsingException {
            Builder builder = new KNNVectorFieldMapper.Builder(name);
            builder.parse(name, parserContext, node);
            return builder;
        }
    }

    public static class KNNVectorFieldType extends MappedFieldType {

        int dimension;
        int maxConnections;
        int beamWidth;

        public KNNVectorFieldType(String name, Map<String, String> meta, int dimension, int maxConnections, int beamWidth) {
            super(name, false, false, true, TextSearchInfo.NONE, meta);
            this.dimension = dimension;
            this.maxConnections = maxConnections;
            this.beamWidth = beamWidth;
        }

        @Override
        public ValueFetcher valueFetcher(QueryShardContext context, SearchLookup searchLookup, String format) {
            throw new UnsupportedOperationException("KNN Vector do not support fields search");
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            return new DocValuesFieldExistsQuery(name());
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            throw new QueryShardException(context, "KNN vector do not support exact searching, use KNN queries " +
                "instead: [" + name() + "]");
        }

        public int getDimension() {
            return dimension;
        }

        public int getMaxConnections() {
            return maxConnections;
        }

        public int getBeamWidth() {
            return beamWidth;
        }
    }

    protected Explicit<Boolean> ignoreMalformed;
    protected boolean stored;
    protected boolean hasDocValues;
    protected Integer dimension;
    protected Integer maxConnections;
    protected Integer beamWidth;
    protected String modelId;

    public KNNVectorFieldMapper(String simpleName, KNNVectorFieldType mappedFieldType, MultiFields multiFields,
                                CopyTo copyTo) {
        super(simpleName, mappedFieldType,  multiFields, copyTo);
        dimension = mappedFieldType.dimension;
        maxConnections = mappedFieldType.maxConnections;
        beamWidth = mappedFieldType.beamWidth;
        fieldType = new FieldType(KNNVectorFieldMapper.Defaults.FIELD_TYPE);
        fieldType.setVectorDimensionsAndSimilarityFunction(mappedFieldType.getDimension(),
            VectorSimilarityFunction.EUCLIDEAN);
        fieldType.freeze();
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
        parseCreateField(context, fieldType().getDimension());
    }

    protected void parseCreateField(ParseContext context, int dimension) throws IOException {
        context.path().add(simpleName());

        ArrayList<Float> vector = new ArrayList<>();
        XContentParser.Token token = context.parser().currentToken();
        float value;
        if (token == XContentParser.Token.START_ARRAY) {
            token = context.parser().nextToken();
            while (token != XContentParser.Token.END_ARRAY) {
                value = context.parser().floatValue();

                if (Float.isNaN(value)) {
                    throw new IllegalArgumentException("KNN vector values cannot be NaN");
                }

                if (Float.isInfinite(value)) {
                    throw new IllegalArgumentException("KNN vector values cannot be infinity");
                }

                vector.add(value);
                token = context.parser().nextToken();
            }
        } else if (token == XContentParser.Token.VALUE_NUMBER) {
            value = context.parser().floatValue();

            if (Float.isNaN(value)) {
                throw new IllegalArgumentException("KNN vector values cannot be NaN");
            }

            if (Float.isInfinite(value)) {
                throw new IllegalArgumentException("KNN vector values cannot be infinity");
            }

            vector.add(value);
            context.parser().nextToken();
        } else if (token == XContentParser.Token.VALUE_NULL) {
            context.path().remove();
            return;
        }

        if (dimension != vector.size()) {
            String errorMessage = String.format("Vector dimension mismatch. Expected: %d, Given: %d", dimension,
                vector.size());
            throw new IllegalArgumentException(errorMessage);
        }

        float[] array = new float[vector.size()];
        int i = 0;
        for (Float f : vector) {
            array[i++] = f;
        }

        KnnVectorField point = new KnnVectorField(name(), array, fieldType);

        context.doc().add(point);
        context.path().remove();
    }

    @Override
    protected boolean docValuesByDefault() {
        return true;
    }

    @Override
    public ParametrizedFieldMapper.Builder getMergeBuilder() {
        return new KNNVectorFieldMapper.Builder(simpleName()).init(this);
    }

    @Override
    public final boolean parsesArrayValue() {
        return true;
    }

    @Override
    public KNNVectorFieldType fieldType() {
        return (KNNVectorFieldType) super.fieldType();
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);
    }

    public static class Defaults {
        public static final FieldType FIELD_TYPE = new FieldType();

        static {
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setIndexOptions(IndexOptions.NONE);
            FIELD_TYPE.setDocValuesType(DocValuesType.NONE);
            FIELD_TYPE.freeze();
        }
    }
}
