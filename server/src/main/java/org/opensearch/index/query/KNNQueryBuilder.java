/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.query;

import org.apache.lucene.search.KnnVectorQuery;
import org.apache.lucene.search.Query;
import org.opensearch.common.ParseField;
import org.opensearch.common.ParsingException;
import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.index.mapper.NumberFieldMapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class KNNQueryBuilder extends AbstractQueryBuilder<KNNQueryBuilder> {

    public static final String NAME = "knn";

    public static final ParseField VECTOR_FIELD = new ParseField("vector");
    public static final ParseField K_FIELD = new ParseField("k");

    private final String fieldName;
    private final float[] vector;
    private final int k;

    public KNNQueryBuilder(String fieldName, float[] vector, int k) {
        if (Strings.isNullOrEmpty(fieldName)) {
            throw new IllegalArgumentException("[" + NAME + "] requires fieldName");
        }
        if (vector == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires query vector");
        }
        if (vector.length == 0) {
            throw new IllegalArgumentException("[" + NAME + "] query vector is empty");
        }
        if (k <= 0) {
            throw new IllegalArgumentException("[" + NAME + "] requires k > 0");
        }

        this.fieldName = fieldName;
        this.vector = vector;
        this.k = k;
    }

    /**
     * @param in Reads from stream
     * @throws IOException Throws IO Exception
     */
    public KNNQueryBuilder(StreamInput in) throws IOException {
        super(in);
        try {
            fieldName = in.readString();
            vector = in.readFloatArray();
            k = in.readInt();
        } catch (IOException ex) {
            throw new RuntimeException("[KNN] Unable to create KNNQueryBuilder: " + ex);
        }
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeFloatArray(vector);
        out.writeInt(k);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.startObject(fieldName);

        builder.field(VECTOR_FIELD.getPreferredName(), vector);
        builder.field(K_FIELD.getPreferredName(), k);
        printBoostAndQueryName(builder);
        builder.endObject();
        builder.endObject();
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        return new KnnVectorQuery(fieldName, vector, k);
    }

    @Override
    protected boolean doEquals(KNNQueryBuilder other) {
        return Objects.equals(fieldName, other.fieldName) &&
            Arrays.equals(vector, other.vector) &&
            Objects.equals(k, other.k);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldName, Arrays.hashCode(vector), k);
    }

    public static KNNQueryBuilder fromXContent(XContentParser parser) throws IOException {
        String fieldName = null;
        List<Object> vector = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        int k = 0;
        String queryName = null;
        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                throwParsingExceptionOnMultipleFields(NAME, parser.getTokenLocation(), fieldName, currentFieldName);
                fieldName = currentFieldName;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (token.isValue() || token == XContentParser.Token.START_ARRAY) {
                        if (VECTOR_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            vector = parser.list();
                        } else if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            boost = parser.floatValue();
                        } else if (K_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            k = (Integer) NumberFieldMapper.NumberType.INTEGER.parse(parser.objectBytes(), false);
                        } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            queryName = parser.text();
                        } else {
                            throw new ParsingException(parser.getTokenLocation(),
                                "[" + NAME + "] query does not support [" + currentFieldName + "]");
                        }
                    } else {
                        throw new ParsingException(parser.getTokenLocation(),
                            "[" + NAME + "] unknown token [" + token + "] after [" + currentFieldName + "]");
                    }
                }
            } else {
                throwParsingExceptionOnMultipleFields(NAME, parser.getTokenLocation(), fieldName, parser.currentName());
                fieldName = parser.currentName();
                vector = parser.list();
            }
        }

        KNNQueryBuilder knnQuery = new KNNQueryBuilder(fieldName, ObjectsToFloats(vector), k);
        knnQuery.queryName(queryName);
        knnQuery.boost(boost);
        return knnQuery;
    }

    private static float[] ObjectsToFloats(List<Object> objs) {
        float[] vec = new float[objs.size()];
        for (int i = 0; i < objs.size(); i++) {
            vec[i] = ((Number) objs.get(i)).floatValue();
        }
        return vec;
    }
}
