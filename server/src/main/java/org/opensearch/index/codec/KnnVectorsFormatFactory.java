/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec;

import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene90.Lucene90HnswVectorsFormat;
import org.opensearch.index.mapper.KNNVectorFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;

public class KnnVectorsFormatFactory {

    private final MapperService mapperService;

    public KnnVectorsFormatFactory(MapperService mapperService) {
        this.mapperService = mapperService;
    }

    public KnnVectorsFormat create(final String field) {
        final MappedFieldType mappedFieldType = mapperService.fieldType(field);
        if (mappedFieldType instanceof KNNVectorFieldMapper.KNNVectorFieldType) {
            final KNNVectorFieldMapper.KNNVectorFieldType knnVectorFieldType = (KNNVectorFieldMapper.KNNVectorFieldType) mappedFieldType;
            final KnnVectorsFormat lucene90HnswVectorsFormat =
                new Lucene90HnswVectorsFormat(knnVectorFieldType.getMaxConnections(), knnVectorFieldType.getBeamWidth());
            return lucene90HnswVectorsFormat;
        }
        return new Lucene90HnswVectorsFormat();
    }
}
