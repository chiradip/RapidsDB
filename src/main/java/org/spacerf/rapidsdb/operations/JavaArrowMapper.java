package org.spacerf.rapidsdb.operations;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

public class JavaArrowMapper<T> {
    private final Schema schema;
    public JavaArrowMapper(T t, Schema schema) {
        this.schema = schema;
    }

    VectorSchemaRoot some() {
        try (BufferAllocator allocator = new RootAllocator()) {
            try (VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schema, allocator)) {
                return vectorSchemaRoot;
            }
        }
    }
    void marshal(T t) {

    }
}
