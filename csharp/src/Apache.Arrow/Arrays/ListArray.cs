// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using Apache.Arrow.Flatbuf;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;

namespace Apache.Arrow
{
    public class ListArray : Array
    {


        public class Builder : IArrowArrayBuilder<ListArray>
        {
            //todo: support null bitmap

            public IArrowArrayBuilder<IArrowArray> ValueBuilder { get; }

            private ArrowBuffer.Builder<int> ValueOffsetsBufferBuilder { get; }

            private ListType DataType { get; }

            public Builder(ListType dataType)
            {
                ValueBuilder = ArrowArrayBuilderFactory.BuildBuilder(dataType.ValueDataType) as IArrowArrayBuilder<IArrowArray>;
                ValueOffsetsBufferBuilder = new ArrowBuffer.Builder<int>();
                DataType = dataType;
            }

            public int GetValueCount()
            {
                return ValueOffsetsBufferBuilder.Length;
            }

            public Builder Append()
            {
                ValueOffsetsBufferBuilder.Append(ValueBuilder.GetValueCount());
                return this;
            }

            public ListArray Build(MemoryAllocator allocator = default)
            {
                var valueLength = ValueBuilder.GetValueCount();
                var valueOffsetLength = ValueOffsetsBufferBuilder.Length;

                if (valueOffsetLength == 0 || 
                    ValueOffsetsBufferBuilder.Span[valueOffsetLength - 1] < valueLength)
                {
                    Append();
                }

                var valueList = ValueBuilder.Build(allocator);


                return new ListArray(DataType, ValueOffsetsBufferBuilder.Length - 1,
                    ValueOffsetsBufferBuilder.Build(allocator), valueList,
                    new ArrowBuffer(), 0, 0);

            }

        }
        
        public IArrowArray Values { get; }

        public ArrowBuffer ValueOffsetsBuffer => Data.Buffers[1];

        public ReadOnlySpan<int> ValueOffsets => ValueOffsetsBuffer.Span.CastTo<int>().Slice(Offset, Length + 1);

        public ListArray(IArrowType dataType, int length,
            ArrowBuffer valueOffsetsBuffer, IArrowArray values,
            ArrowBuffer nullBitmapBuffer, int nullCount = 0, int offset = 0)
            : this(new ArrayData(dataType, length, nullCount, offset,
                new[] {nullBitmapBuffer, valueOffsetsBuffer}, new[] {values.Data}))
        {
        }


        public ListArray(ArrayData data)
            : base(data)
        {
            data.EnsureBufferCount(2);
            data.EnsureDataType(ArrowTypeId.List);
            Values = ArrowArrayFactory.BuildArray(data.Children[0]);
        }

        public override void Accept(IArrowArrayVisitor visitor) => Accept(this, visitor);

        public int GetValueOffset(int index)
        {
            return ValueOffsets[index];
        }

        public int GetValueLength(int index)
        {
            var offsets = ValueOffsets;
            return offsets[index + 1] - offsets[index];
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                Values?.Dispose();
            }
            base.Dispose(disposing);
        }
    }
}
