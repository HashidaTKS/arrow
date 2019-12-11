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

using Apache.Arrow.Types;
using System;
using System.Collections.Generic;
using System.IO;
using Apache.Arrow.Ipc;
using Xunit;

namespace Apache.Arrow.Tests
{
    public class ArrayBuilderTests
    {
        // TODO: Test various builder invariants (Append, AppendRange, Clear, Resize, Reserve, etc)

        [Fact]
        public void PrimitiveArrayBuildersProduceExpectedArray()
        {
            TestArrayBuilder<Int8Array, Int8Array.Builder>(x => x.Append(10).Append(20).Append(30));
            TestArrayBuilder<Int16Array, Int16Array.Builder>(x => x.Append(10).Append(20).Append(30));
            TestArrayBuilder<Int32Array, Int32Array.Builder>(x => x.Append(10).Append(20).Append(30));
            TestArrayBuilder<Int64Array, Int64Array.Builder>(x => x.Append(10).Append(20).Append(30));
            TestArrayBuilder<UInt8Array, UInt8Array.Builder>(x => x.Append(10).Append(20).Append(30));
            TestArrayBuilder<UInt16Array, UInt16Array.Builder>(x => x.Append(10).Append(20).Append(30));
            TestArrayBuilder<UInt32Array, UInt32Array.Builder>(x => x.Append(10).Append(20).Append(30));
            TestArrayBuilder<UInt64Array, UInt64Array.Builder>(x => x.Append(10).Append(20).Append(30));
            TestArrayBuilder<FloatArray, FloatArray.Builder>(x => x.Append(10).Append(20).Append(30));
            TestArrayBuilder<DoubleArray, DoubleArray.Builder>(x => x.Append(10).Append(20).Append(30));
        }

        [Fact]
        public async void TempListArrayBuilderTest()
        {
            var stringField = new Field("item", StringType.Default, true);
            var listBuilder = new ListArray.Builder(new ListType(stringField));
            var valueBuilder = listBuilder.ValueBuilder as StringArray.Builder;
            listBuilder.Append();
            valueBuilder.Append("101");
            listBuilder.Append();
            valueBuilder.Append("2002");
            valueBuilder.Append("20003");
            valueBuilder.Append("200004");
            listBuilder.Append();
            valueBuilder.Append("31");
            valueBuilder.Append("32");
            valueBuilder.Append("33");
            valueBuilder.Append("34");

            var result = listBuilder.Build();
            var listField = new Field("list", new ListType(stringField), true);
            var schema = new Schema(new[] { listField }, null);
            var recordBatch = new RecordBatch(schema, new[] { result }, 3);
            using (var stream = new FileStream(@"C:\python\example3.arrow", FileMode.Create))
            {
                var asw = new ArrowFileWriter(stream, recordBatch.Schema);
                await asw.WriteRecordBatchAsync(recordBatch);
                await asw.WriteEndAsync();
            }

            using (var stream = new FileStream(@"C:\python\example3.arrow", FileMode.Open))
            {
                var asr = new ArrowFileReader(stream);
                var readRecordBatch = asr.ReadNextRecordBatch();
                var col = (ListArray)readRecordBatch.Column(0);
                var vals = (StringArray)col.Values;
                var val1 = (StringArray)vals.Slice(col.GetValueOffset(0), col.GetValueLength(0));
                var val2 = (StringArray)vals.Slice(col.GetValueOffset(1), col.GetValueLength(1));
                var val3 = (StringArray)vals.Slice(col.GetValueOffset(2), col.GetValueLength(2));
            }
        }

        [Fact]
        public async void Int64ListArrayBuilderTest6()
        {
            var int64Field = new Field("item", Int64Type.Default, true);
            var listBuilder = new ListArray.Builder(new ListType(int64Field));
            var valueBuilder = listBuilder.ValueBuilder as Int64Array.Builder;
            Assert.NotNull(valueBuilder);
            listBuilder.Append();
            valueBuilder.Append(1);
            listBuilder.Append();
            valueBuilder.Append(2);
            valueBuilder.Append(3);
            valueBuilder.Append(4);
            listBuilder.Append();
            valueBuilder.Append(21);
            valueBuilder.Append(22);
            valueBuilder.Append(23);
            valueBuilder.Append(24);

            var result = listBuilder.Build();
            var listField = new Field("list", new ListType(int64Field), true);
            var schema = new Schema(new[] { listField }, null);
            var recordBatch = new RecordBatch(schema, new[] { result }, 3);
            using (var stream = new MemoryStream())
            {
                var arrowFileWriter = new ArrowFileWriter(stream, recordBatch.Schema);
                await arrowFileWriter.WriteRecordBatchAsync(recordBatch);
                await arrowFileWriter.WriteEndAsync();

                var arrowFileReader = new ArrowFileReader(stream);
                var readRecordBatch = arrowFileReader.ReadNextRecordBatch();
                var col = (ListArray)readRecordBatch.Column(0);
                var values = (Int64Array)col.Values;
                Assert.Equal(
                    new List<long?> { 1 },
                    ((Int64Array)values.Slice(col.GetValueOffset(0), col.GetValueLength(0))).ToList());
                Assert.Equal(
                    new List<long?> { 2, 3, 4 },
                    ((Int64Array)values.Slice(col.GetValueOffset(1), col.GetValueLength(1))).ToList());
                Assert.Equal(
                    new List<long?> { 21, 22, 23, 24 },
                    ((Int64Array)values.Slice(col.GetValueOffset(2), col.GetValueLength(2))).ToList());

            }
        }


        [Fact]
        public async void Int64ListArrayListBuilderTest6()
        {
            var int64Field = new Field("item", Int64Type.Default, true);
            var childListType = new ListType(int64Field);
            var parentListType = new ListType(childListType);
            var parentListBuilder = new ListArray.Builder(parentListType);
            var childListBuilder = parentListBuilder.ValueBuilder as ListArray.Builder;
            Assert.NotNull(childListBuilder);
            var valueBuilder = childListBuilder.ValueBuilder as Int64Array.Builder;
            Assert.NotNull(valueBuilder);

            parentListBuilder.Append();
            childListBuilder.Append();
            valueBuilder.Append(1);
            childListBuilder.Append();
            valueBuilder.Append(2);
            valueBuilder.Append(3);
            valueBuilder.Append(4);
            parentListBuilder.Append();
            childListBuilder.Append();
            valueBuilder.Append(5);
            valueBuilder.Append(6);
            valueBuilder.Append(7);
            valueBuilder.Append(8);
            parentListBuilder.Append();
            childListBuilder.Append();
            valueBuilder.Append(9);
            valueBuilder.Append(10);
            valueBuilder.Append(11);
            valueBuilder.Append(12);
            valueBuilder.Append(13);
            valueBuilder.Append(14);
            childListBuilder.Build();

            var result = parentListBuilder.Build();
            var parentField = new Field("ListList", new ListType(childListType), true);
            var schema = new Schema(new[] { parentField }, null);
            var recordBatch = new RecordBatch(schema, new[] { result }, 3);
            using (var stream = new MemoryStream())
            {
                var arrowFileWriter = new ArrowFileWriter(stream, recordBatch.Schema);
                await arrowFileWriter.WriteRecordBatchAsync(recordBatch);
                await arrowFileWriter.WriteEndAsync();

                var arrowFileReader = new ArrowFileReader(stream);
                var readRecordBatch = arrowFileReader.ReadNextRecordBatch();
                var parentList = (ListArray)readRecordBatch.Column(0);
                var childLists = (ListArray)parentList.Values;
                var childList1 = (ListArray)childLists.Slice(parentList.GetValueOffset(0), parentList.GetValueLength(0));
                var childList2 = (ListArray)childLists.Slice(parentList.GetValueOffset(1), parentList.GetValueLength(1));
                var childList3 = (ListArray)childLists.Slice(parentList.GetValueOffset(2), parentList.GetValueLength(2));

                Assert.Equal(2, childList1.Length);
                Assert.Equal(1, childList2.Length);
                Assert.Equal(1, childList3.Length);

                var childListValue1 = (Int64Array)childList1.Values;
                var childListValue11 = (Int64Array)childListValue1.Slice(childList1.GetValueOffset(0), childList1.GetValueLength(0));
                Assert.Equal(new List<long?> { 1 }, childListValue11.ToList());
                var childListValue12 = (Int64Array)childListValue1.Slice(childList1.GetValueOffset(1), childList1.GetValueLength(1));
                Assert.Equal(new List<long?> { 2, 3, 4 }, childListValue12.ToList());

                var childListValue2 = (Int64Array)childList2.Values;
                var childListValue21 = (Int64Array)childListValue2.Slice(childList2.GetValueOffset(0), childList2.GetValueLength(0));
                Assert.Equal(new List<long?> { 5, 6, 7, 8 }, childListValue21.ToList());

                var childListValue3 = (Int64Array)childList3.Values;
                var childListValue31 = (Int64Array)childListValue3.Slice(childList3.GetValueOffset(0), childList3.GetValueLength(0));
                Assert.Equal(new List<long?> { 9, 10, 11, 12, 13, 14 }, childListValue31.ToList());
            }
        }



        public class TimestampArrayBuilder
        {
            [Fact]
            public void ProducesExpectedArray()
            {
                var now = DateTimeOffset.UtcNow.ToLocalTime();
                var array = new TimestampArray.Builder(TimeUnit.Nanosecond, TimeZoneInfo.Local.Id)
                    .Append(now)
                    .Build();

                Assert.Equal(1, array.Length);
                Assert.NotNull(array.GetTimestamp(0));
                Assert.Equal(now.Truncate(TimeSpan.FromTicks(100)), array.GetTimestamp(0).Value);
            }
        }

        private static void TestArrayBuilder<TArray, TArrayBuilder>(Action<TArrayBuilder> action)
            where TArray : IArrowArray
            where TArrayBuilder : IArrowArrayBuilder<TArray>, new()
        {
            var builder = new TArrayBuilder();
            action(builder);
            var array = builder.Build(default);

            Assert.IsAssignableFrom<TArray>(array);
            Assert.NotNull(array);
            Assert.Equal(3, array.Length);
            Assert.Equal(0, array.NullCount);
        }

    }
}
