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
        public void ListArrayBuilderTest()
        {
            var int64Field = new Field("val", Int64Type.Default,true); 
            var listBuilder = new ListArray.Builder(new ListType(int64Field));
            var valueBuilder = listBuilder.ValueBuilder as Int64Array.Builder;
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
            var val = result.Values as Int64Array;
            var resultListVal1 = val.Slice(result.GetValueOffset(0), result.GetValueLength(0));
            var resultListVal2 = val.Slice(result.GetValueOffset(1), result.GetValueLength(1));
            var resultListVal3 = val.Slice(result.GetValueOffset(2), result.GetValueLength(2));
        }

        [Fact]
        public void ListArrayBuilderTest2()
        {
            var int64Field = new Field("val", BinaryType.Default, true);
            var listBuilder = new ListArray.Builder(new ListType(int64Field));
            var valueBuilder = listBuilder.ValueBuilder as BinaryArray.Builder;
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
            var val = result.Values as BinaryArray;
            var resultListVal1 = val?.Slice(result.GetValueOffset(0), result.GetValueLength(0)) as BinaryArray;
            //Assert.True(resultListVal1.GetBytes(0).ToArray() == new [] {(byte)2});
            var resultListVal2 = val?.Slice(result.GetValueOffset(1), result.GetValueLength(1)) as BinaryArray;
            var resultListVal3 = val?.Slice(result.GetValueOffset(2), result.GetValueLength(2)) as BinaryArray;
        }

        [Fact]
        public void ListArrayBuilderTest3()
        {
            var int64Field = new Field("val", StringType.Default, true);
            var listBuilder = new ListArray.Builder(new ListType(int64Field));
            var valueBuilder = listBuilder.ValueBuilder as StringArray.Builder;
            listBuilder.Append();
            valueBuilder.Append("111");
            listBuilder.Append();
            valueBuilder.Append("201");
            valueBuilder.Append("3");
            valueBuilder.Append("4");
            listBuilder.Append();
            valueBuilder.Append("21");
            valueBuilder.Append("22");
            valueBuilder.Append("23");
            valueBuilder.Append("24");

            var result = listBuilder.Build();
            var val = result.Values as StringArray;
            var resultListVal1 = val.Slice(result.GetValueOffset(0), result.GetValueLength(0)) as StringArray;
            var resultListVal2 = val.Slice(result.GetValueOffset(1), result.GetValueLength(1)) as StringArray;
            var resultListVal3 = val.Slice(result.GetValueOffset(2), result.GetValueLength(2)) as StringArray;

        }

        [Fact]
        public async void ListArrayBuilderTest5()
        {

            var schema = new Schema(new[] { new Field("str", StringType.Default, true),  }, null);
            var result = new StringArray.Builder().Append("10").Append("20").Append("30").Build();
            var recordBatch = new RecordBatch(schema, new[] { result }, 3);
            using (var stream = new FileStream(@"C:\python\example2.arrow", FileMode.Create))
            {
                var asw = new ArrowFileWriter(stream, recordBatch.Schema);
                await asw.WriteRecordBatchAsync(recordBatch);
                await asw.WriteEndAsync();
                /*
                stream.Position = 0;
                var asr = new ArrowStreamReader(stream);
                var readRecordBatch = asr.ReadNextRecordBatch();
                var col = (ListArray) readRecordBatch.Column("list");
                var vals = (StringArray) col.Values;
                var val1 = (StringArray)vals.Slice(col.GetValueOffset(0), col.GetValueLength(0));
                var val2 = (StringArray)vals.Slice(col.GetValueOffset(1), col.GetValueLength(1));
                var val3 = (StringArray)vals.Slice(col.GetValueOffset(2), col.GetValueLength(2));
                */

            }

        }


        [Fact]
        public async void ListArrayBuilderTest4()
        {
            var int64Field = new Field("item", StringType.Default, true);
            var listBuilder = new ListArray.Builder(new ListType(int64Field));
            var valueBuilder = listBuilder.ValueBuilder as StringArray.Builder;
            listBuilder.Append();
            valueBuilder.Append("1");
            listBuilder.Append();
            valueBuilder.Append("2");
            valueBuilder.Append("3");
            valueBuilder.Append("4");
            listBuilder.Append();
            valueBuilder.Append("21");
            valueBuilder.Append("22");
            valueBuilder.Append("23");
            valueBuilder.Append("24");

            var result = listBuilder.Build();
            var listField = new Field("list", new ListType(int64Field), true);
            var schema = new Schema(new[] {listField},null);
            var recordBatch = new RecordBatch(schema, new[] {result}, 3);
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
                var col = (ListArray) readRecordBatch.Column(0);
                var vals = (StringArray)col.Values;
                var val1 = (StringArray)vals.Slice(col.GetValueOffset(0), col.GetValueLength(0));
                var val2 = (StringArray)vals.Slice(col.GetValueOffset(1), col.GetValueLength(1));
                var val3 = (StringArray)vals.Slice(col.GetValueOffset(2), col.GetValueLength(2));


            }

        }

        [Fact]
        public async void ListArrayBuilderTest7()
        {
            using (var stream = new FileStream(@"C:\python\test.arrow", FileMode.Open))
            {
                var asr = new ArrowFileReader(stream);
                var readRecordBatch = asr.ReadNextRecordBatch();
                var col = (ListArray) readRecordBatch.Column(0);
                var vals = new StringArray(col.Data.Children[0]);
                var val1 = vals.Slice(col.GetValueOffset(0), col.GetValueLength(0));
                var val2 = vals.Slice(col.GetValueOffset(1), col.GetValueLength(1));
                var val3 = vals.Slice(col.GetValueOffset(2), col.GetValueLength(2));

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
            where TArray: IArrowArray
            where TArrayBuilder: IArrowArrayBuilder<TArray>, new()
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
