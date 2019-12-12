using Apache.Arrow.Tests.Fixtures;
using Apache.Arrow.Types;
using System;
using System.Text;
using Xunit;

namespace Apache.Arrow.Tests
{
    public class ArrowArrayTest
    {
        [Fact]
        public void ArraySliceTest()
        {
            TestPrimitiveArraySlice<int, Int32Array, Int32Array.Builder>(x => x.Append(10).Append(20).Append(30));
            //TestPrimitiveArraySlice<sbyte, Int8Array, Int8Array.Builder>(x => x.Append(10).Append(20).Append(30));
            //TestPrimitiveArraySlice<short, Int16Array, Int16Array.Builder>(x => x.Append(10).Append(20).Append(30));
            //TestPrimitiveArraySlice<long,Int64Array, Int64Array.Builder>(x => x.Append(10).Append(20).Append(30));
            //TestPrimitiveArraySlice<byte, UInt8Array, UInt8Array.Builder>(x => x.Append(10).Append(20).Append(30));
            //TestPrimitiveArraySlice<ushort,UInt16Array, UInt16Array.Builder>(x => x.Append(10).Append(20).Append(30));
            //TestPrimitiveArraySlice<uint, UInt32Array, UInt32Array.Builder>(x => x.Append(10).Append(20).Append(30));
            //TestPrimitiveArraySlice<ulong , UInt64Array, UInt64Array.Builder>(x => x.Append(10).Append(20).Append(30));
            //TestPrimitiveArraySlice<float, FloatArray, FloatArray.Builder>(x => x.Append(10).Append(20).Append(30));
            //TestPrimitiveArraySlice<double, DoubleArray, DoubleArray.Builder>(x => x.Append(10).Append(20).Append(30));

            TestStringArraySlice(x => x.Append("10").Append("20").Append("30"));
        }

        private static void TestPrimitiveArraySlice<TType, TArray, TArrayBuilder>(Action<TArrayBuilder> action)
            where TType : struct 
            where TArray : IArrowArray
            where TArrayBuilder : IArrowArrayBuilder<TArray>, new()
        {
            var builder = new TArrayBuilder();
            action(builder);
            var baseArray = builder.Build(default) as PrimitiveArray<TType>;
            Assert.NotNull(baseArray);
            var totalLength = baseArray.Length;

            //check all offsets and length
            for (var offset = 0; offset < totalLength; offset++)
            {
                for(var length = 1; length + offset <= totalLength; length++)
                {
                    var targetArray = baseArray.Slice(offset, length) as PrimitiveArray<TType>;
                    Assert.NotNull(targetArray);
                    for (var index = 0; index < length; index++)
                    {
                        Assert.Equal(baseArray.GetValue(index + offset), targetArray.GetValue(index));
                    }
                }
            }
        }

        //TODO : Unite to TestPrimitiveArraySlice
        private static void TestStringArraySlice(Action<StringArray.Builder> action)
        {
            var builder = new StringArray.Builder();
            action(builder);
            var baseArray = builder.Build(default);
            Assert.NotNull(baseArray);
            var totalLength = baseArray.Length;

            //check all offsets and length
            for (var offset = 0; offset < totalLength; offset++)
            {
                for (var length = 1; length + offset <= totalLength; length++)
                {
                    var targetArray = baseArray.Slice(offset, length) as StringArray;
                    Assert.NotNull(targetArray);
                    for (var index = 0; index < length; index++)
                    {
                        Assert.Equal(baseArray.GetString(index + offset), targetArray.GetString(index));
                    }
                }
            }
        }
    }
}
