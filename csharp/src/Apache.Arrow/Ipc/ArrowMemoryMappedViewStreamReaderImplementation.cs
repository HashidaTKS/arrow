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

using Apache.Arrow.Memory;
using System;
using System.Buffers;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Threading;
using System.Threading.Tasks;
using System.Runtime.InteropServices;
using FlatBuffers;

namespace Apache.Arrow.Ipc
{
    internal class ArrowMemoryMappedViewStreamReaderImplementation : ArrowReaderImplementation
    {
        public Stream BaseStream { get; }

        public ArrowMemoryMappedViewStreamReaderImplementation(Stream stream)
        {
            BaseStream = stream;
        }

        protected override void Dispose(bool disposing)
        {
        }

        public override async ValueTask<RecordBatch> ReadNextRecordBatchAsync(CancellationToken cancellationToken)
        {
            // TODO: Loop until a record batch is read.
            cancellationToken.ThrowIfCancellationRequested();
            return await ReadRecordBatchAsync(cancellationToken).ConfigureAwait(false);
        }

        public override RecordBatch ReadNextRecordBatch()
        {
            return ReadRecordBatch();
        }

        protected async ValueTask<RecordBatch> ReadRecordBatchAsync(CancellationToken cancellationToken = default)
        {
            await ReadSchemaAsync().ConfigureAwait(false);

            int messageLength = 0;

            {
                byte[] rentBytes = ArrayPool<byte>.Shared.Rent(4);
                try
                {
                    int bytesRead = await BaseStream.ReadAsync(rentBytes, 0, 4, cancellationToken).ConfigureAwait(false); 
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(rentBytes);
                }
            }

            if (messageLength == 0)
            {
                // reached end
                return null;
            }

            RecordBatch result = null;
            {
                byte[] rentBytes = ArrayPool<byte>.Shared.Rent(messageLength);
                try
                {
                    int bytesRead = await BaseStream.ReadAsync(rentBytes, 0, messageLength, cancellationToken).ConfigureAwait(false);
                    if (bytesRead != messageLength)
                    {
                        throw new InvalidOperationException("Unexpectedly reached the end of the stream before a full buffer was read.");
                    }
                    Flatbuf.Message message = Flatbuf.Message.GetRootAsMessage(new ByteBuffer(rentBytes, 0));
                    int bodyLength = checked((int)message.BodyLength);
                    bytesRead = await BaseStream.ReadAsync(rentBytes, messageLength, bodyLength, cancellationToken).ConfigureAwait(false);
                    if (bytesRead != bodyLength)
                    {
                        throw new InvalidOperationException("Unexpectedly reached the end of the stream before a full buffer was read.");
                    }
                    ByteBuffer bodybb = new ByteBuffer(rentBytes, 0);
                    result = CreateArrowObjectFromMessage(message, bodybb, null);

                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(rentBytes);
                }
            }

            return result;
        }

        protected RecordBatch ReadRecordBatch()
        {
            ReadSchema();

            int messageLength = 0;

            {
                byte[] rentBytes = ArrayPool<byte>.Shared.Rent(4);
                try
                {
                    int bytesRead = BaseStream.Read(rentBytes, 0, 4);
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(rentBytes);
                }
            }

            if (messageLength == 0)
            {
                // reached end
                return null;
            }

            RecordBatch result = null;
            {
                byte[] rentBytes = ArrayPool<byte>.Shared.Rent(messageLength);
                try
                {
                    int bytesRead = BaseStream.Read(rentBytes, 0, messageLength);
                    if (bytesRead != messageLength)
                    {
                        throw new InvalidOperationException("Unexpectedly reached the end of the stream before a full buffer was read.");
                    }
                    Flatbuf.Message message = Flatbuf.Message.GetRootAsMessage(new ByteBuffer(rentBytes, 0));
                    int bodyLength = checked((int)message.BodyLength);
                    bytesRead = BaseStream.Read(rentBytes, messageLength, bodyLength);
                    if (bytesRead != bodyLength)
                    {
                        throw new InvalidOperationException("Unexpectedly reached the end of the stream before a full buffer was read.");
                    }
                    ByteBuffer bodybb = new ByteBuffer(rentBytes, 0);
                    result = CreateArrowObjectFromMessage(message, bodybb, null);

                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(rentBytes);
                }
            }

            return result;
        }

        protected virtual async ValueTask ReadSchemaAsync()
        {
            if (HasReadSchema)
            {
                return;
            }

            // Figure out length of schema
            int schemaMessageLength = 0;
            await ArrayPool<byte>.Shared.RentReturnAsync(4, async (lengthBuffer) =>
            {
                int bytesRead = await BaseStream.ReadFullBufferAsync(lengthBuffer).ConfigureAwait(false);
                EnsureFullRead(lengthBuffer, bytesRead);

                schemaMessageLength = BitUtility.ReadInt32(lengthBuffer);
            }).ConfigureAwait(false);

            await ArrayPool<byte>.Shared.RentReturnAsync(schemaMessageLength, async (buff) =>
            {
                // Read in schema
                int bytesRead = await BaseStream.ReadFullBufferAsync(buff).ConfigureAwait(false);
                EnsureFullRead(buff, bytesRead);

                var schemabb = CreateByteBuffer(buff);
                Schema = MessageSerializer.GetSchema(ReadMessage<Flatbuf.Schema>(schemabb));
            }).ConfigureAwait(false);
        }

        protected virtual void ReadSchema()
        {
            if (HasReadSchema)
            {
                return;
            }

            // Figure out length of schema
            int schemaMessageLength = 0;
            ArrayPool<byte>.Shared.RentReturn(4, lengthBuffer =>
            {
                int bytesRead = BaseStream.ReadFullBuffer(lengthBuffer);
                EnsureFullRead(lengthBuffer, bytesRead);

                schemaMessageLength = BitUtility.ReadInt32(lengthBuffer);
            });

            ArrayPool<byte>.Shared.RentReturn(schemaMessageLength, buff =>
            {
                int bytesRead = BaseStream.ReadFullBuffer(buff);
                EnsureFullRead(buff, bytesRead);

                var schemabb = CreateByteBuffer(buff);
                Schema = MessageSerializer.GetSchema(ReadMessage<Flatbuf.Schema>(schemabb));
            });
        }

        /// <summary>
        /// Ensures the number of bytes read matches the buffer length
        /// and throws an exception it if doesn't. This ensures we have read
        /// a full buffer from the stream.
        /// </summary>
        internal static void EnsureFullRead(Memory<byte> buffer, int bytesRead)
        {
            if (bytesRead != buffer.Length)
            {
                throw new InvalidOperationException("Unexpectedly reached the end of the stream before a full buffer was read.");
            }
        }
    }
}
