/**
 * Copyright 2026 The Action Engine Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { ChunkStore, LocalChunkStore } from './chunkStore.js';
import { Chunk, endOfStream } from './data.js';
import {
  ChunkStoreReader,
  NumberedChunk,
  ChunkStoreReaderOptions,
} from './chunkStoreReader.js';
import { ChunkStoreWriter } from './chunkStoreWriter.js';
import { BaseActionEngineStream } from './stream.js';
import { Mutex } from './utils.js';

export class NodeMap {
  private nodes: Map<string, AsyncNode>;
  private readonly chunkStoreFactory: (() => ChunkStore) | null;

  constructor(chunkStoreFactory: (() => ChunkStore) | null = null) {
    this.nodes = new Map();
    this.chunkStoreFactory =
      chunkStoreFactory !== null
        ? chunkStoreFactory
        : () => new LocalChunkStore();
  }

  removeNode(id: string): boolean {
    return this.nodes.delete(id);
  }

  hasNode(id: string): boolean {
    return this.nodes.has(id);
  }

  getNode(id: string): AsyncNode {
    if (!this.nodes.has(id)) {
      this.nodes.set(
        id,
        new AsyncNode(id, this.nodes, this.chunkStoreFactory()),
      );
    }
    return this.nodes.get(id) as AsyncNode;
  }
}

export class AsyncNode {
  private readonly nodeMap: NodeMap | Map<string, AsyncNode> | null;
  private readonly chunkStore: ChunkStore;
  private defaultReader: ChunkStoreReader | null;
  private defaultWriter: ChunkStoreWriter | null;

  private writerStream: BaseActionEngineStream | null;
  private readonly mutex: Mutex;

  constructor(
    id: string = '',
    nodeMap: NodeMap | Map<string, AsyncNode> | null = null,
    chunkStore: ChunkStore | null = null,
  ) {
    this.nodeMap = nodeMap;
    this.chunkStore = chunkStore || new LocalChunkStore();
    this.chunkStore.setId(id);
    this.defaultReader = null;
    this.defaultWriter = null;
    this.writerStream = null;
    this.mutex = new Mutex();
  }

  [Symbol.asyncIterator]() {
    return {
      next: async () => {
        const chunk = await this.next();
        return {
          value: chunk !== null ? (chunk as Chunk) : undefined,
          done: chunk === null,
        };
      },
    };
  }

  getId(): string {
    if (this.nodeMap) {
      return this.chunkStore.getId();
    }
    return this.chunkStore.getId();
  }

  async nextNumberedChunk(): Promise<NumberedChunk | null> {
    await this.ensureReader();
    return await this.defaultReader.next();
  }

  async next(): Promise<Chunk | null> {
    const numberedChunk = await this.nextNumberedChunk();
    if (numberedChunk === null) {
      return null;
    }
    return numberedChunk.chunk;
  }

  async put(
    chunk: Chunk,
    seq: number = -1,
    final: boolean = false,
  ): Promise<void> {
    await this.ensureWriter();
    const writtenSeq = await this.defaultWriter.put(chunk, seq, final);
    if (this.writerStream !== null) {
      this.writerStream
        .send({
          nodeFragments: [
            {
              id: this.chunkStore.getId(),
              data: chunk,
              seq: writtenSeq,
              continued: !final,
            },
          ],
        })
        .then();
    }
  }

  async finalize(): Promise<void> {
    return await this.put(endOfStream(), -1, /*final=*/ true);
  }

  async putAndFinalize(chunk: Chunk, seq: number = -1): Promise<void> {
    return await this.put(chunk, seq, /*final=*/ true);
  }

  async bindWriterStream(stream: BaseActionEngineStream | null = null) {
    await this.mutex.runExclusive(async () => {
      this.writerStream = stream;
    });
  }

  setReaderOptions(options: ChunkStoreReaderOptions): AsyncNode {
    this.ensureReader(options).then();
    return this;
  }

  async getWriter(): Promise<ChunkStoreWriter> {
    await this.ensureWriter();
    return this.defaultWriter as ChunkStoreWriter;
  }

  private async ensureReader(options: ChunkStoreReaderOptions = {}) {
    await this.mutex.runExclusive(async () => {
      if (this.defaultReader !== null) {
        await this.defaultReader.setOptions(options);
        return;
      }
      this.defaultReader = new ChunkStoreReader(this.chunkStore, options);
    });
  }

  private async ensureWriter() {
    await this.mutex.runExclusive(() => {
      if (this.defaultWriter !== null) {
        return;
      }
      this.defaultWriter = new ChunkStoreWriter(this.chunkStore);
    });
  }
}
