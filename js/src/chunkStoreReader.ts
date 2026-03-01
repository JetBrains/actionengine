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

import { Channel, Mutex } from './utils.js';
import { ChunkStore } from './chunkStore.js';
import { Chunk, isEndOfStream } from './data.js';

export interface NumberedChunk {
  seq: number;
  chunk: Chunk;
}

export interface ChunkStoreReaderOptions {
  ordered?: boolean;
  removeChunks?: boolean;
  nChunksToBuffer?: number;
  timeout?: number;
  startSeqOrOffset?: number;
}

export class ChunkStoreReader {
  private chunkStore: ChunkStore;

  private options: ChunkStoreReaderOptions;

  private prefetchLoop: Promise<void> | null;
  private buffer: Channel<NumberedChunk> | null;
  private totalChunksRead: number;

  private mu: Mutex;

  constructor(chunkStore: ChunkStore, options: ChunkStoreReaderOptions = {}) {
    this.chunkStore = chunkStore;
    this.options = { ...options };

    this.buffer = null;
    this.totalChunksRead = 0;

    this.prefetchLoop = null;
    this.mu = new Mutex();
  }

  async setOptions(options: ChunkStoreReaderOptions) {
    await this.mu.runExclusive(() => {
      this.options = { ...this.options, ...options };
    });
  }

  async next(): Promise<NumberedChunk | null> {
    const timeout = this.options.timeout ?? -1;

    await this.mu.runExclusive(async () => {
      await this.ensurePrefetchIsRunningOrHasCompleted();
    });

    if (this.buffer === null) {
      return null;
    }

    let nextNumberedChunk: NumberedChunk | null = null;
    try {
      nextNumberedChunk = await this.buffer.receive(timeout);
    } catch {
      return null;
    }

    if (
      !nextNumberedChunk ||
      !nextNumberedChunk.chunk ||
      isEndOfStream(nextNumberedChunk.chunk)
    ) {
      return null;
    }

    return nextNumberedChunk;
  }

  private async nextInternal(): Promise<NumberedChunk | null> {
    let nextReadOffset: number;
    let timeout: number;
    await this.mu.runExclusive(async () => {
      nextReadOffset = this.totalChunksRead;
      timeout = this.options.timeout ?? -1;
    });

    const chunk = await this.chunkStore.getByArrivalOrder(
      nextReadOffset,
      timeout,
    );
    if (chunk === null) {
      return null;
    }

    const seqId = await this.chunkStore.getSeqForArrivalOffset(nextReadOffset);

    return { seq: seqId, chunk };
  }

  private async ensurePrefetchIsRunningOrHasCompleted() {
    if (this.prefetchLoop !== null || this.buffer !== null) {
      return;
    }

    this.buffer = new Channel<NumberedChunk>(
      (this.options.nChunksToBuffer ?? -1) > 0
        ? (this.options.nChunksToBuffer as number)
        : 100,
    );
    this.prefetchLoop = this.runPrefetchLoop();
  }

  private async runPrefetchLoop() {
    await this.mu.runExclusive(() => {
      this.totalChunksRead = this.options.startSeqOrOffset ?? 0;
    });

    while (true) {
      const finalSeq = await this.chunkStore.getFinalSeq();
      let totalChunksRead: number;
      let ordered: boolean;
      let removeChunks: boolean;
      let timeout: number;

      await this.mu.runExclusive(async () => {
        totalChunksRead = this.totalChunksRead;
        ordered = this.options.ordered ?? false;
        removeChunks = this.options.removeChunks ?? false;
        timeout = this.options.timeout ?? -1;
      });

      if (finalSeq >= 0 && totalChunksRead > finalSeq) {
        break;
      }

      let nextChunk: Chunk | null = null;
      let nextSeq: number = -1;
      if (ordered) {
        nextChunk = await this.chunkStore.get(totalChunksRead, timeout);
        nextSeq = totalChunksRead;
      } else {
        const nextNumberedChunk = await this.nextInternal();

        if (nextNumberedChunk !== null) {
          nextChunk = nextNumberedChunk.chunk;
          nextSeq = nextNumberedChunk.seq;
        }
      }

      if (nextChunk !== null) {
        if (this.buffer) {
          await this.buffer.send({ seq: nextSeq, chunk: nextChunk });
        }
        await this.mu.runExclusive(async () => {
          this.totalChunksRead++;
        });

        if (removeChunks && nextSeq >= 0 && !isEndOfStream(nextChunk)) {
          await this.chunkStore.pop(nextSeq);
        }
        if (isEndOfStream(nextChunk)) {
          break;
        }
        // Yield to the event loop to allow other tasks (like UI or message processing) to run
        await new Promise((resolve) => setTimeout(resolve, 0));
      } else {
        if (timeout >= 0) {
          // In C++, if Next returns an error, prefetcher stops.
          // For now, we just break if we can't get a chunk (e.g. timeout or end).
          break;
        }
      }

      const finalSeqAfter = await this.chunkStore.getFinalSeq();
      let totalChunksReadAfter: number;
      await this.mu.runExclusive(async () => {
        totalChunksReadAfter = this.totalChunksRead;
      });

      if (finalSeqAfter >= 0 && totalChunksReadAfter > finalSeqAfter) {
        break;
      }
    }
    if (this.buffer) {
      await this.buffer.close();
    }
  }
}
