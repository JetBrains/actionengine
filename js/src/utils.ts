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

import { Chunk } from './data.js';
import { Status } from './status';
import { encodeStatus } from './msgpack';

class Waiter {
  private readonly promise: Promise<void>;
  private waiters: Set<Waiter>;

  private resolveInternal: (value: void | PromiseLike<void>) => void;
  private cancelInternal: (reason?: Error) => void;
  private timeout: NodeJS.Timeout | null;
  private timedOut: boolean;

  constructor(waiters: Set<Waiter>) {
    this.waiters = waiters;
    this.promise = new Promise((resolve, reject) => {
      this.resolveInternal = resolve;
      this.cancelInternal = reject;
    });
    this.timeout = null;
    this.timedOut = false;
  }

  async wait(
    timeout: number = -1,
    mutex: Mutex | null = null,
  ): Promise<boolean> {
    // precondition: if mutex is not null, it must be locked
    if (mutex !== null && !mutex.isLocked()) {
      throw new Error('Mutex is not locked');
    }

    if (timeout >= 0) {
      this.timeout = setTimeout(async () => {
        this.timedOut = true;
        this.resolveInternal();
      }, timeout);
    }

    this.waiters.add(this);
    try {
      if (mutex !== null) {
        mutex.release();
      }
      await this.promise;
      return this.timedOut;
    } finally {
      this.waiters.delete(this);
      if (mutex !== null) {
        await mutex.acquire();
      }
    }
  }

  cancel(error?: Error) {
    this.cancelInternal(error);
  }

  notify() {
    if (this.timeout) {
      clearTimeout(this.timeout);
    }
    this.resolveInternal();
  }
}

export class Mutex {
  private _locked: boolean;
  private readonly _waiters: Set<Waiter>;

  constructor() {
    this._locked = false;
    this._waiters = new Set<Waiter>();
  }

  async acquire(): Promise<void> {
    if (!this._locked) {
      this._locked = true;
      return;
    }
    while (this._locked) {
      const waiter = new Waiter(this._waiters);
      await waiter.wait(-1, null);
    }
    this._locked = true;
  }

  async runExclusive<T>(callback: () => T | Promise<T>): Promise<T> {
    await this.acquire();
    try {
      return await callback();
    } finally {
      this.release();
    }
  }

  isLocked(): boolean {
    return this._locked;
  }

  async waitForUnlock(): Promise<void> {
    if (!this._locked) {
      return;
    }
    while (this._locked) {
      const waiter = new Waiter(this._waiters);
      await waiter.wait(-1, this);
    }
  }

  release(): void {
    if (!this._locked) {
      throw new Error('Mutex is not locked');
    }
    this._locked = false;
    if (this._waiters.size > 0) {
      const waiter: Waiter = this._waiters.values().next().value;
      if (waiter) {
        waiter.notify();
      }
    }
  }

  cancel(error?: Error): void {
    for (const waiter of this._waiters) {
      waiter.cancel(error);
    }
    this._waiters.clear();
    if (error) {
      throw error;
    }
  }
}

export class CondVar {
  private readonly waiters: Set<Waiter>;

  constructor() {
    this.waiters = new Set() as Set<Waiter>;
  }

  async wait(mutex: Mutex) {
    const waiter = new Waiter(this.waiters);
    await waiter.wait(-1, mutex);
  }

  async waitWithTimeout(mutex: Mutex, timeout: number) {
    const waiter = new Waiter(this.waiters);
    return await waiter.wait(timeout, mutex);
  }

  async waitWithDeadline(mutex: Mutex, deadline: DOMHighResTimeStamp) {
    const timeout = deadline - performance.now();
    const waiter = new Waiter(this.waiters);
    return await waiter.wait(timeout, mutex);
  }

  notifyOne() {
    const waiter: Waiter = this.waiters.values().next().value;
    if (waiter) {
      waiter.notify();
    }
  }

  notifyAll() {
    for (const waiter of this.waiters) {
      waiter.notify();
    }
  }
}

export class Deque<ValueType> {
  data: Array<ValueType | undefined>;
  head: number;
  tail: number;
  capacity: number;
  size: number;

  constructor(capacity: number = 65536) {
    this.data = new Array(capacity);
    this.head = 0;
    this.tail = 0;
    this.capacity = capacity;
    this.size = 0;
  }

  addFront(value: ValueType) {
    if (this.size >= this.capacity) {
      throw new Error('Deque capacity overflow');
    }
    this.head = (this.head - 1 + this.capacity) % this.capacity;
    this.data[this.head] = value;
    this.size++;
  }

  removeFront(): ValueType | null {
    if (this.size === 0) {
      throw new Error('Deque is empty');
    }
    const value = this.data[this.head];
    this.data[this.head] = undefined;
    this.head = (this.head + 1) % this.capacity;
    this.size--;
    return value ?? null;
  }

  peekFront(): ValueType | null {
    if (this.size > 0) {
      return this.data[this.head] ?? null;
    } else {
      return null;
    }
  }

  addBack(value: ValueType) {
    if (this.size >= this.capacity) {
      throw new Error('Deque capacity overflow');
    }
    this.data[this.tail] = value;
    this.tail = (this.tail + 1) % this.capacity;
    this.size++;
  }

  removeBack(): ValueType | null {
    if (this.size === 0) {
      throw new Error('Deque is empty');
    }
    this.tail = (this.tail - 1 + this.capacity) % this.capacity;
    const value = this.data[this.tail];
    this.data[this.tail] = undefined;
    this.size--;
    return value ?? null;
  }

  peekBack(): ValueType | null {
    if (this.size > 0) {
      return this.data[(this.tail - 1 + this.capacity) % this.capacity] ?? null;
    } else {
      return null;
    }
  }
}

export class Channel<ValueType> {
  private deque: Deque<ValueType>;
  private readonly nBufferedMessages: number;
  private readonly mutex: Mutex;
  private cv: CondVar;
  private closed: boolean;

  constructor(nBufferedMessages: number = 100) {
    this.deque = new Deque<ValueType>(Math.max(nBufferedMessages, 100));
    this.nBufferedMessages = nBufferedMessages;
    this.mutex = new Mutex();
    this.cv = new CondVar();
    this.closed = false;
  }

  async send(value: ValueType) {
    return await this.mutex.runExclusive(async () => {
      if (this.closed) {
        throw new Error('Channel closed');
      }
      while (!this.closed && this.deque.size >= this.nBufferedMessages) {
        await this.cv.wait(this.mutex);
      }
      if (this.closed) {
        throw new Error('Channel closed');
      }
      this.deque.addBack(value);
      this.cv.notifyAll();
    });
  }

  async sendNowait(value: ValueType) {
    return await this.mutex.runExclusive(async () => {
      if (this.closed) {
        throw new Error('Channel closed');
      }
      this.deque.addBack(value);
      this.cv.notifyAll();
    });
  }

  async receive(timeout: number = -1): Promise<ValueType> {
    return await this.mutex.runExclusive(async () => {
      while (!this.closed && this.deque.size === 0) {
        if (await this.cv.waitWithTimeout(this.mutex, timeout)) {
          throw new Error('Timeout waiting for value from channel');
        }
      }

      if (this.closed && this.deque.size === 0) {
        throw new Error('Channel closed');
      }

      return this.deque.removeFront() as ValueType;
    });
  }

  async close() {
    await this.mutex.runExclusive(async () => {
      this.closed = true;
      this.cv.notifyAll();
    });
  }

  async isClosed() {
    return await this.mutex.runExclusive(async () => {
      return this.closed;
    });
  }
}

export const makeBlobFromChunk = (chunk: Chunk): Blob => {
  return new Blob([chunk.data as Uint8Array<ArrayBuffer>], {
    type: chunk.metadata.mimetype,
  });
};

export const makeChunkFromBlob = async (blob: Blob): Promise<Chunk> => {
  const bytes = new Uint8Array(await blob.arrayBuffer());
  return { metadata: { mimetype: blob.type }, data: bytes };
};

export const makeChunkFromStatus = (status: Status) => {
  const data = encodeStatus(status);
  return {
    metadata: {
      mimetype: '__status__',
    },
    data: data,
  } as Chunk;
};
