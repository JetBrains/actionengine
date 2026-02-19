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

import { AsyncNode, NodeMap } from './asyncNode.js';
import { BaseActionEngineStream } from './stream.js';
import { Session } from './session.js';
import { ActionMessage, Port } from './data.js';
import { CondVar, makeChunkFromStatus, Mutex } from './utils';
import { internalError, isOk, okStatus, Status } from './status';
import { decodeStatus } from './msgpack';

// eslint-disable-next-line @typescript-eslint/no-unused-vars
const doNothing = async (_: Action) => {};

interface ActionNode {
  name: string;
  type: string;
}

declare type ActionHandler = (action: Action) => Promise<void>;

interface ActionDefinition {
  name: string;
  inputs: ActionNode[];
  outputs: ActionNode[];
}

export class ActionRegistry {
  definitions: Map<string, ActionDefinition>;
  handlers: Map<string, ActionHandler>;

  constructor() {
    this.definitions = new Map();
    this.handlers = new Map();
  }

  register(name: string, def: ActionDefinition, handler?: ActionHandler) {
    this.definitions.set(name, def);
    this.handlers.set(name, handler || doNothing);
  }

  makeActionMessage(name: string, id: string): ActionMessage {
    const def = this.definitions.get(name);

    const inputs: Port[] = [];
    for (const input of def.inputs) {
      inputs.push({
        name: input.name,
        id: `${id}#${input.name}`,
      });
    }

    const outputs: Port[] = [];
    for (const output of def.outputs) {
      outputs.push({
        name: output.name,
        id: `${id}#${output.name}`,
      });
    }

    return {
      id,
      name,
      inputs,
      outputs,
    };
  }

  makeAction(
    name: string,
    id: string,
    nodeMap: NodeMap,
    stream: BaseActionEngineStream,
    session: Session | null,
  ): Action {
    const def = this.definitions.get(name);
    const handler = this.handlers.get(name);

    return new Action(def, id, handler, nodeMap, stream, session);
  }
}

export class Action {
  private readonly definition: ActionDefinition;
  private handler: ActionHandler;
  private readonly id: string;

  private nodeMap: NodeMap | null;
  private stream: BaseActionEngineStream | null;
  private session: Session | null;

  private bindStreamsOnInputsDefault: boolean = true;
  private bindStreamsOnOutputsDefault: boolean = false;

  private hasBeenRun: boolean;
  private hasBeenCalled: boolean = false;
  private runStatus: Status | null = null;
  private readonly nodesWithBoundStreams: Set<AsyncNode>;

  private clearOutputsAfterRun_: boolean;
  private clearInputsAfterRun_: boolean = true;

  private readonly mu: Mutex;
  private readonly cv: CondVar;

  private headers: Map<string, Uint8Array<ArrayBuffer>>;

  constructor(
    definition: ActionDefinition,
    id: string = '',
    handler: ActionHandler | null = null,
    nodeMap: NodeMap | null = null,
    stream: BaseActionEngineStream | null = null,
    session: Session | null = null,
  ) {
    this.definition = definition;
    this.handler = handler;
    this.id = id;
    this.nodeMap = nodeMap;
    this.stream = stream;
    this.session = session;

    this.bindStreamsOnInputsDefault = true;
    this.bindStreamsOnOutputsDefault = false;

    this.hasBeenRun = false;
    this.hasBeenCalled = false;
    this.runStatus = null;
    this.nodesWithBoundStreams = new Set();

    this.clearOutputsAfterRun_ = true;

    this.mu = new Mutex();
    this.cv = new CondVar();

    this.headers = new Map();
  }

  clearOutputsAfterRun(clear: boolean) {
    this.clearOutputsAfterRun_ = clear;
  }

  getDefinition(): ActionDefinition {
    return this.definition;
  }

  getActionMessage(): ActionMessage {
    const def = this.definition;

    const inputs: Port[] = [];
    for (const input of def.inputs) {
      inputs.push({
        name: input.name,
        id: this.getInputId(input.name),
      });
    }

    const outputs: Port[] = [];
    for (const output of def.outputs) {
      outputs.push({
        name: output.name,
        id: this.getOutputId(output.name),
      });
    }

    return {
      id: this.id,
      name: def.name,
      inputs,
      outputs,
      headers: this.headers,
    };
  }

  async run(): Promise<void> {
    return await this.mu.runExclusive(async () => {
      this.bindStreamsOnInputsDefault = false;
      this.bindStreamsOnOutputsDefault = true;

      if (this.hasBeenRun) {
        throw new Error('Action has already been run.');
      }

      if (this.handler === null) {
        throw new Error('Action handler is not bound.');
      }

      this.hasBeenRun = true;
      let handlerStatus = okStatus();

      this.mu.release();
      try {
        await this.handler(this);
      } catch (e: unknown) {
        if (e instanceof Error) {
          handlerStatus = internalError(e.message);
        } else {
          handlerStatus = internalError(
            `Unknown error occurred in action handler: caught ${e}.`,
          );
        }
      } finally {
        await this.mu.acquire();
      }

      const handlerStatusChunk = makeChunkFromStatus(handlerStatus);

      // Propagate status to outputs if not OK
      if (!isOk(handlerStatus)) {
        for (const output of this.definition.outputs) {
          if (
            output.name == '__status__' ||
            output.name == '__dispatch_status__'
          ) {
            continue;
          }
          const outputNode = this.getOutput(output.name);
          await outputNode.put(handlerStatusChunk, -1, true);
        }
      }

      for (const output of this.definition.outputs) {
        if (output.name == '__status__') {
          continue;
        }
        const outputNode = this.getOutput(output.name);
        const writer = await outputNode.getWriter();
        await writer.waitForBufferToDrain();
      }

      await this.unbindStreams();

      if (this.stream !== null) {
        await this.stream.send({
          nodeFragments: [
            {
              id: this.getOutputId('__status__'),
              data: handlerStatusChunk,
              seq: 0,
              continued: false,
            },
          ],
        });
      }

      const statusNode = this.getOutput('__status__', false);
      await statusNode.put(handlerStatusChunk, 0, true);
      const statusWriter = await statusNode.getWriter();
      await statusWriter.waitForBufferToDrain();

      this.runStatus = handlerStatus;
      this.cv.notifyAll();

      if (
        (!this.clearInputsAfterRun_ && !this.clearOutputsAfterRun_) ||
        this.nodeMap === null
      ) {
        if (!isOk(handlerStatus)) {
          throw new Error(
            `Action failed with status: ${handlerStatus.message}`,
          );
        }
        return;
      }

      if (this.clearInputsAfterRun_) {
        for (const input of this.definition.inputs) {
          this.nodeMap.removeNode(this.getInputId(input.name));
        }
      }

      if (this.clearOutputsAfterRun_) {
        for (const output of this.definition.outputs) {
          this.nodeMap.removeNode(this.getOutputId(output.name));
        }
      }

      if (!isOk(handlerStatus)) {
        throw new Error(`Action failed with status: ${handlerStatus.message}`);
      }
      return;
    });
  }

  async call(
    headers: Map<string, Uint8Array<ArrayBuffer>> | null = null,
  ): Promise<void> {
    this.bindStreamsOnInputsDefault = true;
    this.bindStreamsOnOutputsDefault = false;
    this.hasBeenCalled = true;

    if (this.stream !== null) {
      const actionMessage = this.getActionMessage();
      console.log(actionMessage);
      await this.stream.send({
        actions: [actionMessage],
        headers,
      });
    }
  }

  async callAndWaitForDispatchStatus(
    headers: Map<string, Uint8Array<ArrayBuffer>> | null = null,
  ): Promise<Status> {
    await this.call(headers);
    const dispatchStatusNode = this.getOutput('__dispatch_status__', false);
    const chunk = await dispatchStatusNode.next();
    if (chunk === null) {
      return internalError('No dispatch status received.');
    }
    return decodeStatus(chunk.data);
  }

  async awaitAction(timeoutMs?: number): Promise<Status> {
    return await this.mu.runExclusive(async () => {
      if (this.hasBeenRun) {
        const start = Date.now();
        while (this.runStatus === null) {
          const remaining = timeoutMs ? timeoutMs - (Date.now() - start) : -1;
          if (remaining !== undefined && remaining <= 0) {
            throw new Error('Timeout awaiting action');
          }
          await this.cv.waitWithTimeout(this.mu, remaining);
        }
        return this.runStatus;
      }

      if (this.hasBeenCalled) {
        const statusNode = this.getOutput('__status__', false);
        this.mu.release();
        try {
          const statusChunk = await statusNode.next();
          if (statusChunk === null) {
            return internalError('No status received.');
          }
          return decodeStatus(statusChunk.data);
        } finally {
          await this.mu.acquire();
        }
      }

      throw new Error('Action has not been run or called yet.');
    });
  }

  getNode(id: string): AsyncNode {
    return this.nodeMap.getNode(id);
  }

  getInput(name: string, bindStream: boolean | null = null): AsyncNode {
    const node = this.getNode(this.getInputId(name));
    let bindStreamResolved = bindStream;
    if (bindStream === null) {
      bindStreamResolved = this.bindStreamsOnInputsDefault;
    }
    if (this.stream !== null && bindStreamResolved) {
      node.bindWriterStream(this.stream).then(() => {
        this.nodesWithBoundStreams.add(node);
      });
    }
    return node;
  }

  getOutput(name: string, bindStream: boolean | null = null): AsyncNode {
    const node = this.getNode(this.getOutputId(name));
    let bindStreamResolved = bindStream;
    if (bindStream === null) {
      bindStreamResolved = this.bindStreamsOnOutputsDefault;
    }
    if (
      this.stream !== null &&
      bindStreamResolved &&
      name != '__status__' &&
      name != '__dispatch_status__'
    ) {
      node.bindWriterStream(this.stream).then(() => {
        this.nodesWithBoundStreams.add(node);
      });
    }
    return node;
  }

  bindHandler(handler: ActionHandler | null) {
    this.handler = handler;
  }

  bindNodeMap(nodeMap: NodeMap | null) {
    this.nodeMap = nodeMap;
  }

  getNodeMap(): NodeMap | null {
    return this.nodeMap as NodeMap;
  }

  bindStream(stream: BaseActionEngineStream | null) {
    this.stream = stream;
  }

  getStream(): BaseActionEngineStream | null {
    return this.stream as BaseActionEngineStream | null;
  }

  getRegistry(): ActionRegistry {
    return this.session.getActionRegistry();
  }

  bindSession(session: Session) {
    this.session = session;
  }

  getSession(): Session {
    return this.session as Session;
  }

  private getInputId(name: string): string {
    return `${this.id}#${name}`;
  }

  private getOutputId(name: string): string {
    return `${this.id}#${name}`;
  }

  private async unbindStreams() {
    for (const node of this.nodesWithBoundStreams) {
      await node.bindWriterStream(null);
    }
    this.nodesWithBoundStreams.clear();
  }
}

export function fromActionMessage(
  message: ActionMessage,
  registry: ActionRegistry,
  nodeMap: NodeMap,
  stream: BaseActionEngineStream,
  session: Session | null = null,
) {
  const inputs = message.inputs;
  const outputs = message.outputs;

  let actionIdSrc: string = '';
  if (inputs.length == 0) {
    actionIdSrc = outputs[0].id;
  } else {
    actionIdSrc = inputs[0].id;
  }

  const actionIdParts = actionIdSrc.split('#');
  const actionId = actionIdParts[0];

  const def = registry.definitions.get(message.name);
  const handler = registry.handlers.get(message.name);

  return new Action(def, actionId, handler, nodeMap, stream, session);
}
