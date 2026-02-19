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

import { Chunk } from './data';

export enum CanonicalStatusCode {
  OK = 0,
  CANCELLED = 1,
  UNKNOWN = 2,
  INVALID_ARGUMENT = 3,
  DEADLINE_EXCEEDED = 4,
  NOT_FOUND = 5,
  ALREADY_EXISTS = 6,
  PERMISSION_DENIED = 7,
  RESOURCE_EXHAUSTED = 8,
  FAILED_PRECONDITION = 9,
  ABORTED = 10,
  OUT_OF_RANGE = 11,
  UNIMPLEMENTED = 12,
  INTERNAL = 13,
  UNAVAILABLE = 14,
  DATA_LOSS = 15,
  UNAUTHENTICATED = 16,
}

export interface Status {
  code: CanonicalStatusCode;
  message: string;
  details?: unknown[];
}

export const getStatusFromNativeError: (error: Error) => Status = (
  error: Error,
) => {
  return {
    code: CanonicalStatusCode.INTERNAL,
    message: error.message,
    details: [],
  };
};

export const isOk = (status: Status) => {
  return status.code === CanonicalStatusCode.OK;
};

export const okStatus: () => Status = () => {
  return {
    code: CanonicalStatusCode.OK,
    message: 'OK',
  };
};

export const cancelledError: (message: string) => Status = (
  message: string,
) => {
  return {
    code: CanonicalStatusCode.CANCELLED,
    message: message,
  };
};

export const unknownError: (message: string) => Status = (message: string) => {
  return {
    code: CanonicalStatusCode.UNKNOWN,
    message: message,
  };
};

export const invalidArgumentError: (message: string) => Status = (
  message: string,
) => {
  return {
    code: CanonicalStatusCode.INVALID_ARGUMENT,
    message: message,
  };
};

export const deadlineExceededError: (message: string) => Status = (
  message: string,
) => {
  return {
    code: CanonicalStatusCode.DEADLINE_EXCEEDED,
    message: message,
  };
};

export const notFoundError: (message: string) => Status = (message: string) => {
  return {
    code: CanonicalStatusCode.NOT_FOUND,
    message: message,
  };
};

export const alreadyExistsError: (message: string) => Status = (
  message: string,
) => {
  return {
    code: CanonicalStatusCode.ALREADY_EXISTS,
    message: message,
  };
};

export const permissionDeniedError: (message: string) => Status = (
  message: string,
) => {
  return {
    code: CanonicalStatusCode.PERMISSION_DENIED,
    message: message,
  };
};

export const resourceExhaustedError: (message: string) => Status = (
  message: string,
) => {
  return {
    code: CanonicalStatusCode.RESOURCE_EXHAUSTED,
    message: message,
  };
};

export const failedPreconditionError: (message: string) => Status = (
  message: string,
) => {
  return {
    code: CanonicalStatusCode.FAILED_PRECONDITION,
    message: message,
  };
};

export const abortedError: (message: string) => Status = (message: string) => {
  return {
    code: CanonicalStatusCode.ABORTED,
    message: message,
  };
};

export const outOfRangeError: (message: string) => Status = (
  message: string,
) => {
  return {
    code: CanonicalStatusCode.OUT_OF_RANGE,
    message: message,
  };
};

export const unimplementedError: (message: string) => Status = (
  message: string,
) => {
  return {
    code: CanonicalStatusCode.UNIMPLEMENTED,
    message: message,
  };
};

export const internalError: (message: string) => Status = (message: string) => {
  return {
    code: CanonicalStatusCode.INTERNAL,
    message: message,
  };
};

export const unavailableError: (message: string) => Status = (
  message: string,
) => {
  return {
    code: CanonicalStatusCode.UNAVAILABLE,
    message: message,
  };
};
export const dataLossError: (message: string) => Status = (message: string) => {
  return {
    code: CanonicalStatusCode.DATA_LOSS,
    message: message,
  };
};
export const unauthenticatedError: (message: string) => Status = (
  message: string,
) => {
  return {
    code: CanonicalStatusCode.UNAUTHENTICATED,
    message: message,
  };
};

export const isStatusChunk = (chunk: Chunk) => {
  return chunk.metadata.mimetype === '__status__';
};
