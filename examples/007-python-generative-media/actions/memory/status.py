from enum import IntEnum

from fastapi import HTTPException
from pydantic import BaseModel, Field
from absl import logging


class StatusCode(IntEnum):
    """Canonical gRPC / Abseil status codes, mapped to HTTP status codes."""

    OK = 0
    CANCELLED = 1
    UNKNOWN = 2
    INVALID_ARGUMENT = 3
    DEADLINE_EXCEEDED = 4
    NOT_FOUND = 5
    ALREADY_EXISTS = 6
    PERMISSION_DENIED = 7
    RESOURCE_EXHAUSTED = 8
    FAILED_PRECONDITION = 9
    ABORTED = 10
    OUT_OF_RANGE = 11
    UNIMPLEMENTED = 12
    INTERNAL = 13
    UNAVAILABLE = 14
    DATA_LOSS = 15
    UNAUTHENTICATED = 16

    @classmethod
    def __get_pydantic_json_schema__(cls, core_schema, handler):
        schema = handler(core_schema)
        schema["description"] = "\n\n".join(
            f"{member.value} = {member.name}" for member in cls
        )
        return schema

    @staticmethod
    def from_http_code(http_code: int) -> "StatusCode":
        if http_code in range(200, 300):
            return StatusCode.OK

        if http_code in range(400, 500):
            if http_code == 400:
                return StatusCode.INVALID_ARGUMENT
            if http_code == 401:
                return StatusCode.UNAUTHENTICATED
            if http_code == 403:
                return StatusCode.PERMISSION_DENIED
            if http_code == 404:
                return StatusCode.NOT_FOUND
            if http_code == 409:
                return StatusCode.ABORTED
            if http_code == 429:
                return StatusCode.RESOURCE_EXHAUSTED
            return StatusCode.FAILED_PRECONDITION

        if http_code in range(500, 600):
            if http_code == 501:
                return StatusCode.UNIMPLEMENTED
            if http_code == 503:
                return StatusCode.UNAVAILABLE
            return StatusCode.INTERNAL

        return StatusCode.UNKNOWN

    def to_ws_code(self) -> int:
        mapping = {
            self.OK: 1000,
            self.CANCELLED: 4000,
            self.UNKNOWN: 4001,
            self.INVALID_ARGUMENT: 4002,
            self.DEADLINE_EXCEEDED: 4003,
            self.NOT_FOUND: 4004,
            self.ALREADY_EXISTS: 4005,
            self.PERMISSION_DENIED: 4006,
            self.UNAUTHENTICATED: 4007,
            self.RESOURCE_EXHAUSTED: 4008,
            self.FAILED_PRECONDITION: 4009,
            self.ABORTED: 4010,
            self.OUT_OF_RANGE: 4011,
            self.UNIMPLEMENTED: 4012,
            self.INTERNAL: 4013,
            self.UNAVAILABLE: 4014,
            self.DATA_LOSS: 4015,
        }
        return mapping.get(self, 4000)

    def to_http_code(self) -> int:
        mapping = {
            self.OK: 200,
            self.CANCELLED: 499,
            self.UNKNOWN: 500,
            self.INVALID_ARGUMENT: 400,
            self.DEADLINE_EXCEEDED: 504,
            self.NOT_FOUND: 404,
            self.ALREADY_EXISTS: 409,
            self.PERMISSION_DENIED: 403,
            self.RESOURCE_EXHAUSTED: 429,
            self.FAILED_PRECONDITION: 400,
            self.ABORTED: 409,
            self.OUT_OF_RANGE: 400,
            self.UNIMPLEMENTED: 501,
            self.INTERNAL: 500,
            self.UNAVAILABLE: 503,
            self.DATA_LOSS: 500,
            self.UNAUTHENTICATED: 401,
        }
        return mapping.get(self, 500)

    def __str__(self) -> str:
        return self.name


class StatusException(Exception):
    def __init__(self, status: "Status"):
        if status.is_ok():
            raise ValueError(
                "Cannot create a StatusException for an OK status."
            )

        self.status = status
        super().__init__(status.message)

    def __str__(self) -> str:
        return f"{self.status.code}: {self.status.message}"

    def __repr__(self) -> str:
        return f"code: {self.status.code.name}, message: {self.status.message}"


class Status(BaseModel):
    code: StatusCode = Field(
        default=StatusCode.OK,
        description=(
            "The status code representing the outcome. Allowed values are: "
            f" {
            ', '.join(
                f'{member.value} = {member.name}' for member in StatusCode
            )
            }."
        ),
    )
    message: str = Field(
        default="OK",
        description=(
            "A human-readable message providing more details about the status."
        ),
    )
    details: list[dict] = Field(
        default_factory=list,
        description=(
            "A list of additional details about the status, represented as"
            " arbitrary JSON objects."
        ),
        exclude_if=lambda x: not x,
    )

    @staticmethod
    def from_http_exception(
        http_exception: HTTPException,
    ) -> "Status":
        code = StatusCode.from_http_code(http_exception.status_code)
        detail = http_exception.detail
        message = "An error occurred."
        details = []

        if isinstance(detail, dict):
            message = detail.get("message", message)
            details = detail.get("details", details)
        elif isinstance(detail, str):
            message = detail

        return Status(code=code, message=message, details=details)

    @staticmethod
    def get_fastapi_response_dict_for_codes(
        *codes: StatusCode,
    ) -> dict[int, dict]:
        responses = {}
        for code in codes:
            responses[code.to_http_code()] = {
                "model": Status,
                "content": {
                    "application/json": {
                        "example": _STATUS_EXAMPLES[code],
                    }
                },
            }

        validation_error_example = _STATUS_EXAMPLES[
            StatusCode.INVALID_ARGUMENT
        ].model_copy()
        validation_error_example.details = [
            {
                "type": "missing",
                "loc": ["body", "name"],
                "msg": "Field required",
                "input": {},
            }
        ]
        responses[422] = {
            "model": Status,
            "content": {
                "application/json": {
                    "example": validation_error_example,
                }
            },
        }
        return responses

    @staticmethod
    def get_fastapi_response_dict_for_http_codes(
        *codes: int,
    ) -> dict[int, dict]:
        responses = {}
        for http_code in codes:
            responses[http_code] = {"model": Status}
        return responses

    @staticmethod
    def ok(message: str | None = None) -> "Status":
        return Status(
            code=StatusCode.OK,
            message="OK" if message is None else message,
            details=[],
        )

    def is_ok(self) -> bool:
        return self.code == StatusCode.OK

    def to_exception(self) -> Exception:
        if self.is_ok():
            raise ValueError("Cannot convert an OK status to an exception.")
        exc = StatusException(self)
        logging.error(self.message, exc_info=exc, stacklevel=2)
        return exc


_STATUS_EXAMPLES = {
    StatusCode.OK: Status.ok(),
    StatusCode.CANCELLED: Status(
        code=StatusCode.CANCELLED,
        message="The request was cancelled, typically by the caller",
    ),
    StatusCode.UNKNOWN: Status(
        code=StatusCode.UNKNOWN,
        message="There is no way to determine a more specific error code",
    ),
    StatusCode.INVALID_ARGUMENT: Status(
        code=StatusCode.INVALID_ARGUMENT,
        message="The request parameters would never work (validation error)",
    ),
    StatusCode.DEADLINE_EXCEEDED: Status(
        code=StatusCode.DEADLINE_EXCEEDED,
        message="The operation did not complete within the specified deadline",
    ),
    StatusCode.NOT_FOUND: Status(
        code=StatusCode.NOT_FOUND,
        message=(
            "The requested entity does not exist (or sometimes, the requester"
            " does not have access to it)"
        ),
    ),
    StatusCode.ALREADY_EXISTS: Status(
        code=StatusCode.ALREADY_EXISTS,
        message="The entity being created already exists",
    ),
    StatusCode.PERMISSION_DENIED: Status(
        code=StatusCode.PERMISSION_DENIED,
        message="The caller does not have permission to execute the operation",
    ),
    StatusCode.UNAUTHENTICATED: Status(
        code=StatusCode.UNAUTHENTICATED,
        message="The caller’s identity cannot be verified",
    ),
    StatusCode.RESOURCE_EXHAUSTED: Status(
        code=StatusCode.RESOURCE_EXHAUSTED,
        message=(
            "Some infrastructure resource is exhausted (quota, server "
            "capacity, etc); does not always imply caller's fault in \"Too "
            'Many Requests"'
        ),
    ),
    StatusCode.FAILED_PRECONDITION: Status(
        code=StatusCode.FAILED_PRECONDITION,
        message="The system is not in the required state for the operation",
    ),
    StatusCode.ABORTED: Status(
        code=StatusCode.ABORTED,
        message=(
            "The operation was aborted, typically due to a concurrency issue"
            " like sequencer check failures, transaction aborts, etc."
        ),
    ),
    StatusCode.OUT_OF_RANGE: Status(
        code=StatusCode.OUT_OF_RANGE,
        message="The client has iterated too far, and should stop",
    ),
    StatusCode.UNIMPLEMENTED: Status(
        code=StatusCode.UNIMPLEMENTED,
        message="There is no implementation for the requested operation",
    ),
    StatusCode.INTERNAL: Status(
        code=StatusCode.INTERNAL,
        message=(
            "A serious internal invariant is broken (i.e. worthy of a bug or"
            " outage report)"
        ),
    ),
    StatusCode.UNAVAILABLE: Status(
        code=StatusCode.UNAVAILABLE, message="Unavailable"
    ),
    StatusCode.DATA_LOSS: Status(
        code=StatusCode.DATA_LOSS,
        message="Unrecoverable data loss or corruption",
    ),
}


def make_http_exception_from(status: Status) -> HTTPException:
    return HTTPException(
        status_code=status.code.to_http_code(),
        detail={
            "message": status.message,
            "details": status.details,
        },
    )
