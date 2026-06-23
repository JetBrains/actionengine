import datetime
import uuid
from enum import StrEnum
from typing import Any, Literal

import ormsgpack
from pydantic import BaseModel, Field, model_validator

from . import status

Status = status.Status
StatusCode = status.StatusCode


class ActionName(StrEnum):
    MEMORIES_CREATE = "memory_memories_create"
    MEMORIES_GET = "memory_memories_get"
    MEMORIES_UPDATE = "memory_memories_update"
    MEMORIES_DELETE = "memory_memories_delete"
    MEMORIES_SEARCH = "memory_memories_search"
    SCHEMAS_CREATE = "memory_schemas_create"
    SCHEMAS_SEARCH = "memory_schemas_search"
    SCHEMAS_GET = "memory_schemas_get"


debug = {
    "memory": [
        {
            "title": "DBSummary Olist ecommerce SQLite schema",
            "schema_name": "DBSummary",
            "description": "SQLite database contains Brazilian Olist ecommerce data: customers, orders, order_items, payments, reviews, products, sellers, geolocation, and product_category_name_translation. Main joins: orders.customer_id->customers.customer_id; order_items.order_id->orders.order_id; order_items.product_id->products.product_id; order_items.seller_id->sellers.seller_id; payments/reviews.order_id->orders.order_id; products.product_category_name->translation.product_category_name; zip_code_prefix links customers/sellers to geolocation but geolocation has many rows per prefix. 9 tables; no declared PK/FK constraints; one index ix_olist_products_dataset_index on olist_products_dataset(index). Key counts: orders/customers 99,441; items 112,650; payments 103,886; reviews 99,224; products 32,951; sellers 3,095; geolocation 1,000,163; translations 71. Dates 2016-09 to 2018-10. Category names are Portuguese; 13 product categories lack translations. IDs are anonymized hashes. product_name_lenght and product_description_lenght are misspelled. olist_products_dataset duplicates olist_products plus an index column, no duplicate product_id.",
            "brief_description": "Olist ecommerce schema summary with tables, joins, counts, caveats, and index/constraint notes.",
            "awaiting_feedback": False,
        }
    ]
}


class MemoryInBase(BaseModel):
    title: str | None = Field(
        default=None,
        description=(
            "The memory's title. Unique per owner; also the key used to "
            "update the memory."
        ),
    )

    schema_name: str | None = Field(
        default=None,
        description="The name of the schema to use.",
    )

    object: dict | None = Field(
        default=None,
        description="The object to store. Validated against the schema at insert time, if schema is provided. If you are an LLM, you MUST always populate this field.",
    )

    description: str | None = Field(
        default=None,
        description="A description of the memory.",
        exclude_if=lambda x: not x,
    )

    brief_description: str | None = Field(
        default=None,
        description="A brief description of the memory.",
        exclude_if=lambda x: not x,
    )

    metadata: dict | None = Field(
        default=None, description="Additional metadata for the memory."
    )

    correlation_id: str | None = Field(
        default=None,
        description="The correlation ID (originator context) of the memory.",
        exclude_if=lambda x: not x,
    )

    scope: str | None = Field(
        default=None,
        description="The scope of the memory.",
        exclude_if=lambda x: not x,
    )

    qa_session_id: str | None = Field(
        default=None,
        description="Id of claude's session.",
        exclude_if=lambda x: not x,
    )

    mem_session_id: str | None = Field(
        default=None,
        description="Id of claude's session.",
        exclude_if=lambda x: not x,
    )

    awaiting_feedback: bool | None = Field(
        default=None,
        description="Whether the memory is awaiting feedback from the user. "
        "If so, it signifies that the memory agent must revise the memory given the "
        "the user feedback in the same session.",
    )

    @model_validator(mode="after")
    def check_object(self):
        if self.schema_name is not None and self.object is None:
            raise Status(
                code=StatusCode.INVALID_ARGUMENT,
                message="If `schema_name` is provided, `object` must be provided too.",
            ).to_exception()
        if not any(
            value for value in self.model_dump().values() if value is not None
        ):
            raise Status(
                code=StatusCode.INVALID_ARGUMENT,
                message="At least one of the value must be not null.",
            ).to_exception()

        return self


class CreateMemoryRequest(MemoryInBase):
    title: str = Field(
        min_length=1,
        description=(
            "The memory's title. Unique per owner; also the key used to"
            " update the memory."
        ),
    )

    awaiting_feedback: bool = Field(
        default=True,
        description="Whether the memory is awaiting feedback from the user. "
        "If so, it signifies that the memory agent must revise the memory given the "
        "the user feedback in the same session.",
    )


class RawMemoryOut(BaseModel):
    @staticmethod
    def ensure_str(value: Any) -> Any:
        return str(value) if value is not None else None

    uid: uuid.UUID = Field(
        description="The UID of the memory.",
    )

    title: str | None = Field(
        default=None,
        description="The title of the memory.",
        exclude_if=lambda x: x is None,
    )

    description: str | None = Field(
        default=None,
        description="The description of the memory.",
        exclude_if=lambda x: x is None,
    )
    brief_description: str | None = Field(
        default=None,
        description=(
            "The brief description of the memory. This field is only "
            "available in admin / service views. Otherwise, even if brief "
            "descriptions, they will be in the `description` field."
        ),
        exclude_if=lambda x: x is None,
    )
    metadata: dict | None = Field(
        default=None,
        description="The metadata of the memory.",
        exclude_if=lambda x: x is None,
    )

    correlation_id: str | None = Field(
        default=None,
        description="The correlation ID of the memory.",
        exclude_if=lambda x: not x,
    )
    owner: str | None = Field(
        description="The owner of the memory.",
        exclude_if=lambda x: False,
    )

    schema_name: str | None = Field(
        default=None,
        description="The schema name of the memory.",
        exclude_if=lambda x: x is None,
    )
    object: bytes | None = Field(
        default=None,
        description="The object of the memory.",
        exclude_if=lambda x: x is None,
    )

    created_at: datetime.datetime | None = Field(
        default=None,
        description="The creation date of the memory.",
        exclude_if=lambda x: x is None,
    )
    updated_at: datetime.datetime | None = Field(
        default=None,
        description="The last update date of the memory.",
        exclude_if=lambda x: x is None,
    )

    awaiting_feedback: bool = Field(
        default=True,
        description="Whether the memory is awaiting feedback from the user. "
        "If so, it signifies that the memory agent must revise the memory given the "
        "the user feedback in the same session.",
    )


class ObjectMemoryOut(RawMemoryOut):
    encoding: Literal["json", "msgpack"] | None = Field(
        default="json", description="The encoding of the memory object."
    )
    object: dict | bytes | str | None = Field(
        default=None,
        description="The object of the memory.",
        exclude_if=lambda x: x is None,
    )

    @model_validator(mode="before")
    @classmethod
    def respect_encoding(cls, data: Any):
        if not isinstance(data, dict):
            raise Status(
                code=StatusCode.INVALID_ARGUMENT,
                message=(
                    "Unexpected raw data type for ObjectMemoryOut:"
                    f" {type(data)}"
                ),
            ).to_exception()

        obj = data.get("object")
        encoding = data.get("encoding")

        # deduce encoding if not set
        if not encoding:
            if isinstance(obj, dict):
                encoding = data["encoding"] = "json"
            elif isinstance(obj, bytes):
                encoding = data["encoding"] = "msgpack"

        # convert object to desired encoding
        if encoding == "json" and isinstance(obj, bytes):
            data["object"] = ormsgpack.unpackb(obj)
        if encoding == "msgpack" and isinstance(obj, dict):
            data["object"] = ormsgpack.packb(obj)

        return data


class SearchRequest(BaseModel):
    correlation_id: str = Field(
        default="",
        description="The correlation ID of the memories to retrieve.",
    )

    owner: str = Field(
        default="",
        description="The owner of the memories to retrieve.",
    )

    types: list[str] = Field(
        default_factory=list,
        description="The types (schema names) of the memories to retrieve.",
    )

    scopes: list[str] = Field(
        default_factory=list,
        description="The scopes of the memories to retrieve.",
    )

    return_awaiting_feedback: bool = Field(
        default=False,
        description="Whether to return memories that are awaiting feedback. ",
    )

    search: str = Field(
        default="",
        max_length=1024,
        description=(
            "The search query to filter memories by (through description and "
            "metadata)."
        ),
    )

    title: str = Field(
        default="",
        description="The title of the memory to retrieve. "
        "If set, the `search` parameter must be empty - ",
    )

    output_order: list[
        Literal[
            "created_at",
            "-created_at",
            "updated_at",
            "-updated_at",
        ]
    ] = Field(
        default_factory=list,
        description=(
            "The order in which to return memories. The `-` before the column "
            "stands for in 'descending order'. If the `search` parameter is not empty, "
            "additionally the result set will be ranked by the fts rank."
        ),
    )

    output_description: Literal["full", "brief"] | None = Field(
        default=None,
        description=(
            "Whether to include the description of the memories, and which"
            " type."
        ),
    )

    output_text_template: str = Field(
        default="",
        description=(
            "The template to render the memories with. If set, `types` must"
            " either be empty or a single type."
        ),
    )

    @model_validator(mode="after")
    def check_output_type(self):
        if self.output_text_template and len(self.types) > 1:
            raise Status(
                code=StatusCode.INVALID_ARGUMENT,
                message=(
                    "If `output_text_template` is set, `types` must be a "
                    "single type or empty."
                ),
            ).to_exception()
        return self

    @model_validator(mode="after")
    def check_title(self):
        if self.title and self.search:
            raise Status(
                code=StatusCode.INVALID_ARGUMENT,
                message="Only one of `title` or `search` can be provided.",
            ).to_exception()
        return self


class QueryParameters(BaseModel):
    offset: int = Field(
        default=0,
        ge=0,
        description="The offset of the first object to return.",
    )
    limit: int = Field(
        default=100,
        ge=1,
        le=100,
        description="The maximum number of objects to return.",
    )


class SearchSchemasRequest(BaseModel):
    search: str | None = Field(
        default="",
        max_length=1024,
        description=(
            "The search query to select schemas by (through name and "
            "description)."
        ),
    )

    owner: str | None = Field(
        default="",
        description="The owner of the schemas to retrieve.",
    )


class _SchemaExample(BaseModel):
    model_config = {
        "json_schema_extra": {
            "title": "SqlRightAndWrongUsageTip",
            "example": {
                "tip": (
                    "Never `SELECT` all columns from a table. Always specify"
                    " the columns you need."
                ),
                "example": "SELECT name, age FROM users",
                "wrong_example": "SELECT * FROM users",
                "source": "system",
            },
        },
    }
    tip: str = Field(..., description="The usage tip text.", max_length=1024)
    example: str = Field(..., description="An example of the usage.")
    wrong_example: str | None = Field(
        default=None, description="A wrong example of the usage."
    )
    source: Literal["user", "system", "agent"] = Field(
        default="agent", description="The source of the usage tip."
    )


class SchemaOut(BaseModel):
    model_config = {
        "json_schema_extra": {
            "example": {
                "name": "SqlRightAndWrongUsageTip",
                "definition": _SchemaExample.model_json_schema(),
                "description": (
                    "An SQL usage tip, with a right and wrong example."
                ),
            }
        }
    }

    name: str | None = Field(
        default=None, description="The name of the schema."
    )
    definition: dict | None = Field(
        default=None, description="The schema definition."
    )
    description: str | None = Field(
        default=None, description="A description of the schema."
    )
