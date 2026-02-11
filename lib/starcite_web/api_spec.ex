defmodule StarciteWeb.ApiSpec do
  @moduledoc """
  OpenAPI specification for Starcite HTTP endpoints.
  """

  @behaviour OpenApiSpex.OpenApi

  alias OpenApiSpex.{Info, OpenApi, Operation, PathItem, Response, Schema, Server}

  require OpenApiSpex

  @impl OpenApiSpex.OpenApi
  def spec do
    %OpenApi{
      info: %Info{
        title: "Starcite API",
        version: "0.1.0",
        description:
          "Session primitives for AI products: create sessions, append ordered events, and tail from cursor over WebSocket."
      },
      servers: [Server.from_endpoint(StarciteWeb.Endpoint)],
      paths: %{
        "/v1/sessions" => %PathItem{post: create_session_operation()},
        "/v1/sessions/{id}/append" => %PathItem{post: append_event_operation()},
        "/v1/sessions/{id}/tail" => %PathItem{get: tail_session_operation()},
        "/health/live" => %PathItem{get: health_live_operation()},
        "/health/ready" => %PathItem{get: health_ready_operation()}
      }
    }
    |> OpenApiSpex.resolve_schema_modules()
  end

  defp create_session_operation do
    %Operation{
      tags: ["sessions"],
      summary: "Create session",
      description: "Create a new session.",
      operationId: "SessionCreate",
      requestBody:
        Operation.request_body(
          "Session create payload",
          "application/json",
          __MODULE__.CreateSessionRequest,
          required: true
        ),
      responses: %{
        201 =>
          Operation.response("Session created", "application/json", __MODULE__.SessionResponse),
        422 =>
          Operation.response(
            "Invalid session payload",
            "application/json",
            __MODULE__.ValidationErrorResponse
          ),
        409 =>
          Operation.response(
            "Session already exists",
            "application/json",
            __MODULE__.ErrorResponse
          )
      }
    }
  end

  defp append_event_operation do
    %Operation{
      tags: ["sessions"],
      summary: "Append event",
      description: "Append one event to an existing session.",
      operationId: "SessionAppend",
      parameters: [session_id_parameter()],
      requestBody:
        Operation.request_body(
          "Session event payload",
          "application/json",
          __MODULE__.AppendEventRequest,
          required: true
        ),
      responses: %{
        201 =>
          Operation.response("Event appended", "application/json", __MODULE__.AppendEventResponse),
        422 =>
          Operation.response(
            "Invalid event payload",
            "application/json",
            __MODULE__.ValidationErrorResponse
          ),
        404 =>
          Operation.response("Session not found", "application/json", __MODULE__.ErrorResponse),
        409 =>
          Operation.response(
            "Expected sequence or idempotency conflict",
            "application/json",
            __MODULE__.ErrorResponse
          ),
        503 =>
          Operation.response("Cluster unavailable", "application/json", __MODULE__.ErrorResponse)
      }
    }
  end

  defp health_live_operation do
    %Operation{
      tags: ["health"],
      summary: "Liveness",
      description: "Liveness probe endpoint.",
      operationId: "HealthLive",
      responses: %{
        200 => Operation.response("Live", "application/json", __MODULE__.HealthStatusResponse)
      }
    }
  end

  defp tail_session_operation do
    %Operation{
      tags: ["sessions"],
      summary: "Tail events over WebSocket",
      description:
        "WebSocket upgrade endpoint. On connect, replays committed events where seq > cursor, then streams new committed events.",
      operationId: "SessionTail",
      parameters: [
        session_id_parameter(),
        Operation.parameter(
          :cursor,
          :query,
          %Schema{type: :integer, minimum: 0},
          "Replay cursor. Events with seq greater than this value are replayed first.",
          example: 0
        )
      ],
      responses: %{
        101 => %Response{description: "Switching protocols to WebSocket"},
        400 =>
          Operation.response(
            "Invalid cursor or upgrade request",
            "application/json",
            __MODULE__.ErrorResponse
          ),
        404 =>
          Operation.response("Session not found", "application/json", __MODULE__.ErrorResponse)
      }
    }
  end

  defp health_ready_operation do
    %Operation{
      tags: ["health"],
      summary: "Readiness",
      description: "Readiness probe endpoint.",
      operationId: "HealthReady",
      responses: %{
        200 => Operation.response("Ready", "application/json", __MODULE__.HealthStatusResponse)
      }
    }
  end

  defp session_id_parameter do
    Operation.parameter(:id, :path, :string, "Session identifier")
  end

  defmodule JsonObject do
    @moduledoc false

    OpenApiSpex.schema(
      %{
        title: "JsonObject",
        description: "Arbitrary JSON object.",
        type: :object,
        additionalProperties: true,
        example: %{"tenant_id" => "acme"}
      },
      struct?: false,
      derive?: false
    )
  end

  defmodule Refs do
    @moduledoc false

    OpenApiSpex.schema(
      %{
        title: "Refs",
        description: "Optional causal or request references.",
        type: :object,
        properties: %{
          to_seq: %Schema{type: :integer, minimum: 0},
          request_id: %Schema{type: :string},
          sequence_id: %Schema{type: :string},
          step: %Schema{type: :integer, minimum: 0}
        },
        additionalProperties: true,
        example: %{
          "to_seq" => 41,
          "request_id" => "req_123",
          "sequence_id" => "seq_alpha",
          "step" => 1
        }
      },
      struct?: false,
      derive?: false
    )
  end

  defmodule CreateSessionRequest do
    @moduledoc false

    OpenApiSpex.schema(
      %{
        title: "CreateSessionRequest",
        type: :object,
        properties: %{
          id: %Schema{type: :string, minLength: 1},
          title: %Schema{type: :string},
          metadata: StarciteWeb.ApiSpec.JsonObject
        },
        additionalProperties: true,
        example: %{
          "id" => "ses_custom_123",
          "title" => "Draft contract",
          "metadata" => %{"tenant_id" => "acme", "workflow" => "contract_drafting"}
        }
      },
      struct?: false,
      derive?: false
    )
  end

  defmodule AppendEventRequest do
    @moduledoc false

    OpenApiSpex.schema(
      %{
        title: "AppendEventRequest",
        type: :object,
        required: [:type, :payload, :actor],
        properties: %{
          type: %Schema{type: :string, minLength: 1},
          payload: StarciteWeb.ApiSpec.JsonObject,
          actor: %Schema{type: :string, minLength: 1},
          source: %Schema{type: :string, minLength: 1},
          metadata: StarciteWeb.ApiSpec.JsonObject,
          refs: StarciteWeb.ApiSpec.Refs,
          idempotency_key: %Schema{type: :string, minLength: 1},
          expected_seq: %Schema{type: :integer, minimum: 0}
        },
        additionalProperties: true,
        example: %{
          "type" => "state",
          "payload" => %{"state" => "running"},
          "actor" => "agent:researcher",
          "source" => "agent",
          "metadata" => %{"role" => "worker", "identity" => %{"provider" => "codex"}},
          "refs" => %{
            "to_seq" => 41,
            "request_id" => "req_123",
            "sequence_id" => "seq_alpha",
            "step" => 1
          },
          "idempotency_key" => "run_123-step_8",
          "expected_seq" => 41
        }
      },
      struct?: false,
      derive?: false
    )
  end

  defmodule SessionResponse do
    @moduledoc false

    OpenApiSpex.schema(
      %{
        title: "SessionResponse",
        type: :object,
        required: [:id, :metadata, :last_seq, :created_at, :updated_at],
        properties: %{
          id: %Schema{type: :string, minLength: 1},
          title: %Schema{type: :string, nullable: true},
          metadata: StarciteWeb.ApiSpec.JsonObject,
          last_seq: %Schema{type: :integer, minimum: 0},
          created_at: %Schema{type: :string, format: :"date-time"},
          updated_at: %Schema{type: :string, format: :"date-time"}
        },
        additionalProperties: false,
        example: %{
          "id" => "ses_custom_123",
          "title" => "Draft contract",
          "metadata" => %{"tenant_id" => "acme", "workflow" => "contract_drafting"},
          "last_seq" => 0,
          "created_at" => "2026-02-08T15:00:00Z",
          "updated_at" => "2026-02-08T15:00:00Z"
        }
      },
      struct?: false,
      derive?: false
    )
  end

  defmodule AppendEventResponse do
    @moduledoc false

    OpenApiSpex.schema(
      %{
        title: "AppendEventResponse",
        type: :object,
        required: [:seq, :last_seq, :deduped],
        properties: %{
          seq: %Schema{type: :integer, minimum: 0},
          last_seq: %Schema{type: :integer, minimum: 0},
          deduped: %Schema{type: :boolean}
        },
        additionalProperties: false,
        example: %{
          "seq" => 42,
          "last_seq" => 42,
          "deduped" => false
        }
      },
      struct?: false,
      derive?: false
    )
  end

  defmodule HealthStatusResponse do
    @moduledoc false

    OpenApiSpex.schema(
      %{
        title: "HealthStatusResponse",
        type: :object,
        required: [:status],
        properties: %{
          status: %Schema{type: :string, enum: ["ok"]}
        },
        additionalProperties: false,
        example: %{"status" => "ok"}
      },
      struct?: false,
      derive?: false
    )
  end

  defmodule ErrorResponse do
    @moduledoc false

    OpenApiSpex.schema(
      %{
        title: "ErrorResponse",
        type: :object,
        required: [:error, :message],
        properties: %{
          error: %Schema{type: :string},
          message: %Schema{type: :string}
        },
        additionalProperties: false,
        example: %{
          "error" => "expected_seq_conflict",
          "message" => "Expected seq 41, current seq is 42"
        }
      },
      struct?: false,
      derive?: false
    )
  end

  defmodule ValidationErrorSource do
    @moduledoc false

    OpenApiSpex.schema(
      %{
        title: "ValidationErrorSource",
        type: :object,
        required: [:pointer],
        properties: %{
          pointer: %Schema{type: :string}
        },
        additionalProperties: false,
        example: %{"pointer" => "/metadata"}
      },
      struct?: false,
      derive?: false
    )
  end

  defmodule ValidationErrorItem do
    @moduledoc false

    OpenApiSpex.schema(
      %{
        title: "ValidationErrorItem",
        type: :object,
        required: [:title, :source, :detail],
        properties: %{
          title: %Schema{type: :string},
          source: StarciteWeb.ApiSpec.ValidationErrorSource,
          detail: %Schema{type: :string}
        },
        additionalProperties: false,
        example: %{
          "title" => "Invalid value",
          "source" => %{"pointer" => "/metadata"},
          "detail" => "Invalid object. Got: string"
        }
      },
      struct?: false,
      derive?: false
    )
  end

  defmodule ValidationErrorResponse do
    @moduledoc false

    OpenApiSpex.schema(
      %{
        title: "ValidationErrorResponse",
        type: :object,
        required: [:errors],
        properties: %{
          errors: %Schema{
            type: :array,
            items: StarciteWeb.ApiSpec.ValidationErrorItem
          }
        },
        additionalProperties: false,
        example: %{
          "errors" => [
            %{
              "title" => "Invalid value",
              "source" => %{"pointer" => "/metadata"},
              "detail" => "Invalid object. Got: string"
            }
          ]
        }
      },
      struct?: false,
      derive?: false
    )
  end
end
