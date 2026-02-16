defmodule StarciteWeb.ErrorJSON do
  @moduledoc false

  def render("404.json", _assigns), do: %{error: "not_found"}
  def render("500.json", _assigns), do: %{error: "internal_server_error"}

  def template_not_found(_template, assigns), do: render("500.json", assigns)
end
