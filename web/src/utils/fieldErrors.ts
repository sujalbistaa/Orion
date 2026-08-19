import { ApiError } from "../api/client";
import type { FieldError } from "../api/types";

/** Extracts field-level validation errors (pkg/api/v1 ValidationError) from a caught error. */
export function extractFieldErrors(err: unknown): FieldError[] {
  if (err instanceof ApiError && err.fields) return err.fields;
  return [];
}

export function fieldError(fields: FieldError[], name: string): string | undefined {
  return fields.find((f) => f.field === name)?.detail;
}
