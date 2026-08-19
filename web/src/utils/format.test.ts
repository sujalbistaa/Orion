import { describe, expect, it } from "vitest";
import { formatAge, formatBytes, formatCPU, formatGoDuration, formatPercent } from "./format";

describe("formatCPU", () => {
  it("renders whole cores without decimals", () => {
    expect(formatCPU(2000)).toBe("2 vCPU");
  });
  it("renders fractional cores", () => {
    expect(formatCPU(250)).toBe("0.25 vCPU");
  });
  it("renders zero", () => {
    expect(formatCPU(0)).toBe("0");
  });
});

describe("formatBytes", () => {
  it("renders bytes below 1024 as-is", () => {
    expect(formatBytes(512)).toBe("512 B");
  });
  it("renders MiB", () => {
    expect(formatBytes(256 * 1024 * 1024)).toBe("256 MiB");
  });
  it("renders GiB with one decimal", () => {
    expect(formatBytes(1.5 * 1024 * 1024 * 1024)).toBe("1.5 GiB");
  });
});

describe("formatPercent", () => {
  it("computes a rounded percentage", () => {
    expect(formatPercent(50, 200)).toBe("25%");
  });
  it("returns an em dash for zero total", () => {
    expect(formatPercent(5, 0)).toBe("—");
  });
});

describe("formatAge", () => {
  it("returns an em dash for an undefined timestamp", () => {
    expect(formatAge(undefined)).toBe("—");
  });
  it("renders seconds for a recent timestamp", () => {
    const now = new Date().toISOString();
    expect(formatAge(now)).toMatch(/^\d+s$/);
  });
});

describe("formatGoDuration", () => {
  it("humanizes a combined duration", () => {
    expect(formatGoDuration("1h30m0s")).toBe("1h 30m");
  });
  it("passes through an em dash for empty input", () => {
    expect(formatGoDuration(undefined)).toBe("—");
  });
});
