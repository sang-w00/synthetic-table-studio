import { BarChart } from "echarts/charts";
import { GridComponent, TooltipComponent } from "echarts/components";
import * as echarts from "echarts/core";
import { CanvasRenderer } from "echarts/renderers";
import { useEffect, useRef } from "react";

import type { PrimaryReport } from "./api";

echarts.use([BarChart, GridComponent, TooltipComponent, CanvasRenderer]);

interface ReportChartProps {
  report: PrimaryReport;
}

export function ReportChart({ report }: ReportChartProps) {
  const host = useRef<HTMLDivElement>(null);
  const columns = (report.columns ?? []).filter(
    (column) => typeof column.distance === "number" || typeof column.baseline_excess === "number",
  );

  useEffect(() => {
    if (!host.current || columns.length === 0) return;
    const tokens = getComputedStyle(document.documentElement);
    const accent = tokens.getPropertyValue("--color-accent").trim();
    const focus = tokens.getPropertyValue("--color-focus").trim();
    const muted = tokens.getPropertyValue("--color-muted").trim();
    const line = tokens.getPropertyValue("--color-line").trim();
    const lineStrong = tokens.getPropertyValue("--color-line-strong").trim();
    const chart = echarts.init(host.current, undefined, { renderer: "canvas" });
    chart.setOption({
      animationDuration: 160,
      color: [accent, focus],
      grid: { left: 42, right: 16, top: 24, bottom: 58 },
      tooltip: { trigger: "axis" },
      xAxis: {
        type: "category",
        data: columns.map((column) => column.name),
        axisLabel: { rotate: columns.length > 4 ? 28 : 0, color: muted },
        axisLine: { lineStyle: { color: lineStrong } },
      },
      yAxis: {
        type: "value",
        min: 0,
        max: 1,
        name: "거리 (0–1)",
        nameTextStyle: { color: muted },
        axisLabel: { color: muted },
        splitLine: { lineStyle: { color: line } },
      },
      series: [
        {
          name: "합성 거리",
          type: "bar",
          data: columns.map((column) => column.distance ?? 0),
          itemStyle: { borderRadius: [3, 3, 0, 0] },
        },
        {
          name: "기준선 초과",
          type: "bar",
          data: columns.map((column) => column.baseline_excess ?? 0),
          itemStyle: { borderRadius: [3, 3, 0, 0] },
        },
      ],
    });
    const observer = new ResizeObserver(() => chart.resize());
    observer.observe(host.current);
    return () => {
      observer.disconnect();
      chart.dispose();
    };
  }, [columns]);

  if (columns.length === 0) {
    return <p className="empty-note">표시할 적용 가능한 열 거리 집계가 없습니다.</p>;
  }

  return (
    <div
      ref={host}
      className="report-chart"
      role="img"
      aria-label="열별 합성 거리와 기준선 초과 막대 차트"
      data-testid="report-chart"
    />
  );
}
