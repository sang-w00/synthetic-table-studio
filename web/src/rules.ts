import type { RuleSpec } from "./api";

function writtenColumns(rule: RuleSpec): string[] {
  switch (rule.kind) {
    case "mask_prefix":
      return [rule.column];
    case "fixed_combination":
      return rule.columns;
    case "conditional_set":
    case "sum_equals":
      return [rule.target];
    case "compare":
      return [rule.right];
    default:
      return [];
  }
}

function dependencyEdges(rule: RuleSpec): Array<[string, string]> {
  switch (rule.kind) {
    case "conditional_set":
      return [[rule.when.column, rule.target]];
    case "sum_equals":
      return rule.sources.map((source) => [source, rule.target]);
    case "compare":
      return [[rule.left, rule.right]];
    default:
      return [];
  }
}

export function findRuleConflicts(rules: RuleSpec[]): string[] {
  const conflicts: string[] = [];
  const ids = new Set<string>();
  const writers = new Map<string, string>();
  const graph = new Map<string, Set<string>>();

  for (const rule of rules) {
    if (ids.has(rule.id)) conflicts.push(`규칙 ID '${rule.id}'가 중복되었습니다.`);
    ids.add(rule.id);

    if (rule.kind === "sum_equals" && rule.sources.includes(rule.target)) {
      conflicts.push(`합계 대상 '${rule.target}'은 원본 열 목록에 포함될 수 없습니다.`);
    }
    if (rule.kind === "compare" && rule.left === rule.right) {
      conflicts.push(`비교 규칙의 왼쪽과 오른쪽 열은 달라야 합니다.`);
    }
    if (rule.kind === "fixed_combination") {
      if (rule.columns.length < 2) conflicts.push("고정 조합에는 두 개 이상의 열이 필요합니다.");
      if (new Set(rule.columns).size !== rule.columns.length) {
        conflicts.push("고정 조합의 열 목록에 중복이 있습니다.");
      }
    }

    for (const column of writtenColumns(rule)) {
      const owner = writers.get(column);
      if (owner) {
        conflicts.push(`열 '${column}'에 규칙 '${owner}'와 '${rule.id}'가 모두 값을 씁니다.`);
      } else {
        writers.set(column, rule.id);
      }
    }
    for (const [source, target] of dependencyEdges(rule)) {
      if (!graph.has(source)) graph.set(source, new Set());
      graph.get(source)?.add(target);
    }
  }

  const visiting = new Set<string>();
  const visited = new Set<string>();
  const hasCycle = (node: string): boolean => {
    if (visiting.has(node)) return true;
    if (visited.has(node)) return false;
    visiting.add(node);
    for (const neighbor of graph.get(node) ?? []) {
      if (hasCycle(neighbor)) return true;
    }
    visiting.delete(node);
    visited.add(node);
    return false;
  };
  if ([...graph.keys()].some((node) => hasCycle(node))) {
    conflicts.push("규칙의 읽기/쓰기 그래프에 순환이 있어 재구성 순서를 정할 수 없습니다.");
  }
  return [...new Set(conflicts)];
}
