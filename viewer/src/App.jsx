import {
  startTransition,
  useDeferredValue,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import {
  createColumnHelper,
  flexRender,
  getCoreRowModel,
  getSortedRowModel,
  useReactTable,
} from "@tanstack/react-table";
import { useVirtualizer } from "@tanstack/react-virtual";

const DATA_PATH = "/data/ine_dirce_workflows_enriched_top20.json";
const DESKTOP_TABLE_ROW_HEIGHT = 72;
const MOBILE_TABLE_ROW_HEIGHT = 92;
const MOBILE_BREAKPOINT = 720;
const MOBILE_ANCHOR_COLUMNS = ["task_name", "priority_score"];
const MODAL_TASK_PARAM = "task";
const MODAL_ACTIVITY_PARAM = "activity";

const columnHelper = createColumnHelper();

const COLUMN_LABELS = {
  sector: "Sector",
  subsector: "Subsector",
  estimated_employees_2024: "Empleo estimado 2024",
  task_name: "Tarea",
  verdict: "Veredicto",
  priority_score: "Priority score",
  ai_feasibility_score: "AI feasibility",
  business_opportunity_score: "Business opportunity",
  recommended_business_model: "Modelo negocio",
  recommended_horizon: "Horizonte",
  relative_priority: "Prioridad relativa",
  total_2024: "Total 2024",
  size_micro_pct: "Micro %",
  size_small_pct: "Small %",
  size_medium_pct: "Medium %",
  size_large_pct: "Large %",
  sector_context: "Contexto sectorial",
  recommended_solution_type: "Tipo solución",
  person_actions: "Acciones humanas",
  typical_input: "Input típico",
  typical_output: "Output típico",
  frequency: "Frecuencia",
  process_variability: "Variabilidad proceso",
  common_exceptions: "Excepciones",
  executing_role: "Rol ejecutor",
  typical_seniority: "Seniority",
  non_human_costs: "Costes no humanos",
  economic_assumptions: "Supuestos económicos",
  business_model_reason: "Razón modelo negocio",
  legal_regulatory_risks: "Riesgos legales",
  reputational_risks: "Riesgos reputacionales",
  precision_risks: "Riesgos de precisión",
  integration_bottlenecks: "Cuellos integración",
  misleading_attractiveness: "Falsa atractividad",
  Estimated_Employees_2024: "Empleados 2024",
  Estimated_Employees_pct: "Peso empleo %",
  Median_YoY_Growth_pct: "Mediana YoY %",
  Growth_2020_2024_pct: "Crec. 2020-2024 %",
  Total_2024: "Total 2024",
  Total_2023: "Total 2023",
  Total_2022: "Total 2022",
  Total_2021: "Total 2021",
  Total_2020: "Total 2020",
};

const GROUP_LABELS = {
  context: "Contexto",
  scores: "Scores",
  workflow: "Workflow",
  automation: "Automatización",
  economics: "Economía",
  business: "Negocio",
  risks: "Riesgos",
  dirceSummary: "DIRCE resumen",
  dirceSize: "DIRCE tamaño",
  dirceStrata: "DIRCE estratos",
  dirceCondition: "DIRCE sociedad",
  other: "Otros",
};

const GROUP_ORDER = [
  "context",
  "scores",
  "workflow",
  "automation",
  "economics",
  "business",
  "risks",
  "dirceSummary",
  "dirceSize",
  "dirceStrata",
  "dirceCondition",
  "other",
];

const CONTEXT_KEYS = [
  "task_name",
  "subsector",
  "sector",
  "verdict",
  "relative_priority",
  "recommended_solution_type",
  "recommended_business_model",
  "recommended_horizon",
];

const SCORE_KEYS = [
  "priority_score",
  "ai_feasibility_score",
  "business_opportunity_score",
];

const WORKFLOW_KEYS = [
  "sector_context",
  "person_actions",
  "typical_input",
  "typical_output",
  "frequency",
  "process_variability",
  "common_exceptions",
  "executing_role",
  "typical_seniority",
];

const ECONOMIC_RANGE_SPECS = [
  {
    id: "annual_gross_salary_eur",
    label: "Salario bruto anual",
    minKey: "annual_gross_salary_eur_min",
    maxKey: "annual_gross_salary_eur_max",
  },
  {
    id: "company_cost_eur",
    label: "Coste empresa anual",
    minKey: "company_cost_eur_min",
    maxKey: "company_cost_eur_max",
  },
  {
    id: "monthly_human_cost_eur",
    label: "Coste humano mensual",
    minKey: "monthly_human_cost_eur_min",
    maxKey: "monthly_human_cost_eur_max",
  },
];

const ECONOMIC_SIMPLE_KEYS = [
  "estimated_employees_2024",
  "total_2024",
  "size_micro_pct",
  "size_small_pct",
  "size_medium_pct",
  "size_large_pct",
  "avg_time_per_task_minutes",
  "monthly_volume_per_company",
  "non_human_costs",
  "economic_assumptions",
];

const RISK_KEYS = [
  "legal_regulatory_risks",
  "reputational_risks",
  "precision_risks",
  "integration_bottlenecks",
  "misleading_attractiveness",
];

const DIRCE_SUMMARY_KEYS = [
  "Estimated_Employees_2024",
  "Estimated_Employees_pct",
  "Median_YoY_Growth_pct",
  "Growth_2020_2024_pct",
  "Total_2024",
  "Total_2023",
  "Total_2022",
  "Total_2021",
  "Total_2020",
];

const AUTOMATION_SCORE_SPECS = [
  { id: "verifiability", label: "Verificabilidad" },
  { id: "verification_latency", label: "Latencia verificación" },
  { id: "input_standardization", label: "Estandarización input" },
  { id: "output_standardization", label: "Estandarización output" },
  { id: "error_tolerance", label: "Tolerancia al error" },
  { id: "integration_ease", label: "Facilidad integración" },
  { id: "autonomy", label: "Autonomía" },
];

const BUSINESS_SCORE_SPECS = [
  { id: "pain_size", label: "Dolor del problema" },
  { id: "recurrence", label: "Recurrencia" },
  { id: "buyer_budget", label: "Presupuesto comprador" },
  { id: "roi_clarity", label: "Claridad ROI" },
  { id: "adoption_ease", label: "Facilidad adopción" },
  { id: "productization_ease", label: "Facilidad productizar" },
  { id: "service_component_need", label: "Necesidad servicio" },
];

const BUSINESS_SIMPLE_KEYS = ["business_model_reason"];
const HERO_BADGE_KEYS = [
  "verdict",
  "relative_priority",
  "recommended_solution_type",
  "recommended_business_model",
  "recommended_horizon",
];
const HERO_METRIC_KEYS = [
  "priority_score",
  "ai_feasibility_score",
  "business_opportunity_score",
  "monthly_volume_per_company",
  "avg_time_per_task_minutes",
  "estimated_employees_2024",
];

const KNOWN_COLUMN_ORDER = [
  ...CONTEXT_KEYS,
  ...SCORE_KEYS,
  ...WORKFLOW_KEYS,
  ...AUTOMATION_SCORE_SPECS.flatMap((spec) => [
    `${spec.id}_score`,
    `${spec.id}_reason`,
  ]),
  ...ECONOMIC_RANGE_SPECS.flatMap((spec) => [spec.minKey, spec.maxKey]),
  ...ECONOMIC_SIMPLE_KEYS,
  ...BUSINESS_SCORE_SPECS.flatMap((spec) => [
    `${spec.id}_score`,
    `${spec.id}_reason`,
  ]),
  ...BUSINESS_SIMPLE_KEYS,
  ...RISK_KEYS,
  ...DIRCE_SUMMARY_KEYS,
];
const FALLBACK_SCORE_METADATA = {
  normalization: {
    inputScale: "1-5",
    outputScale: "0-100",
    formula: "((score - 1) / 4) * 100",
    inverseFormula: "(1 - ((score - 1) / 4)) * 100",
  },
  scores: {
    ai_feasibility_score: {
      label: "AI Feasibility Score",
      formula: "Suma ponderada de 7 dimensiones de automatización",
      components: [
        { field: "verifiability_score", label: "Verificabilidad", weight: 0.3 },
        {
          field: "verification_latency_score",
          label: "Latencia de verificación",
          weight: 0.2,
        },
        {
          field: "input_standardization_score",
          label: "Estandarización de inputs",
          weight: 0.1,
        },
        {
          field: "output_standardization_score",
          label: "Estandarización de outputs",
          weight: 0.1,
        },
        { field: "error_tolerance_score", label: "Tolerancia al error", weight: 0.15 },
        {
          field: "integration_ease_score",
          label: "Facilidad de integración",
          weight: 0.1,
        },
        { field: "autonomy_score", label: "Autonomía", weight: 0.05 },
      ],
    },
    business_opportunity_score: {
      label: "Business Opportunity Score",
      formula: "Suma ponderada de 6 dimensiones de oportunidad",
      components: [
        { field: "pain_size_score", label: "Dolor económico", weight: 0.25 },
        { field: "recurrence_score", label: "Recurrencia", weight: 0.2 },
        { field: "roi_clarity_score", label: "Claridad del ROI", weight: 0.2 },
        { field: "adoption_ease_score", label: "Facilidad de adopción", weight: 0.15 },
        {
          field: "productization_ease_score",
          label: "Facilidad de productizar",
          weight: 0.1,
        },
        { field: "buyer_budget_score", label: "Presupuesto del comprador", weight: 0.1 },
      ],
      excluded: [
        {
          field: "service_component_need_score",
          label: "Necesidad de componente de servicio",
          reason:
            "No entra en Business Opportunity; se usa solo como ajuste de priorización/product scaling fit.",
        },
      ],
    },
    priority_score: {
      label: "Priority Score",
      formula:
        "Suma ponderada directa de 14 scores: fiabilidad de automatización 50%, valor de negocio 32%, encaje producto-escalabilidad 18%",
      groups: [
        {
          id: "automation_reliability",
          label: "Fiabilidad de automatización",
          weight: 0.5,
        },
        { id: "business_value", label: "Valor de negocio", weight: 0.32 },
        {
          id: "product_scaling_fit",
          label: "Encaje producto-escalabilidad",
          weight: 0.18,
        },
      ],
      components: [
        {
          field: "verifiability_score",
          label: "Verificabilidad",
          weight: 0.14,
          group: "automation_reliability",
        },
        {
          field: "verification_latency_score",
          label: "Latencia de verificación",
          weight: 0.08,
          group: "automation_reliability",
        },
        {
          field: "error_tolerance_score",
          label: "Tolerancia al error",
          weight: 0.08,
          group: "automation_reliability",
        },
        {
          field: "integration_ease_score",
          label: "Facilidad de integración",
          weight: 0.07,
          group: "automation_reliability",
        },
        {
          field: "autonomy_score",
          label: "Autonomía",
          weight: 0.05,
          group: "automation_reliability",
        },
        {
          field: "input_standardization_score",
          label: "Estandarización de inputs",
          weight: 0.04,
          group: "automation_reliability",
        },
        {
          field: "output_standardization_score",
          label: "Estandarización de outputs",
          weight: 0.04,
          group: "automation_reliability",
        },
        {
          field: "pain_size_score",
          label: "Dolor económico",
          weight: 0.1,
          group: "business_value",
        },
        {
          field: "recurrence_score",
          label: "Recurrencia",
          weight: 0.08,
          group: "business_value",
        },
        {
          field: "roi_clarity_score",
          label: "Claridad del ROI",
          weight: 0.1,
          group: "business_value",
        },
        {
          field: "buyer_budget_score",
          label: "Presupuesto del comprador",
          weight: 0.04,
          group: "business_value",
        },
        {
          field: "adoption_ease_score",
          label: "Facilidad de adopción",
          weight: 0.06,
          group: "product_scaling_fit",
        },
        {
          field: "productization_ease_score",
          label: "Facilidad de productizar",
          weight: 0.07,
          group: "product_scaling_fit",
        },
        {
          field: "service_component_need_score",
          label: "Necesidad de componente de servicio",
          weight: 0.05,
          group: "product_scaling_fit",
          invert: true,
          note: "Se invierte: menos necesidad de servicio implica más prioridad.",
        },
      ],
      thresholds: [
        { min: 80, relative_priority: "muy_alta", recommended_horizon: "ya" },
        { min: 65, relative_priority: "alta", recommended_horizon: "12_meses" },
        { min: 50, relative_priority: "media", recommended_horizon: "24_meses" },
        { min: 0, relative_priority: "baja", recommended_horizon: "no_priorizar" },
      ],
    },
  },
};

function normalizeText(value) {
  return String(value ?? "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase();
}

function normalizeQuery(value) {
  return normalizeText(value).trim();
}

function readModalParamsFromUrl() {
  if (typeof window === "undefined") {
    return { task: "", activity: "" };
  }

  const params = new URLSearchParams(window.location.search);
  return {
    task: params.get(MODAL_TASK_PARAM) ?? "",
    activity: params.get(MODAL_ACTIVITY_PARAM) ?? "",
  };
}

function updateModalUrl(row, method = "push") {
  if (typeof window === "undefined") {
    return;
  }

  const url = new URL(window.location.href);

  if (row?.task_name) {
    url.searchParams.set(MODAL_TASK_PARAM, row.task_name);
    if (row.subsector) {
      url.searchParams.set(MODAL_ACTIVITY_PARAM, row.subsector);
    } else {
      url.searchParams.delete(MODAL_ACTIVITY_PARAM);
    }
  } else {
    url.searchParams.delete(MODAL_TASK_PARAM);
    url.searchParams.delete(MODAL_ACTIVITY_PARAM);
  }

  const nextUrl = `${url.pathname}${url.search}${url.hash}`;
  if (method === "replace") {
    window.history.replaceState(null, "", nextUrl);
    return;
  }

  window.history.pushState(null, "", nextUrl);
}

function findRowFromModalParams(rows, modalParams) {
  if (!modalParams?.task) {
    return null;
  }

  const normalizedTask = normalizeText(modalParams.task);
  const normalizedActivity = normalizeText(modalParams.activity);
  const matchingTaskRows = rows.filter(
    (row) => normalizeText(row.task_name) === normalizedTask,
  );

  if (!matchingTaskRows.length) {
    return null;
  }

  if (!normalizedActivity) {
    return matchingTaskRows[0];
  }

  return (
    matchingTaskRows.find(
      (row) => normalizeText(row.subsector) === normalizedActivity,
    ) ?? matchingTaskRows[0]
  );
}

function useIsNarrowViewport(maxWidth = MOBILE_BREAKPOINT) {
  const query = `(max-width: ${maxWidth}px)`;
  const getMatches = () =>
    typeof window !== "undefined" ? window.matchMedia(query).matches : false;

  const [matches, setMatches] = useState(getMatches);

  useEffect(() => {
    if (typeof window === "undefined") {
      return undefined;
    }

    const mediaQuery = window.matchMedia(query);
    const handleChange = (event) => setMatches(event.matches);
    setMatches(mediaQuery.matches);

    if (typeof mediaQuery.addEventListener === "function") {
      mediaQuery.addEventListener("change", handleChange);
      return () => mediaQuery.removeEventListener("change", handleChange);
    }

    mediaQuery.addListener(handleChange);
    return () => mediaQuery.removeListener(handleChange);
  }, [query]);

  return matches;
}

function matchesQuery(normalizedQuery, ...values) {
  if (!normalizedQuery) {
    return true;
  }

  return values.some((value) => normalizeText(value).includes(normalizedQuery));
}

function humanizeFieldName(key) {
  if (COLUMN_LABELS[key]) {
    return COLUMN_LABELS[key];
  }

  return key
    .replaceAll("_", " ")
    .replace(/\bai\b/gi, "AI")
    .replace(/\broi\b/gi, "ROI")
    .replace(/\beur\b/gi, "EUR")
    .replace(/\bpct\b/gi, "%")
    .replace(/\bpr\b/gi, "PR")
    .replace(/\bllm\b/gi, "LLM");
}

function formatWeight(weight) {
  return `${Math.round(weight * 100)}%`;
}

function formatPriorityLabel(value) {
  return String(value ?? "")
    .replaceAll("_", " ")
    .replace(/\b\w/g, (character) => character.toUpperCase());
}

function roundScore(value) {
  return Math.round(Number(value) * 10) / 10;
}

function formatScoreNumber(value, { minimumFractionDigits = 0, maximumFractionDigits = 1 } = {}) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "—";
  }

  return new Intl.NumberFormat("es-ES", {
    minimumFractionDigits,
    maximumFractionDigits,
  }).format(numeric);
}

function normalizePriorityComponent(rawScore, invert = false) {
  const numeric = Number(rawScore);
  if (!Number.isFinite(numeric)) {
    return null;
  }

  const normalized = ((numeric - 1) / 4) * 100;
  return roundScore(invert ? 100 - normalized : normalized);
}

function getPriorityThreshold(breakdown) {
  if (!breakdown?.thresholds?.length) {
    return null;
  }

  return (
    breakdown.thresholds.find((threshold) => breakdown.total >= threshold.min) ??
    breakdown.thresholds[breakdown.thresholds.length - 1]
  );
}

function buildPriorityBreakdown(row, scoreMetadata) {
  if (row?.__priority_breakdown?.groups?.length) {
    return row.__priority_breakdown;
  }

  const priorityInfo = scoreMetadata?.scores?.priority_score;
  if (!row || !priorityInfo?.components?.length) {
    return null;
  }

  const groupsById = new Map(
    (priorityInfo.groups ?? []).map((group) => [
      group.id,
      { ...group, contribution: 0, normalized_score: null, components: [] },
    ]),
  );

  const components = [];
  for (const component of priorityInfo.components) {
    const rawScore = Number(row[component.field]);
    const normalizedScore = normalizePriorityComponent(rawScore, component.invert);
    if (!Number.isFinite(rawScore) || normalizedScore === null) {
      continue;
    }

    const contribution = roundScore(normalizedScore * component.weight);
    const detail = {
      ...component,
      raw_score: roundScore(rawScore),
      normalized_score: normalizedScore,
      contribution,
    };
    components.push(detail);

    if (!groupsById.has(component.group)) {
      groupsById.set(component.group, {
        id: component.group,
        label: component.group,
        weight: 0,
        contribution: 0,
        normalized_score: null,
        components: [],
      });
    }

    const group = groupsById.get(component.group);
    group.contribution = roundScore(group.contribution + contribution);
    group.components.push(detail);
  }

  const groups = [...groupsById.values()].map((group) => ({
    ...group,
    normalized_score: group.weight
      ? roundScore(group.contribution / group.weight)
      : null,
  }));

  return {
    total: roundScore(components.reduce((sum, component) => sum + component.contribution, 0)),
    groups,
    components,
    thresholds: priorityInfo.thresholds ?? [],
  };
}

function buildPriorityTooltip(row, scoreMetadata) {
  const breakdown = buildPriorityBreakdown(row, scoreMetadata);
  if (!breakdown) {
    return formatValue("priority_score", row?.priority_score);
  }

  const threshold = getPriorityThreshold(breakdown);
  const lines = [`Priority Score: ${formatScoreNumber(breakdown.total)}`];

  if (threshold) {
    lines.push(
      `Tramo actual: ${threshold.relative_priority} / ${threshold.recommended_horizon}`,
    );
  }

  if (breakdown.groups?.length) {
    lines.push("Grupos:");
    for (const group of breakdown.groups) {
      lines.push(
        `- ${group.label}: ${formatScoreNumber(group.contribution)} pts | grupo ${formatScoreNumber(
          group.normalized_score,
        )}/100 | peso ${formatWeight(group.weight)}`,
      );
    }
  }

  if (breakdown.components?.length) {
    lines.push("Componentes:");
    for (const component of breakdown.components) {
      const inversionNote = component.invert ? " | invertido" : "";
      lines.push(
        `- ${component.label}: raw ${formatScoreNumber(component.raw_score, {
          maximumFractionDigits: 2,
        })} | norm ${formatScoreNumber(component.normalized_score)}/100 | peso ${formatWeight(
          component.weight,
        )} | contrib ${formatScoreNumber(component.contribution)}${inversionNote}`,
      );
    }
  }

  return lines.join("\n");
}

function getScoreTooltip(columnId, scoreMetadata) {
  const scoreInfo = scoreMetadata?.scores?.[columnId];
  if (!scoreInfo) {
    return humanizeFieldName(columnId);
  }

  const lines = [scoreInfo.label];

  if (scoreMetadata?.normalization?.formula && columnId !== "priority_score") {
    lines.push(`Normalización: ${scoreMetadata.normalization.formula}`);
  }

  if (scoreInfo.formula) {
    lines.push(scoreInfo.formula);
  }

  if (scoreInfo.groups?.length) {
    lines.push("Bloques:");
    for (const group of scoreInfo.groups) {
      lines.push(`- ${group.label}: ${formatWeight(group.weight)}`);
    }
  }

  if (scoreInfo.components?.length) {
    lines.push("Pesos:");
    for (const component of scoreInfo.components) {
      const parts = [`- ${component.label}: ${formatWeight(component.weight)}`];
      if (component.group) {
        const groupLabel =
          scoreInfo.groups?.find((group) => group.id === component.group)?.label ??
          component.group;
        parts.push(`grupo ${groupLabel}`);
      }
      if (component.invert && scoreMetadata?.normalization?.inverseFormula) {
        parts.push(`inversión ${scoreMetadata.normalization.inverseFormula}`);
      }
      if (component.note) {
        parts.push(component.note);
      }
      lines.push(parts.join(" | "));
    }
  }

  if (scoreInfo.excluded?.length) {
    lines.push("No incluidos:");
    for (const excluded of scoreInfo.excluded) {
      lines.push(`- ${excluded.label}: ${excluded.reason}`);
    }
  }

  if (scoreInfo.thresholds?.length) {
    lines.push("Tramos:");
    for (const threshold of scoreInfo.thresholds) {
      lines.push(
        `- ≥ ${threshold.min}: ${threshold.relative_priority} / ${threshold.recommended_horizon}`,
      );
    }
  }

  return lines.join("\n");
}

function formatValue(columnId, value) {
  if (value === null || value === undefined || value === "") {
    return "—";
  }

  if (typeof value === "string") {
    return value;
  }

  const key = String(columnId).toLowerCase();
  const isCurrency = key.includes("eur");
  const isPercent =
    key.endsWith("_pct") ||
    key.includes("growth_pct") ||
    key.includes("size_") && key.endsWith("_pct");
  const isIntegerLike = Math.abs(value - Math.round(value)) < 1e-6;

  const formatter = new Intl.NumberFormat("es-ES", {
    minimumFractionDigits: isPercent && !isIntegerLike ? 1 : 0,
    maximumFractionDigits: isPercent ? 2 : isIntegerLike ? 0 : 2,
  });

  const formatted = formatter.format(value);

  if (isCurrency) {
    return `${formatted} €`;
  }

  if (isPercent) {
    return `${formatted} %`;
  }

  if (key.includes("minutes")) {
    return `${formatted} min`;
  }

  return formatted;
}

function getCellTitle(columnId, row, scoreMetadata) {
  if (columnId === "priority_score") {
    return buildPriorityTooltip(row, scoreMetadata);
  }

  return formatValue(columnId, row?.[columnId]);
}

function getSemanticGroup(columnId) {
  if (CONTEXT_KEYS.includes(columnId)) {
    return "context";
  }

  if (SCORE_KEYS.includes(columnId)) {
    return "scores";
  }

  if (WORKFLOW_KEYS.includes(columnId)) {
    return "workflow";
  }

  if (
    AUTOMATION_SCORE_SPECS.some(
      (spec) =>
        columnId === `${spec.id}_score` || columnId === `${spec.id}_reason`,
    )
  ) {
    return "automation";
  }

  if (
    ECONOMIC_SIMPLE_KEYS.includes(columnId) ||
    ECONOMIC_RANGE_SPECS.some(
      (spec) => columnId === spec.minKey || columnId === spec.maxKey,
    )
  ) {
    return "economics";
  }

  if (
    BUSINESS_SIMPLE_KEYS.includes(columnId) ||
    BUSINESS_SCORE_SPECS.some(
      (spec) =>
        columnId === `${spec.id}_score` || columnId === `${spec.id}_reason`,
    )
  ) {
    return "business";
  }

  if (RISK_KEYS.includes(columnId)) {
    return "risks";
  }

  if (DIRCE_SUMMARY_KEYS.includes(columnId) || columnId.startsWith("Total_")) {
    return "dirceSummary";
  }

  if (columnId.startsWith("Size_")) {
    return "dirceSize";
  }

  if (columnId.startsWith("Estrato_")) {
    return "dirceStrata";
  }

  if (columnId.startsWith("Condicion_")) {
    return "dirceCondition";
  }

  return "other";
}

function getMobileAnchorPriority(columnId) {
  const index = MOBILE_ANCHOR_COLUMNS.indexOf(columnId);
  return index >= 0 ? index : null;
}

function getColumnPriority(columnId) {
  const explicitIndex = KNOWN_COLUMN_ORDER.indexOf(columnId);
  if (explicitIndex >= 0) {
    return explicitIndex;
  }

  return KNOWN_COLUMN_ORDER.length + 1000;
}

function getNumericColumn(columnType) {
  return columnType.includes("int") || columnType.includes("float");
}

function getColumnWidth(columnId, isMobile = false) {
  if (isMobile) {
    if (columnId === "task_name") {
      return 248;
    }

    if (columnId === "priority_score") {
      return 116;
    }

    if (columnId === "sector" || columnId === "subsector") {
      return 220;
    }

    if (columnId.endsWith("_reason") || RISK_KEYS.includes(columnId)) {
      return 240;
    }

    if (columnId.includes("score")) {
      return 124;
    }

    if (
      columnId.includes("cost") ||
      columnId.includes("salary") ||
      columnId.includes("employees") ||
      columnId.startsWith("Total_")
    ) {
      return 128;
    }

    return 170;
  }

  if (columnId === "task_name") {
    return 320;
  }

  if (columnId === "sector" || columnId === "subsector") {
    return 280;
  }

  if (columnId.endsWith("_reason") || RISK_KEYS.includes(columnId)) {
    return 320;
  }

  if (
    columnId.includes("score") ||
    columnId.includes("cost") ||
    columnId.includes("salary") ||
    columnId.includes("employees") ||
    columnId.startsWith("Total_")
  ) {
    return 150;
  }

  return 220;
}

function sortColumnMeta(columnMeta, isMobile = false) {
  return [...columnMeta].sort((left, right) => {
    if (isMobile) {
      const leftAnchor = getMobileAnchorPriority(left.id);
      const rightAnchor = getMobileAnchorPriority(right.id);

      if (leftAnchor !== null || rightAnchor !== null) {
        if (leftAnchor === null) {
          return 1;
        }

        if (rightAnchor === null) {
          return -1;
        }

        if (leftAnchor !== rightAnchor) {
          return leftAnchor - rightAnchor;
        }
      }
    }

    const groupDiff =
      GROUP_ORDER.indexOf(getSemanticGroup(left.id)) -
      GROUP_ORDER.indexOf(getSemanticGroup(right.id));
    if (groupDiff !== 0) {
      return groupDiff;
    }

    const priorityDiff = getColumnPriority(left.id) - getColumnPriority(right.id);
    if (priorityDiff !== 0) {
      return priorityDiff;
    }

    return humanizeFieldName(left.id).localeCompare(humanizeFieldName(right.id), "es");
  });
}

function renderCellContent(columnId, info, isMobile) {
  const value = formatValue(columnId, info.getValue());

  if (columnId === "priority_score") {
    return (
      <div
        className={`priority-chip priority-chip--${
          info.row.original.relative_priority ?? "default"
        } ${isMobile ? "priority-chip--compact" : ""}`}
      >
        <strong>{value}</strong>
        <span>{formatPriorityLabel(info.row.original.relative_priority)}</span>
      </div>
    );
  }

  if (columnId === "task_name" && isMobile) {
    return (
      <div className="task-cell task-cell--mobile">
        <strong>{value}</strong>
        <span>{formatValue("subsector", info.row.original.subsector)}</span>
      </div>
    );
  }

  return value;
}

function buildColumns(columnMeta, scoreMetadata, isMobile = false) {
  const taskColumnWidth = getColumnWidth("task_name", isMobile);

  return sortColumnMeta(columnMeta, isMobile).map((meta) => {
    const group = getSemanticGroup(meta.id);
    const numeric = getNumericColumn(meta.type);
    const scoreTooltip = getScoreTooltip(meta.id, scoreMetadata);
    const hasScoreInfo = Boolean(scoreMetadata?.scores?.[meta.id]);
    const width = getColumnWidth(meta.id, isMobile);
    const sticky =
      meta.id === "task_name" || (isMobile && meta.id === "priority_score");
    const stickyLeft =
      meta.id === "task_name"
        ? 0
        : isMobile && meta.id === "priority_score"
          ? taskColumnWidth
          : undefined;

    return columnHelper.accessor(meta.id, {
      id: meta.id,
      header: () => (
        <div className="header-content" title={scoreTooltip}>
          <span className={`group-pill group-pill--${group}`}>{GROUP_LABELS[group]}</span>
          <span className="header-label-row">
            <span>{humanizeFieldName(meta.id)}</span>
            {hasScoreInfo ? (
              <span
                className="header-info"
                aria-label={`Explicación de ${humanizeFieldName(meta.id)}`}
                title={scoreTooltip}
              >
                i
              </span>
            ) : null}
          </span>
        </div>
      ),
      cell: (info) => renderCellContent(meta.id, info, isMobile),
      meta: {
        group,
        numeric,
        sticky,
        stickyLeft,
        stickyEdge: isMobile && meta.id === "priority_score",
        width,
      },
    });
  });
}

function prepareRows(rows) {
  return rows.map((row, index) => ({
    ...row,
    __id: `${row.subsector ?? ""}::${row.task_name ?? ""}::${index}`,
    __search: normalizeText(
      [
        row.sector,
        row.subsector,
        row.task_name,
        row.verdict,
        row.relative_priority,
        row.recommended_solution_type,
      ].join(" "),
    ),
  }));
}

function filterRows(rows, normalizedQuery) {
  if (!normalizedQuery) {
    return rows;
  }

  return rows.filter((row) => row.__search.includes(normalizedQuery));
}

function buildSimpleEntries(source, keys, normalizedQuery) {
  return keys
    .filter((key) => key in source)
    .map((key) => ({
      id: key,
      label: humanizeFieldName(key),
      value: formatValue(key, source[key]),
      rawValue: source[key],
      kind:
        typeof source[key] === "number"
          ? "metric"
          : typeof source[key] === "string" && source[key].length > 96
            ? "narrative"
            : "short",
      searchValues: [key, humanizeFieldName(key), source[key]],
    }))
    .filter((entry) => matchesQuery(normalizedQuery, ...entry.searchValues));
}

function buildRangeEntries(source, specs, normalizedQuery) {
  return specs
    .filter((spec) => spec.minKey in source || spec.maxKey in source)
    .map((spec) => ({
      id: spec.id,
      label: spec.label,
      value: `${formatValue(spec.minKey, source[spec.minKey])} → ${formatValue(
        spec.maxKey,
        source[spec.maxKey],
      )}`,
      searchValues: [spec.label, spec.minKey, spec.maxKey, source[spec.minKey], source[spec.maxKey]],
    }))
    .filter((entry) => matchesQuery(normalizedQuery, ...entry.searchValues));
}

function buildScoreReasonEntries(source, specs, normalizedQuery) {
  return specs
    .filter(
      (spec) =>
        `${spec.id}_score` in source || `${spec.id}_reason` in source,
    )
    .map((spec) => ({
      id: spec.id,
      label: spec.label,
      score: formatValue(`${spec.id}_score`, source[`${spec.id}_score`]),
      note: formatValue(`${spec.id}_reason`, source[`${spec.id}_reason`]),
      searchValues: [
        spec.label,
        `${spec.id}_score`,
        `${spec.id}_reason`,
        source[`${spec.id}_score`],
        source[`${spec.id}_reason`],
      ],
    }))
    .filter((entry) => matchesQuery(normalizedQuery, ...entry.searchValues));
}

function buildPairedMetrics(source, prefix, normalizedQuery) {
  return Object.keys(source)
    .filter((key) => key.startsWith(prefix) && key.endsWith("_abs"))
    .map((absKey) => {
      const baseKey = absKey.slice(0, -4);
      const pctKey = `${baseKey}_pct`;
      return {
        id: baseKey,
        label: humanizeFieldName(absKey).replace(" (abs)", ""),
        absolute: formatValue(absKey, source[absKey]),
        percentage: formatValue(pctKey, source[pctKey]),
        searchValues: [absKey, pctKey, humanizeFieldName(absKey), source[absKey], source[pctKey]],
      };
    })
    .filter((entry) => matchesQuery(normalizedQuery, ...entry.searchValues));
}

function getUnhandledKeys(source, excludedKeys) {
  return Object.keys(source).filter(
    (key) =>
      !key.startsWith("__") &&
      !excludedKeys.has(key),
  );
}

function DetailSection({ title, children }) {
  return (
    <section className="detail-section">
      <div className="detail-section__header">
        <h3>{title}</h3>
      </div>
      {children}
    </section>
  );
}

function partitionEntries(entries) {
  const compact = [];
  const narrative = [];

  for (const entry of entries) {
    if (entry.kind === "narrative") {
      narrative.push(entry);
    } else {
      compact.push(entry);
    }
  }

  return { compact, narrative };
}

function BadgeStrip({ entries }) {
  return (
    <div className="badge-strip">
      {entries.map((entry) => (
        <article key={entry.id} className="badge-card">
          <span>{entry.label}</span>
          <strong>{entry.value}</strong>
        </article>
      ))}
    </div>
  );
}

function FieldGrid({ entries, compact = false }) {
  return (
    <div className={`field-grid ${compact ? "field-grid--compact" : ""}`}>
      {entries.map((entry) => (
        <article
          key={entry.id}
          className={`field-card field-card--${entry.kind}`}
        >
          <span>{entry.label}</span>
          <strong>{entry.value}</strong>
        </article>
      ))}
    </div>
  );
}

function NarrativeList({ entries }) {
  return (
    <div className="narrative-list">
      {entries.map((entry) => (
        <article key={entry.id} className="narrative-card">
          <span>{entry.label}</span>
          <p>{entry.value}</p>
        </article>
      ))}
    </div>
  );
}

function FieldCluster({ entries, compact = false }) {
  const { compact: compactEntries, narrative } = partitionEntries(entries);

  return (
    <>
      {compactEntries.length ? <FieldGrid entries={compactEntries} compact={compact} /> : null}
      {narrative.length ? <NarrativeList entries={narrative} /> : null}
    </>
  );
}

function ScoreReasonGrid({ entries }) {
  return (
    <div className="score-grid">
      {entries.map((entry) => (
        <article key={entry.id} className="score-card">
          <div className="score-card__top">
            <span>{entry.label}</span>
            <strong>{entry.score}</strong>
          </div>
          <p>{entry.note}</p>
        </article>
      ))}
    </div>
  );
}

function PriorityBreakdownPanel({ breakdown }) {
  if (!breakdown?.groups?.length) {
    return null;
  }

  const threshold = getPriorityThreshold(breakdown);

  return (
    <div className="score-breakdown">
      <div className="score-breakdown__summary">
        <article className="score-breakdown__summary-card">
          <span>Priority actual</span>
          <strong>{formatScoreNumber(breakdown.total, { minimumFractionDigits: 1 })}</strong>
        </article>
        {threshold ? (
          <article className="score-breakdown__summary-card">
            <span>Tramo</span>
            <strong>
              {threshold.relative_priority} / {threshold.recommended_horizon}
            </strong>
          </article>
        ) : null}
      </div>

      <div className="score-breakdown__groups">
        {breakdown.groups.map((group) => (
          <article key={group.id} className="score-breakdown__group">
            <header className="score-breakdown__group-head">
              <div>
                <span>{group.label}</span>
                <strong>{formatWeight(group.weight)}</strong>
              </div>
              <div className="score-breakdown__group-metrics">
                <strong>{formatScoreNumber(group.contribution)} pts</strong>
                <span>{formatScoreNumber(group.normalized_score)}/100</span>
              </div>
            </header>

            <div className="score-breakdown__rows">
              {group.components.map((component) => (
                <article key={component.field} className="score-breakdown__row">
                  <div className="score-breakdown__row-main">
                    <div className="score-breakdown__row-label">
                      <span>{component.label}</span>
                      {component.invert ? <em>invertido</em> : null}
                    </div>
                    <strong>+{formatScoreNumber(component.contribution)}</strong>
                  </div>
                  <div className="score-breakdown__row-meta">
                    <code>
                      {formatScoreNumber(component.raw_score, { maximumFractionDigits: 2 })} →{" "}
                      {formatScoreNumber(component.normalized_score)}/100
                    </code>
                    <code>{formatWeight(component.weight)}</code>
                  </div>
                </article>
              ))}
            </div>
          </article>
        ))}
      </div>
    </div>
  );
}

function ScoreMethodPanel({ scoreMetadata, row }) {
  if (!scoreMetadata?.scores) {
    return null;
  }

  const priorityInfo = scoreMetadata.scores.priority_score;
  const aiInfo = scoreMetadata.scores.ai_feasibility_score;
  const bizInfo = scoreMetadata.scores.business_opportunity_score;
  const priorityBreakdown = buildPriorityBreakdown(row, scoreMetadata);

  return (
    <div className="score-explainer">
      <p className="score-explainer__lead">{priorityInfo.formula}</p>
      <div className="score-explainer__meta">
        <span>Normalización directa: {scoreMetadata.normalization.formula}</span>
        {scoreMetadata.normalization.inverseFormula ? (
          <span>Inversión servicio: {scoreMetadata.normalization.inverseFormula}</span>
        ) : null}
      </div>

      {priorityBreakdown ? <PriorityBreakdownPanel breakdown={priorityBreakdown} /> : null}

      <div className="score-explainer__columns">
        <div className="score-explainer__block">
          <strong>{aiInfo.label}</strong>
          <ul className="score-explainer__list">
            {aiInfo.components.map((component) => (
              <li key={component.field}>
                <span>{component.label}</span>
                <strong>{formatWeight(component.weight)}</strong>
              </li>
            ))}
          </ul>
        </div>

        <div className="score-explainer__block">
          <strong>{bizInfo.label}</strong>
          <ul className="score-explainer__list">
            {bizInfo.components.map((component) => (
              <li key={component.field}>
                <span>{component.label}</span>
                <strong>{formatWeight(component.weight)}</strong>
              </li>
            ))}
          </ul>
          {bizInfo.excluded?.length ? (
            <p className="score-explainer__note">
              {bizInfo.excluded[0].label}: {bizInfo.excluded[0].reason}
            </p>
          ) : null}
        </div>
      </div>
      <div className="score-explainer__thresholds">
        <span>Tramos de prioridad:</span>
        {priorityInfo.thresholds?.map((threshold) => (
          <span key={threshold.min}>
            ≥ {threshold.min}: {threshold.relative_priority} / {threshold.recommended_horizon}
          </span>
        ))}
      </div>
    </div>
  );
}

function PairTable({ label, rows }) {
  if (!rows.length) {
    return null;
  }

  return (
    <DetailSection title={label}>
      <div className="pair-table">
        <div className="pair-table__head">
          <span className="pair-table__cell pair-table__cell--label">Campo</span>
          <span className="pair-table__cell pair-table__cell--value">Abs.</span>
          <span className="pair-table__cell pair-table__cell--value">%</span>
        </div>
        {rows.map((row) => (
          <div key={row.id} className="pair-table__row">
            <span className="pair-table__cell pair-table__cell--label">{row.label}</span>
            <strong className="pair-table__cell pair-table__cell--value">{row.absolute}</strong>
            <strong className="pair-table__cell pair-table__cell--value">{row.percentage}</strong>
          </div>
        ))}
      </div>
    </DetailSection>
  );
}

function DetailModal({ row, query, onQueryChange, onClose, scoreMetadata }) {
  const normalizedQuery = useMemo(() => normalizeQuery(query), [query]);
  const heroBadgeEntries = useMemo(
    () => buildSimpleEntries(row, HERO_BADGE_KEYS, normalizedQuery),
    [normalizedQuery, row],
  );
  const heroMetricEntries = useMemo(
    () => buildSimpleEntries(row, HERO_METRIC_KEYS, normalizedQuery),
    [normalizedQuery, row],
  );

  const contextEntries = useMemo(
    () =>
      buildSimpleEntries(
        row,
        CONTEXT_KEYS.filter((key) => !HERO_BADGE_KEYS.includes(key)),
        normalizedQuery,
      ),
    [normalizedQuery, row],
  );
  const scoreEntries = useMemo(
    () => buildSimpleEntries(row, SCORE_KEYS, normalizedQuery),
    [normalizedQuery, row],
  );
  const workflowEntries = useMemo(
    () => buildSimpleEntries(row, WORKFLOW_KEYS, normalizedQuery),
    [normalizedQuery, row],
  );
  const automationEntries = useMemo(
    () => buildScoreReasonEntries(row, AUTOMATION_SCORE_SPECS, normalizedQuery),
    [normalizedQuery, row],
  );
  const economicRangeEntries = useMemo(
    () => buildRangeEntries(row, ECONOMIC_RANGE_SPECS, normalizedQuery),
    [normalizedQuery, row],
  );
  const economicEntries = useMemo(
    () => buildSimpleEntries(row, ECONOMIC_SIMPLE_KEYS, normalizedQuery),
    [normalizedQuery, row],
  );
  const businessEntries = useMemo(
    () => buildScoreReasonEntries(row, BUSINESS_SCORE_SPECS, normalizedQuery),
    [normalizedQuery, row],
  );
  const businessSimpleEntries = useMemo(
    () => buildSimpleEntries(row, BUSINESS_SIMPLE_KEYS, normalizedQuery),
    [normalizedQuery, row],
  );
  const riskEntries = useMemo(
    () => buildSimpleEntries(row, RISK_KEYS, normalizedQuery),
    [normalizedQuery, row],
  );
  const dirceSummaryEntries = useMemo(
    () => buildSimpleEntries(row, DIRCE_SUMMARY_KEYS, normalizedQuery),
    [normalizedQuery, row],
  );
  const sizeEntries = useMemo(
    () => buildPairedMetrics(row, "Size_", normalizedQuery),
    [normalizedQuery, row],
  );
  const strataEntries = useMemo(
    () => buildPairedMetrics(row, "Estrato_", normalizedQuery),
    [normalizedQuery, row],
  );
  const conditionEntries = useMemo(
    () => buildPairedMetrics(row, "Condicion_", normalizedQuery),
    [normalizedQuery, row],
  );

  const excludedKeys = useMemo(
    () =>
      new Set([
        ...CONTEXT_KEYS,
        ...SCORE_KEYS,
        ...WORKFLOW_KEYS,
        ...AUTOMATION_SCORE_SPECS.flatMap((spec) => [
          `${spec.id}_score`,
          `${spec.id}_reason`,
        ]),
        ...ECONOMIC_RANGE_SPECS.flatMap((spec) => [spec.minKey, spec.maxKey]),
        ...ECONOMIC_SIMPLE_KEYS,
        ...BUSINESS_SCORE_SPECS.flatMap((spec) => [
          `${spec.id}_score`,
          `${spec.id}_reason`,
        ]),
        ...BUSINESS_SIMPLE_KEYS,
        ...RISK_KEYS,
        ...DIRCE_SUMMARY_KEYS,
        ...Object.keys(row).filter((key) => key.startsWith("Size_")),
        ...Object.keys(row).filter((key) => key.startsWith("Estrato_")),
        ...Object.keys(row).filter((key) => key.startsWith("Condicion_")),
      ]),
    [row],
  );

  const otherEntries = useMemo(
    () => buildSimpleEntries(row, getUnhandledKeys(row, excludedKeys), normalizedQuery),
    [excludedKeys, normalizedQuery, row],
  );

  const hasVisibleContent =
    contextEntries.length ||
    scoreEntries.length ||
    workflowEntries.length ||
    automationEntries.length ||
    economicRangeEntries.length ||
    economicEntries.length ||
    businessEntries.length ||
    businessSimpleEntries.length ||
    riskEntries.length ||
    dirceSummaryEntries.length ||
    sizeEntries.length ||
    strataEntries.length ||
    conditionEntries.length ||
    otherEntries.length;

  return (
    <div className="modal-backdrop" onClick={onClose}>
      <section
        className="detail-modal"
        onClick={(event) => event.stopPropagation()}
        role="dialog"
        aria-modal="true"
        aria-label="Detalle de la fila"
      >
        <header className="modal-header">
          <div className="modal-header__copy">
            <p className="eyebrow">Detalle de la fila</p>
            <h2>{row.task_name}</h2>
            <p className="detail-subtitle">
              {row.subsector}
              <br />
              {row.sector}
            </p>
          </div>
          <div className="modal-header__actions">
            <label className="search-field search-field--detail">
              <span>Buscar columna en el modal</span>
              <input
                type="search"
                value={query}
                onChange={(event) => {
                  const nextValue = event.target.value;
                  startTransition(() => onQueryChange(nextValue));
                }}
                placeholder="Ej. risks, ROI, salary, Total 2024..."
              />
            </label>
            <button type="button" className="modal-close" onClick={onClose}>
              Cerrar
            </button>
          </div>
        </header>

        <div className="modal-scroll">
          {heroBadgeEntries.length ? <BadgeStrip entries={heroBadgeEntries} /> : null}
          {heroMetricEntries.length ? <FieldGrid entries={heroMetricEntries} compact /> : null}

          <div className="modal-layout">
            <div className="modal-layout__main">
              {workflowEntries.length ? (
                <DetailSection title="Workflow actual">
                  <FieldCluster entries={workflowEntries} />
                </DetailSection>
              ) : null}

              {automationEntries.length ? (
                <DetailSection title="Automatización y verificabilidad">
                  <ScoreReasonGrid entries={automationEntries} />
                </DetailSection>
              ) : null}

              {businessEntries.length || businessSimpleEntries.length ? (
                <DetailSection title="Oportunidad de negocio">
                  {businessEntries.length ? <ScoreReasonGrid entries={businessEntries} /> : null}
                  {businessSimpleEntries.length ? (
                    <FieldCluster entries={businessSimpleEntries} compact />
                  ) : null}
                </DetailSection>
              ) : null}

              {riskEntries.length ? (
                <DetailSection title="Riesgos">
                  <FieldCluster entries={riskEntries} />
                </DetailSection>
              ) : null}

              {otherEntries.length ? (
                <DetailSection title="Otras columnas disponibles">
                  <FieldCluster entries={otherEntries} />
                </DetailSection>
              ) : null}
            </div>

            <aside className="modal-layout__side">
              {contextEntries.length ? (
                <DetailSection title="Contexto y tarea">
                  <FieldCluster entries={contextEntries} compact />
                </DetailSection>
              ) : null}

              {scoreEntries.length ? (
                <DetailSection title="Scores principales">
                  <FieldCluster entries={scoreEntries} compact />
                  <ScoreMethodPanel scoreMetadata={scoreMetadata} row={row} />
                </DetailSection>
              ) : null}

              {economicRangeEntries.length || economicEntries.length ? (
                <DetailSection title="Economía de la tarea">
                  <FieldCluster entries={[...economicRangeEntries, ...economicEntries]} compact />
                </DetailSection>
              ) : null}

              {dirceSummaryEntries.length ? (
                <DetailSection title="DIRCE resumen">
                  <FieldCluster entries={dirceSummaryEntries} compact />
                </DetailSection>
              ) : null}

              {sizeEntries.length || strataEntries.length ? (
                <div className="pair-grid">
                  <PairTable label="DIRCE tamaño empresa" rows={sizeEntries} />
                  <PairTable label="DIRCE estratos" rows={strataEntries} />
                </div>
              ) : null}

              {conditionEntries.length ? (
                <PairTable label="DIRCE condición jurídica" rows={conditionEntries} />
              ) : null}
            </aside>
          </div>

          {!hasVisibleContent ? (
            <div className="empty-section">No hay columnas que coincidan con el filtro.</div>
          ) : null}
        </div>
      </section>
    </div>
  );
}

function App() {
  const tableContainerRef = useRef(null);
  const isMobile = useIsNarrowViewport();
  const tableRowHeight = isMobile ? MOBILE_TABLE_ROW_HEIGHT : DESKTOP_TABLE_ROW_HEIGHT;
  const [modalParams, setModalParams] = useState(() => readModalParamsFromUrl());
  const [dataset, setDataset] = useState(null);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(true);
  const [sorting, setSorting] = useState([{ id: "priority_score", desc: true }]);
  const [search, setSearch] = useState("");
  const [detailSearch, setDetailSearch] = useState("");
  const [selectedRowId, setSelectedRowId] = useState("");
  const [isModalOpen, setIsModalOpen] = useState(false);
  const deferredSearch = useDeferredValue(search);

  useEffect(() => {
    let ignore = false;

    async function loadData() {
      setLoading(true);

      try {
        const response = await fetch(DATA_PATH);
        if (!response.ok) {
          throw new Error(`No se pudo cargar ${DATA_PATH}`);
        }

        const nextDataset = await response.json();
        if (ignore) {
          return;
        }

        setDataset(nextDataset);
        setError("");
      } catch (nextError) {
        if (!ignore) {
          setError(nextError instanceof Error ? nextError.message : "Error cargando el dataset");
        }
      } finally {
        if (!ignore) {
          setLoading(false);
        }
      }
    }

    loadData();

    return () => {
      ignore = true;
    };
  }, []);

  const preparedRows = useMemo(() => prepareRows(dataset?.rows ?? []), [dataset?.rows]);
  const normalizedSearch = useMemo(() => normalizeQuery(deferredSearch), [deferredSearch]);
  const rows = useMemo(
    () => filterRows(preparedRows, normalizedSearch),
    [preparedRows, normalizedSearch],
  );
  const allRowsById = useMemo(
    () => new Map(preparedRows.map((row) => [row.__id, row])),
    [preparedRows],
  );
  const rowsById = useMemo(
    () => new Map(rows.map((row) => [row.__id, row])),
    [rows],
  );
  const scoreMetadata = dataset?.scoreMetadata ?? FALLBACK_SCORE_METADATA;
  const columns = useMemo(
    () => buildColumns(dataset?.columns ?? [], scoreMetadata, isMobile),
    [dataset?.columns, scoreMetadata, isMobile],
  );

  useEffect(() => {
    if (!rows.length) {
      return;
    }

    if (selectedRowId && allRowsById.has(selectedRowId)) {
      return;
    }

    if (!rowsById.has(selectedRowId)) {
      setSelectedRowId(rows[0].__id);
    }
  }, [allRowsById, rows, rowsById, selectedRowId]);

  useEffect(() => {
    function handlePopState() {
      setModalParams(readModalParamsFromUrl());
    }

    window.addEventListener("popstate", handlePopState);
    return () => {
      window.removeEventListener("popstate", handlePopState);
    };
  }, []);

  useEffect(() => {
    if (!preparedRows.length) {
      return;
    }

    const modalRow = findRowFromModalParams(preparedRows, modalParams);
    if (modalRow) {
      setSelectedRowId(modalRow.__id);
      setDetailSearch("");
      setIsModalOpen(true);
      return;
    }

    if (!modalParams.task) {
      setIsModalOpen(false);
    }
  }, [modalParams, preparedRows]);

  const selectedRow = allRowsById.get(selectedRowId) ?? rows[0] ?? preparedRows[0] ?? null;

  const table = useReactTable({
    data: rows,
    columns,
    state: { sorting },
    onSortingChange: setSorting,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
    getRowId: (row) => row.__id,
  });

  const tableRows = table.getRowModel().rows;
  const rowVirtualizer = useVirtualizer({
    count: tableRows.length,
    getScrollElement: () => tableContainerRef.current,
    estimateSize: () => tableRowHeight,
    overscan: 12,
  });

  const virtualRows = rowVirtualizer.getVirtualItems();

  useEffect(() => {
    rowVirtualizer.scrollToOffset(0);
  }, [normalizedSearch, sorting, rowVirtualizer, tableRowHeight]);

  useEffect(() => {
    if (!isModalOpen) {
      return undefined;
    }

    const previousOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";

    function handleKeyDown(event) {
      if (event.key === "Escape") {
        closeModal();
      }
    }

    window.addEventListener("keydown", handleKeyDown);

    return () => {
      document.body.style.overflow = previousOverflow;
      window.removeEventListener("keydown", handleKeyDown);
    };
  }, [isModalOpen]);

  function closeModal() {
    setIsModalOpen(false);
    setDetailSearch("");
    setModalParams({ task: "", activity: "" });
    updateModalUrl(null, "replace");
  }

  function openRowModal(rowId) {
    const row = allRowsById.get(rowId);
    if (!row) {
      return;
    }

    setSelectedRowId(rowId);
    setDetailSearch("");
    setIsModalOpen(true);
    const nextModalParams = {
      task: row.task_name ?? "",
      activity: row.subsector ?? "",
    };
    setModalParams(nextModalParams);
    updateModalUrl(row, "push");
  }

  return (
    <main className="page-shell">
      <section className="hero">
        <div>
          <h1>Future of Work</h1>
          <p className="hero-copy">
            Tareas con más potencial de automatización y oportunidad de negocio del mercado
            laboral en España.
          </p>
          <div className="hero-methodology">
            <strong>Metodología</strong>
            <p>
              Este viewer parte del dataset enriquecido de actividades DIRCE del INE. Para cada
              actividad principal se identifican workflows concretos de trabajo y se evalúan de
              forma estructurada según su automatizabilidad, su impacto económico y su encaje como
              producto o negocio.
            </p>
            <ul>
              <li>
                <span>1.</span>
                <p>
                  <strong>Universo analizado.</strong> Se seleccionan actividades con peso
                  relevante en empleo y tejido empresarial, y se conservan también sus métricas
                  DIRCE de tamaño, crecimiento y composición empresarial.
                </p>
              </li>
              <li>
                <span>2.</span>
                <p>
                  <strong>Generación de tareas.</strong> Para cada actividad se proponen workflows
                  específicos, no tareas genéricas, incluyendo contexto sectorial, inputs, outputs,
                  frecuencia, excepciones, rol ejecutor y riesgos operativos.
                </p>
              </li>
              <li>
                <span>3.</span>
                <p>
                  <strong>Scoring base.</strong> Cada workflow se puntúa en escala 1-5 en
                  dimensiones de automatización como verificabilidad, latencia de verificación,
                  estandarización, tolerancia al error, integración y autonomía; y en dimensiones
                  de negocio como pain size, recurrencia, ROI, presupuesto comprador, adopción y
                  productización.
                </p>
              </li>
              <li>
                <span>4.</span>
                <p>
                  <strong>Priority Score.</strong> La prioridad final se normaliza a 0-100 y
                  combina tres bloques: fiabilidad de automatización (50%), valor de negocio (32%)
                  y encaje producto-escalabilidad (18%). La necesidad de componente de servicio se
                  invierte: cuanto menos servicio humano requiera una tarea, más prioridad recibe.
                </p>
              </li>
            </ul>
          </div>
          <p className="hero-credits">
            Creado por{" "}
            <a
              href="https://x.com/victorianoi"
              target="_blank"
              rel="noreferrer"
            >
              Victoriano Izquierdo (@victorianoi)
            </a>
          </p>
        </div>
      </section>

      <section className="toolbar">
        <label className="search-field">
          <span>Buscar fila</span>
          <input
            type="search"
            value={search}
            onChange={(event) => {
              const nextValue = event.target.value;
              startTransition(() => setSearch(nextValue));
            }}
            placeholder="Ej. limpieza, PR, ROI, software..."
          />
        </label>
        <div className="toolbar-note">
          La tabla muestra todas las columnas del CSV ordenadas por semántica.
          <br />
          Haz click en una fila para abrir un modal superior con todo el detalle.
        </div>
      </section>

      {loading ? <div className="panel-state">Cargando dataset…</div> : null}
      {error ? <div className="panel-state panel-state--error">{error}</div> : null}

      {!loading && !error ? (
        <section className="table-panel">
          <div className="table-panel__meta">
            <p>{rows.length} filas visibles</p>
          </div>
          <p className="table-scroll-hint">
            En móvil quedan fijas <strong>Tarea</strong> y <strong>Priority Score</strong>.
            Desliza horizontalmente para explorar el resto.
          </p>
          <div
            ref={tableContainerRef}
            className={`table-wrapper ${isMobile ? "table-wrapper--mobile" : ""}`}
          >
            <div className="table-canvas" style={{ width: table.getTotalSize() }}>
              <div className="table-head">
                {table.getHeaderGroups().map((headerGroup) => (
                  <div key={headerGroup.id} className="table-row table-row--header">
                    {headerGroup.headers.map((header) => {
                      const meta = header.column.columnDef.meta ?? {};
                      const stickyClass = meta.sticky ? "is-sticky" : "";
                      const stickyEdgeClass = meta.stickyEdge ? "is-sticky-edge" : "";
                      const numericClass = meta.numeric ? "is-numeric" : "";

                      return (
                        <div
                          key={header.id}
                          className={`table-cell table-cell--header ${stickyClass} ${stickyEdgeClass} ${numericClass}`}
                          style={{
                            width: meta.width,
                            minWidth: meta.width,
                            maxWidth: meta.width,
                            left: meta.stickyLeft,
                          }}
                        >
                          {header.isPlaceholder ? null : (
                            <button
                              type="button"
                              className="header-button"
                              onClick={header.column.getToggleSortingHandler()}
                            >
                              {flexRender(header.column.columnDef.header, header.getContext())}
                              <span className="sort-indicator">
                                {{
                                  asc: "↑",
                                  desc: "↓",
                                }[header.column.getIsSorted()] ?? "↕"}
                              </span>
                            </button>
                          )}
                        </div>
                      );
                    })}
                  </div>
                ))}
              </div>

              <div className="table-body" style={{ height: `${rowVirtualizer.getTotalSize()}px` }}>
                {virtualRows.map((virtualRow) => {
                  const row = tableRows[virtualRow.index];
                  const isSelected = row.original.__id === selectedRow?.__id;

                  return (
                    <div
                      key={row.id}
                      className={`table-row table-row--body ${isSelected ? "is-selected" : ""}`}
                      style={{
                        transform: `translateY(${virtualRow.start}px)`,
                        height: `${tableRowHeight}px`,
                      }}
                      tabIndex={0}
                      onClick={() => openRowModal(row.original.__id)}
                      onKeyDown={(event) => {
                        if (event.key === "Enter" || event.key === " ") {
                          event.preventDefault();
                          openRowModal(row.original.__id);
                        }
                      }}
                    >
                      {row.getVisibleCells().map((cell) => {
                        const meta = cell.column.columnDef.meta ?? {};
                        const stickyClass = meta.sticky ? "is-sticky" : "";
                        const stickyEdgeClass = meta.stickyEdge ? "is-sticky-edge" : "";
                        const numericClass = meta.numeric ? "is-numeric" : "";

                        return (
                          <div
                            key={cell.id}
                            className={`table-cell table-cell--body ${stickyClass} ${stickyEdgeClass} ${numericClass}`}
                            style={{
                              width: meta.width,
                              minWidth: meta.width,
                              maxWidth: meta.width,
                              left: meta.stickyLeft,
                            }}
                            title={getCellTitle(cell.column.id, row.original, scoreMetadata)}
                          >
                            <div className="table-cell__content">
                              {flexRender(cell.column.columnDef.cell, cell.getContext())}
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  );
                })}
              </div>
            </div>
          </div>
        </section>
      ) : null}

      {isModalOpen && selectedRow ? (
        <DetailModal
          row={selectedRow}
          query={detailSearch}
          onQueryChange={setDetailSearch}
          onClose={closeModal}
          scoreMetadata={scoreMetadata}
        />
      ) : null}
    </main>
  );
}

export default App;
