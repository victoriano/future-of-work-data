from __future__ import annotations

from copy import deepcopy


def normalize_score(score: int | float) -> float:
    """Convert a 1-5 score to a 0-100 scale."""
    return ((float(score) - 1) / 4) * 100


def inverse_normalize_score(score: int | float) -> float:
    """Invert a 1-5 score and convert it to a 0-100 scale."""
    return (1 - ((float(score) - 1) / 4)) * 100


AI_FEASIBILITY_COMPONENTS = [
    {"field": "verifiability_score", "label": "Verificabilidad", "weight": 0.30},
    {
        "field": "verification_latency_score",
        "label": "Latencia de verificación",
        "weight": 0.20,
    },
    {
        "field": "input_standardization_score",
        "label": "Estandarización de inputs",
        "weight": 0.10,
    },
    {
        "field": "output_standardization_score",
        "label": "Estandarización de outputs",
        "weight": 0.10,
    },
    {"field": "error_tolerance_score", "label": "Tolerancia al error", "weight": 0.15},
    {"field": "integration_ease_score", "label": "Facilidad de integración", "weight": 0.10},
    {"field": "autonomy_score", "label": "Autonomía", "weight": 0.05},
]

BUSINESS_OPPORTUNITY_COMPONENTS = [
    {"field": "pain_size_score", "label": "Dolor económico", "weight": 0.25},
    {"field": "recurrence_score", "label": "Recurrencia", "weight": 0.20},
    {"field": "roi_clarity_score", "label": "Claridad del ROI", "weight": 0.20},
    {"field": "adoption_ease_score", "label": "Facilidad de adopción", "weight": 0.15},
    {
        "field": "productization_ease_score",
        "label": "Facilidad de productizar",
        "weight": 0.10,
    },
    {"field": "buyer_budget_score", "label": "Presupuesto del comprador", "weight": 0.10},
]

PRIORITY_GROUPS = [
    {
        "id": "automation_reliability",
        "label": "Fiabilidad de automatización",
        "weight": 0.50,
    },
    {"id": "business_value", "label": "Valor de negocio", "weight": 0.32},
    {
        "id": "product_scaling_fit",
        "label": "Encaje producto-escalabilidad",
        "weight": 0.18,
    },
]

PRIORITY_COMPONENTS = [
    {
        "field": "verifiability_score",
        "label": "Verificabilidad",
        "weight": 0.14,
        "group": "automation_reliability",
    },
    {
        "field": "verification_latency_score",
        "label": "Latencia de verificación",
        "weight": 0.08,
        "group": "automation_reliability",
    },
    {
        "field": "error_tolerance_score",
        "label": "Tolerancia al error",
        "weight": 0.08,
        "group": "automation_reliability",
    },
    {
        "field": "integration_ease_score",
        "label": "Facilidad de integración",
        "weight": 0.07,
        "group": "automation_reliability",
    },
    {
        "field": "autonomy_score",
        "label": "Autonomía",
        "weight": 0.05,
        "group": "automation_reliability",
    },
    {
        "field": "input_standardization_score",
        "label": "Estandarización de inputs",
        "weight": 0.04,
        "group": "automation_reliability",
    },
    {
        "field": "output_standardization_score",
        "label": "Estandarización de outputs",
        "weight": 0.04,
        "group": "automation_reliability",
    },
    {
        "field": "pain_size_score",
        "label": "Dolor económico",
        "weight": 0.10,
        "group": "business_value",
    },
    {
        "field": "recurrence_score",
        "label": "Recurrencia",
        "weight": 0.08,
        "group": "business_value",
    },
    {
        "field": "roi_clarity_score",
        "label": "Claridad del ROI",
        "weight": 0.10,
        "group": "business_value",
    },
    {
        "field": "buyer_budget_score",
        "label": "Presupuesto del comprador",
        "weight": 0.04,
        "group": "business_value",
    },
    {
        "field": "adoption_ease_score",
        "label": "Facilidad de adopción",
        "weight": 0.06,
        "group": "product_scaling_fit",
    },
    {
        "field": "productization_ease_score",
        "label": "Facilidad de productizar",
        "weight": 0.07,
        "group": "product_scaling_fit",
    },
    {
        "field": "service_component_need_score",
        "label": "Necesidad de componente de servicio",
        "weight": 0.05,
        "group": "product_scaling_fit",
        "invert": True,
        "note": "Se invierte: menos necesidad de servicio implica más prioridad.",
    },
]

PRIORITY_THRESHOLDS = [
    {"min": 80, "relative_priority": "muy_alta", "recommended_horizon": "ya"},
    {"min": 65, "relative_priority": "alta", "recommended_horizon": "12_meses"},
    {"min": 50, "relative_priority": "media", "recommended_horizon": "24_meses"},
    {"min": 0, "relative_priority": "baja", "recommended_horizon": "no_priorizar"},
]

SCORE_METADATA = {
    "normalization": {
        "inputScale": "1-5",
        "outputScale": "0-100",
        "formula": "((score - 1) / 4) * 100",
        "inverseFormula": "(1 - ((score - 1) / 4)) * 100",
    },
    "scores": {
        "ai_feasibility_score": {
            "label": "AI Feasibility Score",
            "formula": "Suma ponderada de 7 dimensiones de automatización",
            "components": deepcopy(AI_FEASIBILITY_COMPONENTS),
        },
        "business_opportunity_score": {
            "label": "Business Opportunity Score",
            "formula": "Suma ponderada de 6 dimensiones de oportunidad",
            "components": deepcopy(BUSINESS_OPPORTUNITY_COMPONENTS),
            "excluded": [
                {
                    "field": "service_component_need_score",
                    "label": "Necesidad de componente de servicio",
                    "reason": (
                        "No entra en Business Opportunity; se usa solo como ajuste de "
                        "priorización/product scaling fit."
                    ),
                }
            ],
        },
        "priority_score": {
            "label": "Priority Score",
            "formula": (
                "Suma ponderada directa de 14 scores: fiabilidad de automatización "
                "50%, valor de negocio 32%, encaje producto-escalabilidad 18%"
            ),
            "groups": deepcopy(PRIORITY_GROUPS),
            "components": deepcopy(PRIORITY_COMPONENTS),
            "thresholds": deepcopy(PRIORITY_THRESHOLDS),
        },
    },
}


def _component_breakdown(row: dict, components: list[dict]) -> tuple[float, list[dict]]:
    total = 0.0
    breakdown = []

    for component in components:
        raw_score = float(row[component["field"]])
        normalized = (
            inverse_normalize_score(raw_score)
            if component.get("invert")
            else normalize_score(raw_score)
        )
        contribution = normalized * component["weight"]
        total += contribution
        breakdown.append(
            {
                **component,
                "raw_score": round(raw_score, 2),
                "normalized_score": round(normalized, 1),
                "contribution": round(contribution, 1),
            }
        )

    return round(total, 1), breakdown


def compute_priority_breakdown(row: dict) -> dict:
    total, components = _component_breakdown(row, PRIORITY_COMPONENTS)
    groups = []

    for group in PRIORITY_GROUPS:
        group_components = [c for c in components if c["group"] == group["id"]]
        contribution = round(sum(c["contribution"] for c in group_components), 1)
        normalized_score = round(contribution / group["weight"], 1) if group["weight"] else None
        groups.append(
            {
                **group,
                "normalized_score": normalized_score,
                "contribution": contribution,
                "components": group_components,
            }
        )

    return {
        "total": total,
        "groups": groups,
        "components": components,
        "thresholds": deepcopy(PRIORITY_THRESHOLDS),
    }


def _classify_priority(priority: float) -> tuple[str, str]:
    for threshold in PRIORITY_THRESHOLDS:
        if priority >= threshold["min"]:
            return threshold["relative_priority"], threshold["recommended_horizon"]
    return "baja", "no_priorizar"


def compute_scores(row: dict) -> dict:
    ai, _ = _component_breakdown(row, AI_FEASIBILITY_COMPONENTS)
    biz, _ = _component_breakdown(row, BUSINESS_OPPORTUNITY_COMPONENTS)
    priority = compute_priority_breakdown(row)["total"]
    relative, horizon = _classify_priority(priority)

    return {
        "ai_feasibility_score": ai,
        "business_opportunity_score": biz,
        "priority_score": priority,
        "relative_priority": relative,
        "recommended_horizon": horizon,
    }
