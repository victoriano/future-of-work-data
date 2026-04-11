#!/usr/bin/env python3
"""
Enrich INE DIRCE activities with AI-workflow opportunity analysis using cuery.

For each "Actividad principal" (subsector) in the aggregated DIRCE table, asks an
LLM to identify 3–5 concrete, activity-specific workflows that humans currently
perform and evaluate each one across:

  - AI automation feasibility (verifiability-first scoring)
  - Economic profile of the task
  - Business opportunity (pain, ROI, adoption, productization, ...)

The aggregate AI Feasibility, Business Opportunity and Priority scores are
computed deterministically in this script from the per-dimension 1-5 scores
produced by the model (the LLM only outputs the component scores + rationales).

Output CSV keeps ALL the original DIRCE columns for each activity, so each row
is one (actividad, workflow) pair with both DIRCE metrics and the LLM analysis.
"""

import argparse
import asyncio
import os
import sys
from pathlib import Path
from typing import Literal

import pandas as pd
import polars as pl
from cuery import task
from cuery.prompt import Prompt
from cuery.response import Response
from pydantic import Field

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from fow.workflow_scoring import compute_scores

INPUT_FILE = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "ine_dirce"
    / "ine_dirce_aggregated_by_activity.parquet"
)
OUTPUT_DIR = PROJECT_ROOT / "data" / "processed" / "ine_dirce"

DEFAULT_MODEL = "google/gemini-2.5-pro"


# ============================
# Load API keys from ~/.config/api-keys/.env if available
# ============================


def _load_api_keys():
    path = Path.home() / ".config" / "api-keys" / ".env"
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


_load_api_keys()


# ============================
# Response schema
# ============================

Verdict = Literal["mala", "media", "buena"]
SolutionType = Literal["software", "servicio", "hibrido", "no_recomendable"]
BusinessModel = Literal[
    "saas_puro",
    "servicio_habilitado_por_ia",
    "saas_mas_operaciones_humanas",
    "consultoria_implementacion",
    "no_recomendable",
]


class WorkflowAnalysis(Response):
    """Evaluation of a single concrete workflow inside a given economic activity."""

    # 1) Executive summary
    task_name: str = Field(
        ..., min_length=5, max_length=120, description="Nombre corto y concreto de la tarea"
    )
    sector_context: str = Field(
        ..., max_length=160, description="Sector o tipo de empresa donde aparece la tarea"
    )
    verdict: Verdict = Field(..., description="Veredicto global: mala, media o buena")
    recommended_solution_type: SolutionType = Field(
        ..., description="Tipo de solución más razonable hoy"
    )

    # 2) Operational description
    person_actions: str = Field(
        ..., max_length=400, description="Qué hace exactamente la persona"
    )
    typical_input: str = Field(..., max_length=250)
    typical_output: str = Field(..., max_length=250)
    frequency: str = Field(..., max_length=120, description="Frecuencia de ejecución")
    process_variability: str = Field(..., max_length=250)
    common_exceptions: str = Field(..., max_length=250)

    # 3) AI automation evaluation (1-5 component scores + short rationales)
    verifiability_score: int = Field(..., ge=1, le=5)
    verifiability_reason: str = Field(..., max_length=220)

    verification_latency_score: int = Field(..., ge=1, le=5)
    verification_latency_reason: str = Field(..., max_length=220)

    input_standardization_score: int = Field(..., ge=1, le=5)
    input_standardization_reason: str = Field(..., max_length=220)

    output_standardization_score: int = Field(..., ge=1, le=5)
    output_standardization_reason: str = Field(..., max_length=220)

    error_tolerance_score: int = Field(..., ge=1, le=5)
    error_tolerance_reason: str = Field(..., max_length=220)

    integration_ease_score: int = Field(..., ge=1, le=5)
    integration_ease_reason: str = Field(..., max_length=220)

    autonomy_score: int = Field(..., ge=1, le=5)
    autonomy_reason: str = Field(..., max_length=220)

    # 4) Economic evaluation
    executing_role: str = Field(..., max_length=120)
    typical_seniority: str = Field(..., max_length=80)
    annual_gross_salary_eur_min: int = Field(..., ge=0, le=500_000)
    annual_gross_salary_eur_max: int = Field(..., ge=0, le=500_000)
    company_cost_eur_min: int = Field(..., ge=0, le=700_000)
    company_cost_eur_max: int = Field(..., ge=0, le=700_000)
    avg_time_per_task_minutes: float = Field(..., ge=0, le=10_000)
    monthly_volume_per_company: int = Field(
        ..., ge=0, description="Volumen mensual típico de esta tarea por empresa"
    )
    monthly_human_cost_eur_min: int = Field(..., ge=0)
    monthly_human_cost_eur_max: int = Field(..., ge=0)
    non_human_costs: str = Field(
        ...,
        max_length=500,
        description="Software, licencias, BPO, retrabajo, coordinación interna, etc.",
    )
    economic_assumptions: str = Field(
        ..., max_length=400, description="Supuestos usados cuando faltan datos concretos"
    )

    # 5) Business opportunity (1-5 component scores + rationales)
    pain_size_score: int = Field(..., ge=1, le=5)
    pain_size_reason: str = Field(..., max_length=220)

    recurrence_score: int = Field(..., ge=1, le=5)
    recurrence_reason: str = Field(..., max_length=220)

    buyer_budget_score: int = Field(..., ge=1, le=5)
    buyer_budget_reason: str = Field(..., max_length=220)

    roi_clarity_score: int = Field(..., ge=1, le=5)
    roi_clarity_reason: str = Field(..., max_length=220)

    adoption_ease_score: int = Field(..., ge=1, le=5)
    adoption_ease_reason: str = Field(..., max_length=220)

    productization_ease_score: int = Field(..., ge=1, le=5)
    productization_ease_reason: str = Field(..., max_length=220)

    service_component_need_score: int = Field(..., ge=1, le=5)
    service_component_need_reason: str = Field(..., max_length=220)

    # 6) Recommended business model
    recommended_business_model: BusinessModel
    business_model_reason: str = Field(..., max_length=350)

    # 7) Risks and limits
    legal_regulatory_risks: str = Field(..., max_length=350)
    reputational_risks: str = Field(..., max_length=350)
    precision_risks: str = Field(..., max_length=350)
    integration_bottlenecks: str = Field(..., max_length=350)
    misleading_attractiveness: str = Field(
        ...,
        max_length=350,
        description="Razones por las que la tarea podría parecer atractiva pero no serlo",
    )


class ActivityWorkflows(Response):
    """Set of 3–5 workflow analyses for one Actividad principal."""

    workflows: list[WorkflowAnalysis] = Field(
        ...,
        min_length=3,
        max_length=5,
        description="Entre 3 y 5 workflows concretos y específicos de esta actividad económica",
    )


# ============================
# Prompt
# ============================

SYSTEM_PROMPT = """Actúa como un analista experto en automatización con IA, diseño de productos SaaS y servicios digitales B2B, y evaluación económica de procesos operativos.

Te voy a dar una actividad económica española (CNAE a 3 dígitos del DIRCE). Tu trabajo es:

1. Identificar entre 3 y 5 tareas/workflows CONCRETOS y ESPECÍFICOS de esa actividad (no genéricos de oficina tipo "gestión de emails" ni "rellenar Excel") que hoy realizan personas en empresas de ese sector.
2. Evaluar cada uno desde dos perspectivas:
   a) Qué tan viable es automatizarlo con IA actual (o IA + software + supervisión humana).
   b) Qué tan buena oportunidad de negocio sería construir un producto, servicio o híbrido para sustituir parcial o totalmente ese trabajo humano y reducir costes.

Tu criterio principal es la VERIFICABILIDAD del resultado:
- Qué tan objetivamente puede comprobarse que la tarea está bien resuelta.
- Si esa verificación puede hacerse automáticamente.
- Cuánto tiempo tarda esa verificación.

HIPÓTESIS BASE
- Las tareas con outputs muy verificables y feedback rápido son mejores candidatas para automatización con IA.
- Las tareas con feedback lento, ambiguo o difícil de verificar son peores candidatas, aunque parezcan valiosas.

REGLAS IMPORTANTES
- No asumas que una tarea es buena para IA sólo porque es repetitiva.
- Penaliza mucho las tareas cuya calidad sólo puede juzgarse a largo plazo o con fuerte subjetividad.
- Premia mucho las tareas con validación automática o casi automática.
- Distingue entre "IA puede ayudar" y "IA puede sustituir trabajo humano de verdad".
- Si la mejor solución es un flujo híbrido, dilo claramente.
- Cuando falten datos, usa supuestos razonables y explícalos en `economic_assumptions`.
- Sólo tareas basadas en ordenador o papel (nada físico/manual no automatizable por software).
- Responde en español.

ESCALAS (1-5, obligatorio puntuar las 7 dimensiones de IA y las 7 de oportunidad)
IA:
- Verificabilidad: 1 = totalmente subjetivo; 5 = verificable automáticamente y sin ambigüedad.
- Latencia de verificación: 1 = semanas/meses; 5 = segundos/minutos.
- Estandarización de inputs: 1 = muy variables/no estructurados; 5 = formato fijo/estructurado.
- Estandarización de outputs: 1 = respuestas abiertas y únicas; 5 = plantilla/campo fijo.
- Tolerancia al error / reversibilidad: 1 = errores catastróficos e irreversibles; 5 = fácil revertir y bajo impacto.
- Facilidad de integración: 1 = sistemas cerrados/legacy; 5 = APIs disponibles, plug & play.
- Nivel de autonomía hoy: 1 = requiere supervisión humana constante; 5 = operable end-to-end.

Oportunidad de negocio:
- Dolor económico: 1 = problema menor; 5 = sangrado fuerte y conocido.
- Frecuencia / recurrencia: 1 = puntual/anual; 5 = continuo/diario.
- Presupuesto del comprador: 1 = presupuestos muy pequeños; 5 = presupuestos grandes y existentes.
- Claridad del ROI: 1 = difícil de demostrar; 5 = ROI evidente y rápido.
- Facilidad de adopción: 1 = cambia mucho los procesos; 5 = drop-in, sin fricción.
- Facilidad de productizar: 1 = cada cliente es único; 5 = producto replicable.
- Necesidad de componente de servicio: 1 = puramente software; 5 = requiere equipo humano importante (este score NO se mezcla en el Business Opportunity agregado, sólo guía el modelo recomendado).

El script calcula automáticamente `AI Feasibility Score`, `Business Opportunity Score` y `Priority Score` a partir de tus puntuaciones, así que sé coherente entre los scores y tus justificaciones."""

USER_PROMPT = """Actividad económica a analizar:

Sector (división CNAE): {{sector}}
Subsector / Actividad principal (CNAE 3 dígitos): {{subsector}}
Nº de empresas en 2024: {{total_2024}}
Empleados estimados en 2024: {{estimated_employees_2024}}
Mix por tamaño (2024):
  - Micro (0-9): {{size_micro_pct}}%
  - Pequeñas (10-49): {{size_small_pct}}%
  - Medianas (50-249): {{size_medium_pct}}%
  - Grandes (250+): {{size_large_pct}}%

Identifica entre 3 y 5 workflows concretos y específicos de ESTA actividad (no tareas genéricas de oficina) y evalúa cada uno rellenando el esquema exactamente."""

PROMPT = Prompt(
    messages=[
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": USER_PROMPT},
    ],
    required=[
        "sector",
        "subsector",
        "total_2024",
        "estimated_employees_2024",
        "size_micro_pct",
        "size_small_pct",
        "size_medium_pct",
        "size_large_pct",
    ],
)


# ============================
# Main
# ============================


async def main_async(args):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Reading: {INPUT_FILE}")
    df = pl.read_parquet(INPUT_FILE)

    # Top-N by Estimated_Employees_2024 (descending)
    top = df.sort("Estimated_Employees_2024", descending=True, nulls_last=True).head(args.top_n)
    print(f"Selected top {top.height} activities by Estimated_Employees_2024.")
    print(
        top.select(["Actividad principal", "Estimated_Employees_2024"]).to_pandas().to_string(
            index=False
        )
    )

    # Build LLM context (pandas for cuery)
    ctx = (
        top.select(
            pl.col("Division").alias("sector"),
            pl.col("Actividad principal").alias("subsector"),
            pl.col("Total_2024").alias("total_2024"),
            pl.col("Estimated_Employees_2024").alias("estimated_employees_2024"),
            pl.col("Size_Micro (0-9)_pct").alias("size_micro_pct"),
            pl.col("Size_Small (10-49)_pct").alias("size_small_pct"),
            pl.col("Size_Medium (50-249)_pct").alias("size_medium_pct"),
            pl.col("Size_Large (250+)_pct").alias("size_large_pct"),
        )
        .to_pandas()
    )

    task_obj = task.Task(
        name="ActivityWorkflows",
        prompt=PROMPT,
        response=ActivityWorkflows,
        model=args.model,
    )

    print(f"\nRunning {args.model} (concurrency={args.concurrent}) over {len(ctx)} activities...")
    result = await task_obj(context=ctx, model=args.model, n_concurrent=args.concurrent)

    # Cost tracking (OpenAI only)
    try:
        usage_df = result.usage()
        if usage_df is not None and not usage_df.empty and "cost" in usage_df.columns:
            total_cost = float(usage_df["cost"].sum())
            prompt_tok = int(usage_df.get("prompt", pd.Series([0])).sum())
            compl_tok = int(usage_df.get("completion", pd.Series([0])).sum())
            print(
                f"Tokens: {prompt_tok:,} prompt + {compl_tok:,} completion. "
                f"Total cost: ${total_cost:.4f}"
            )
    except Exception as e:  # noqa: BLE001
        print(f"(No cost info available: {e})")

    workflows_df: pd.DataFrame = result.to_pandas()
    print(f"LLM returned {len(workflows_df)} workflow rows.")

    # Merge back every DIRCE column for the selected activities
    dirce_full = top.to_pandas().rename(
        columns={"Division": "sector", "Actividad principal": "subsector"}
    )
    merged = workflows_df.merge(dirce_full, on=["sector", "subsector"], how="left")

    # Compute aggregate scores deterministically from the component 1-5 scores
    score_cols = merged.apply(lambda r: pd.Series(compute_scores(r.to_dict())), axis=1)
    merged = pd.concat([merged, score_cols], axis=1)

    # Order: activities by employees desc, then workflows by priority desc
    merged = merged.sort_values(
        ["estimated_employees_2024", "priority_score"], ascending=[False, False]
    ).reset_index(drop=True)

    # Move key analysis columns to the front for quick reading
    front = [
        "sector",
        "subsector",
        "estimated_employees_2024",
        "task_name",
        "verdict",
        "priority_score",
        "ai_feasibility_score",
        "business_opportunity_score",
        "recommended_business_model",
        "recommended_horizon",
        "relative_priority",
    ]
    front = [c for c in front if c in merged.columns]
    rest = [c for c in merged.columns if c not in front]
    merged = merged[front + rest]

    output_csv = OUTPUT_DIR / f"{args.output_name}.csv"
    merged.to_csv(output_csv, index=False)
    print(f"\nSaved: {output_csv}  ({len(merged)} rows, {len(merged.columns)} columns)")

    if args.also_parquet:
        output_parquet = OUTPUT_DIR / f"{args.output_name}.parquet"
        merged.to_parquet(output_parquet, index=False)
        print(f"Saved: {output_parquet}")


def main():
    parser = argparse.ArgumentParser(
        description="Enrich top-N DIRCE activities with AI-workflow opportunity analysis via cuery."
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=20,
        help="Top N activities by Estimated_Employees_2024 (default: 20).",
    )
    parser.add_argument(
        "--concurrent",
        type=int,
        default=5,
        help="Max concurrent LLM requests (default: 5).",
    )
    parser.add_argument(
        "--model",
        type=str,
        default=DEFAULT_MODEL,
        help=f"Cuery model identifier (default: {DEFAULT_MODEL}).",
    )
    parser.add_argument(
        "--output-name",
        type=str,
        default="ine_dirce_workflows_enriched_top20",
        help="Output CSV/Parquet basename (no extension).",
    )
    parser.add_argument(
        "--also-parquet",
        action="store_true",
        help="Also save a .parquet file alongside the CSV.",
    )
    args = parser.parse_args()

    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
