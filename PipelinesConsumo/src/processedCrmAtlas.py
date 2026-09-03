import pandas as pd
import numpy as np
import pytz
from datetime import datetime
from unidecode import unidecode

try:
    from PipelinesConsumo.src.crm_config import (
        CRM_RENAME_DICTS,
        CRM_REQUIRED_COLUMNS,
        CRM_REPORTE_OPORTUNIDADES_COLUMNS,
        CRM_REPORTE_CITAS_COLUMNS,
        CRM_REPORTE_SIMULACIONES_COLUMNS,
        CRM_STATUS_CITA_COMPLETA,
        CRM_STATUS_PEDIDOS_ABIERTOS,
        CRM_EQUIPOS_SALES_CENTER,
        CRM_EQUIPOS_ESPACIOS,
        CRM_WORK_TYPES_COMPRADOR,
        CRM_WORK_TYPES_VENDEDOR,
        CRM_CRITERIOS_SHOW_CITAS,
        CRM_CRITERIOS_HISTSHOW_CITAS,
    )
    from PipelinesConsumo.src.constants import mexico_tz
except ModuleNotFoundError:
    from src.crm_config import (
        CRM_RENAME_DICTS,
        CRM_REQUIRED_COLUMNS,
        CRM_REPORTE_OPORTUNIDADES_COLUMNS,
        CRM_REPORTE_CITAS_COLUMNS,
        CRM_REPORTE_SIMULACIONES_COLUMNS,
        CRM_STATUS_CITA_COMPLETA,
        CRM_STATUS_PEDIDOS_ABIERTOS,
        CRM_EQUIPOS_SALES_CENTER,
        CRM_EQUIPOS_ESPACIOS,
        CRM_WORK_TYPES_COMPRADOR,
        CRM_WORK_TYPES_VENDEDOR,
        CRM_CRITERIOS_SHOW_CITAS,
        CRM_CRITERIOS_HISTSHOW_CITAS,
    )
    from src.constants import mexico_tz


class ProcessedCrmAtlas:
    """Business transforms for Salesforce CRM reports used by Atlas Consumo."""

    SALES_CENTER_KPI_REQUIRED_COLUMNS = {
        "opportunity_id",
        "fecha_asignacion",
        "case_owner_equipo_perf_sc",
        "equipo_asesor_caso_tomado_perf_sc",
        "fecha_caso_tomado_sc",
        "perf_fecha_primer_contacto",
        "perf_contactado",
        "perf_interesado",
        "perf_intencion_pago",
    }

    def __init__(self):
        """Store the run date in Mexico City timezone for date-window logic."""
        self.today = datetime.now(tz=pytz.timezone(mexico_tz)).strftime("%Y-%m-%d")

    @staticmethod
    def _normalize_value(value):
        """Lowercase, trim, and remove accents from text values only."""
        if isinstance(value, str):
            return unidecode(value).strip().lower()
        return value

    @classmethod
    def _normalize_series(cls, series):
        """Apply CRM text normalization to every value in a pandas Series."""
        return series.apply(cls._normalize_value)

    @staticmethod
    def _to_datetime_str(series,fmt="%d/%m/%Y, %H:%M" ,fmt_string="%Y-%m-%d", utc=False, tz=None):
        """
        Convert a date/datetime Series to formatted strings with safe coercion.

        Invalid dates become null-like values instead of breaking the whole
        transform. Use utc=True and tz=mexico_tz for Salesforce UTC timestamps
        that need to be rendered in local Mexico City time.
        """
        dt = pd.to_datetime(series,format=fmt, errors="coerce", utc=utc)
        if utc and tz:
            dt = dt.dt.tz_convert(tz)
        if utc:
            dt = dt.dt.tz_localize(None)
        return dt.dt.strftime(fmt_string)

    @staticmethod
    def _to_int(series):
        """Convert a Series to pandas nullable integers, coercing bad values."""
        return pd.to_numeric(series, errors="coerce").astype("Int64")

    @staticmethod
    def _select_existing_columns(df, columns):
        """Select the requested output columns that exist in the DataFrame."""
        return df[[col for col in columns if col in df.columns]]

    @staticmethod
    def _dedupe_columns(columns):
        """Preserve column order while removing duplicate column names."""
        seen = set()
        result = []
        for col in columns:
            if col not in seen:
                result.append(col)
                seen.add(col)
        return result

    def _validate_columns(self, df, table_name):
        """
        Verify that a raw Salesforce export has the configured required columns.

        This fails early with the table name and missing Salesforce field names,
        which makes source/report drift easier to diagnose from the CRM log.
        """
        missing = [
            col
            for col in CRM_REQUIRED_COLUMNS.get(table_name, [])
            if col not in df.columns
        ]
        if missing:
            raise ValueError(
                f"{table_name}: faltan columnas requeridas de Salesforce: {missing}"
            )

    def _rename(self, rawdf, table_name):
        """Validate and rename one Salesforce export using crm_config.py."""
        self._validate_columns(rawdf, table_name)
        return rawdf.rename(columns=CRM_RENAME_DICTS[table_name])

    def proc_pedidos(self, rawdf):
        """
        Normalize the Salesforce Pedidos export.

        Produces typed order/customer IDs, normalized order status, and helper
        fields parsed from opportunity_name such as opportunity_source and
        opportunity_creation_date.
        """
        pedidos = (
            self._rename(rawdf, "pedidos")
            .assign(
                sf_order_id=lambda x: self._to_int(x.sf_order_id),
                commerce_order_id=lambda x: self._to_int(x.commerce_order_id),
                id_am_vendedor=lambda x: self._to_int(x.id_am_vendedor),
                id_am_comprador=lambda x: self._to_int(x.id_am_comprador),
                created_date=lambda x: self._to_datetime_str(x.created_date),
                status=lambda x: self._normalize_series(x.status),
                opportunity_source=lambda x: self._normalize_series(
                    x.opportunity_name.str.split("-").str[0]
                ),
                opportunity_creation_date=lambda x: pd.to_datetime(
                    x.opportunity_name.str.split("-").str[-1],
                    format="%Y%m%d",
                    errors="coerce",
                ).dt.strftime("%Y-%m-%d")
            )
        )
        return pedidos

    def proc_oportunidades(self, rawdf):
        """
        Normalize the Salesforce Oportunidades export.

        Converts buyer IDs, renders Salesforce UTC creation timestamps in Mexico
        City time, normalizes source/contact fields, and keeps opportunity-level
        profiling/credit attributes ready for the final opportunities report.
        """
        oportunidades = (
            self._rename(rawdf, "oportunidades")
            .assign(
                id_am_comprador=lambda x: self._to_int(x.id_am_comprador),
                opportunity_source=lambda x: self._normalize_series(x.opportunity_source),
                opportunity_created_date=lambda x: self._to_datetime_str(
                    x.opportunity_created_date,
                    fmt_string="%Y-%m-%d %H:%M",
                    
                ),
                last_modified_date=lambda x: self._to_datetime_str(
                    x.last_modified_date
                ),
                perf_contactado=lambda x: self._normalize_series(x.perf_contactado),
                perf_interesado=lambda x: self._normalize_series(x.perf_interesado),
                perf_comentarios=lambda x: self._normalize_series(x.perf_comentarios),
                perf_canal_respuesta=lambda x: self._normalize_series(
                    x.perf_canal_respuesta
                ),
                cred_contactado=lambda x: self._normalize_series(x.cred_contactado),
                cred_perfilamiento_credit=lambda x: self._normalize_series(
                    x.cred_perfilamiento_credit
                ),
                cred_canal_respuesta_credito=lambda x: self._normalize_series(
                    x.cred_canal_respuesta_credito
                ),
                opportunity_stage = lambda x: self._normalize_series(x.opportunity_stage)
            )
        )
        return oportunidades

    def proc_casos(self, rawdf):
        """
        Normalize the Salesforce Casos export.

        Keeps only cases with a subject, standardizes order IDs and case dates,
        and adds case_subject_clean for accent-insensitive business filters like
        perfilamiento contact center / credito.
        """
        casos = (
            self._rename(rawdf, "casos")
            .assign(
                sf_order_id=lambda x: self._to_int(x.sf_order_id),
                commerce_order_id=lambda x: self._to_int(x.commerce_order_id),
                case_created_date=lambda x: self._to_datetime_str(x.case_created_date),
                case_closed_date=lambda x: self._to_datetime_str(x.case_closed_date),
                case_status=lambda x: self._normalize_series(x.case_status),
                case_subject=lambda x: self._normalize_series(x.case_subject),
            )
            [lambda x: x.case_subject.notna()]
        )
        return casos

    def proc_citas(self, rawdf):
        """
        Normalize the Salesforce Citas export.

        Converts appointment/order/customer IDs, standardizes created and
        scheduled timestamps, and normalizes appointment status/work type for
        the buyer appointment summary.
        """
        citas = (
            self._rename(rawdf, "citas")
            .assign(
                commerce_order_id=lambda x: self._to_int(x.commerce_order_id),
                id_am=lambda x: self._to_int(x.id_am),
                created_date=lambda x: self._to_datetime_str(
                    x.created_date,
                    fmt_string="%Y-%m-%d %H:%M",
                ),
                work_type_name=lambda x: self._normalize_series(x.work_type_name),
                status=lambda x: self._normalize_series(x.status),
                sched_start_time=lambda x: self._to_datetime_str(
                    x.sched_start_time,
                    fmt_string="%Y-%m-%d %H:%M",
                ),
                sched_end_time=lambda x: self._to_datetime_str(
                    x.sched_end_time,
                    fmt_string="%Y-%m-%d %H:%M",
                ),
                territorio_cita=lambda x: self._normalize_series(x.territorio_cita),
                fecha_reagendamiento = lambda x: self._to_datetime_str(x.fecha_reagendamiento,
                    fmt_string="%Y-%m-%d %H:%M")
            )
        )
        return citas

    def proc_solicitudes_credito(self, rawdf):
        """
        Normalize the Salesforce Solicitudes de Credito export.

        Standardizes IDs, credit dates, and text statuses. The resulting table is
        both a consumption output and an input for identifying API vs contingency
        credit origin in proc_reporte_oportunidades.
        """
        solicitudes = (
            self._rename(rawdf, "solicitudes_credito")
            .assign(
                id_am_comprador=lambda x: self._to_int(x.id_am_comprador),
                commerce_order_id=lambda x: self._to_int(x.commerce_order_id),
                created_date=lambda x: self._to_datetime_str(
                    x.created_date,
                    fmt_string="%Y-%m-%d %H:%M",
                ),
                fecha_oferta=lambda x: self._to_datetime_str(x.fecha_oferta),
                fecha_ingreso_solicitud=lambda x: self._to_datetime_str(
                    x.fecha_ingreso_solicitud,fmt="%d/%m/%Y"
                ),
                
                status_solicitud=lambda x: self._normalize_series(x.status_solicitud),
                tipo_conclusion_flujo_credito=lambda x: self._normalize_series(
                    x.tipo_conclusion_flujo_credito
                ),
                ingreso_solicitud=lambda x: self._normalize_series(x.ingreso_solicitud),
                documentacion=lambda x: self._normalize_series(x.documentacion),
                tipo_credito=lambda x: self._normalize_series(x.tipo_credito),
                proveedor_credito=lambda x: self._normalize_series(x.proveedor_credito),
                tipo_inicio_flujo_credito=lambda x: self._normalize_series(
                    x.tipo_inicio_flujo_credito
                ),
                motivo_cierre_credito=lambda x: self._normalize_series(
                    x.motivo_cierre_credito
                ),
            )
        )
        return solicitudes

    def proc_historico_casos(self, rawdf):
        """
        Normalize the Salesforce Historico Casos export.

        Adds cleaned versions of field, old_value, and new_value so assignment
        events can be detected reliably despite accents/case differences.
        """
        historico = (
            self._rename(rawdf, "historico_casos")
            .assign(
                created_date=lambda x: self._to_datetime_str(
                    x.created_date,
                    fmt_string="%Y-%m-%d %H:%M",
                ),
                field=lambda x: self._normalize_series(x.field),
                old_value=lambda x: self._normalize_series(x.old_value),
                new_value=lambda x: self._normalize_series(x.new_value),
            )
        )
        return historico

    def proc_historico_citas(self, rawdf):
            """
            Normalize the Salesforce Historico Citas export.
    
            Adds cleaned versions of field, old_value, and new_value so assignment
            events can be detected reliably despite accents/case differences.
            """
            historico = (
                self._rename(rawdf, "historico_citas")
                .assign(
                    created_date=lambda x: self._to_datetime_str(
                        x.created_date,
                        fmt_string="%Y-%m-%d %H:%M",
                    ),
                    field=lambda x: self._normalize_series(x.field),
                    old_value=lambda x: self._normalize_series(x.old_value),
                    new_value=lambda x: self._normalize_series(x.new_value),
                    created_by = lambda x: self._normalize_series(x.created_by).str.upper()
                )
            )
            return historico

    def proc_historico_oportunidades(self, rawdf):
        """
        Normalize the Salesforce Historico Oportunidades export.

        Adds cleaned versions of field, old_value, and new_value so assignment
        events can be detected reliably despite accents/case differences.
        """
        historico = (
            self._rename(rawdf, "historico_oportunidades")
            .assign(
                created_date=lambda x: self._to_datetime_str(
                    x.created_date,
                    fmt_string="%Y-%m-%d %H:%M",
                ),
                field=lambda x: self._normalize_series(x.field),
                old_value=lambda x: self._normalize_series(x.old_value),
                new_value=lambda x: self._normalize_series(x.new_value),
            )
        )
        return historico

    def proc_catalogo_usuarios(self,rawdf):
        """
        Normalize the Salesforce Catalogo de Usuarios export.

        Adds cleaned versions of field, old_value, and new_value so assignment
        events can be detected reliably despite accents/case differences.
        """
        catalogo = (
            self._rename(rawdf, "catalogo_usuarios")
            .assign(equipo= lambda x: self._normalize_series(x.equipo),
         division = lambda x: self._normalize_series(x.division),
         name = lambda x: self._normalize_series(x.name).str.upper(),
        #  email = lambda x: ProcessedCrmAtlas._normalize_series(x.email)
         )
        )
        return catalogo

    
##############################################################################################################
################################################ REPORTES ####################################################
##############################################################################################################
    def proc_reporte_oportunidades(
        self,
        oportunidades,
        pedidos,
        casos,
        citas,
        solicitudes_credito,
        historico_casos,
        catalogo_usuarios,
        historico_oportunidades,
        historico_citas
    ):
        """
        Build the CRM opportunities consumption report.

        This report enriches oportunidades with:
        - Contact-center and credit profiling case IDs/statuses.
        - First assignment owner/date inferred from Historico Casos.
        - Credit origin classification from Solicitudes Credito.
        - Pedido counts and open-pedido counts.
        - Buyer appointment counts, first/last appointment dates, and completed
          appointment counts.

        Inputs are already-normalized DataFrames from the proc_* methods above.
        """
        casos_perfilamiento_sc = (
            casos[lambda x: x.case_subject.eq(
                "perfilamiento contact center")]
            .rename(
                columns={
                    "case_id": "case_id_perfilamiento_sc",
                    "case_status": "case_status_perfilamiento_sc",
                }
            )
        )

        casos_perfilamiento_credito = (
            casos[lambda x: x.case_subject.eq("perfilamiento credito")]
            .rename(
                columns={
                    "case_id": "case_id_perfilamiento_credito",
                    "case_status": "case_status_perfilamiento_credito",
                }
            )
        )

        asesor_perfilamiento_sc = (
            historico_casos[
                lambda x: x.case_id.isin(
                    casos_perfilamiento_sc["case_id_perfilamiento_sc"].dropna(
                    ).unique()
                )
            ]
            [
                lambda x: x.field.eq("status")
                & x.old_value.eq("abierto")
                & x.new_value.eq("in progress")
            ]
            .sort_values(by="created_date")
            .drop_duplicates(subset="case_id", keep="first")
            .assign(
                fecha_caso_tomado_sc=lambda x: pd.to_datetime(
                    x.created_date,
                    errors="coerce",
                ).dt.strftime("%Y-%m-%d %H:%M:%S"),
                asesor_caso_tomado_perf_sc=lambda x: x.created_by.str.upper(),
            )
            .rename(columns={"case_id": "case_id_perfilamiento_sc",
                             "created_by_id": "asesor_perfilamiento_sc_id"}
                    )
            [["case_id_perfilamiento_sc", "asesor_perfilamiento_sc_id",
                "asesor_caso_tomado_perf_sc", "fecha_caso_tomado_sc"]]
        )

        asesor_perfilamiento_credito = (
            historico_casos[
                lambda x: x.case_id.isin(
                    casos_perfilamiento_credito[
                        "case_id_perfilamiento_credito"
                    ].dropna().unique()
                )
            ]
            [
                lambda x: x.field.eq("status")
                & x.old_value.eq("abierto")
                & x.new_value.eq("in progress")
            ]
            .sort_values(by="created_date")
            .drop_duplicates(subset="case_id", keep="first")
            .assign(
                fecha_asignacion_perfilamiento_credito=lambda x: pd.to_datetime(
                    x.created_date,
                    errors="coerce",
                ).dt.strftime("%Y-%m-%d %H:%M:%S"),
                asesor_perfilamiento_credito=lambda x: x.created_by.str.upper(),
            )
            .rename(columns={"case_id": "case_id_perfilamiento_credito",
                             "created_by_id": "asesor_perfilamiento_credito_id"
                             })
            [
                [
                    "case_id_perfilamiento_credito",
                    "asesor_perfilamiento_credito",
                    "asesor_perfilamiento_credito_id",
                    "fecha_asignacion_perfilamiento_credito",
                ]
            ]
        )

        origen_credito_aux = (
            solicitudes_credito
            .sort_values(by=["opportunity_id", "created_date"], ascending=[True, True])
            .drop_duplicates(subset="opportunity_id", keep="first")
            [lambda x: x.tipo_conclusion_flujo_credito == 'flujo contingente']
            .assign(opportunity_source_aux="credito am contingencia")
            [["opportunity_id", "opportunity_source_aux"]]
        )

        citas_comprador = (
            citas[lambda x: x.opportunity_id.notna()]
            .rename(columns={"status": "status_cita"})
            .merge(
                pedidos[["commerce_order_id", "sf_order_id", "status"]],
                on="commerce_order_id",
                how="left",
            )
            .rename(columns={"status": "status_pedido"})
            [lambda x: x.work_type_name.isin(CRM_WORK_TYPES_COMPRADOR)]
            .sort_values(by=["opportunity_id", "created_date"], ascending=[False, False])
            .assign(
                citas_completas=lambda x: x.status_cita.isin(
                    CRM_STATUS_CITA_COMPLETA
                ).multiply(1),
                fecha_agendada=lambda x: pd.to_datetime(
                    x.sched_start_time,
                    errors="coerce",
                ).dt.strftime("%Y-%m-%d").fillna(''),
            )
        )

        summary_citas_comprador = (
            citas_comprador
            
            .groupby("opportunity_id", as_index=False)
            .agg(
                numero_citas_comprador=("created_date", "nunique"),
                fecha_primera_cita_visita_comp=("fecha_agendada", "min"),
                fecha_ultima_cita_visita_comp=("fecha_agendada", "max"),
                citas_completas_comprador=("citas_completas", "sum"),
            )
            .assign(
                citas_completas=lambda x: x[
                    ["numero_citas_comprador", "citas_completas_comprador"]
                ].min(axis=1)
            )
            .drop(columns=["citas_completas_comprador"])
        )

        summary_pedidos = (
            pedidos
            .assign(
                pedidos_abiertos=lambda x: x.status.isin(
                    CRM_STATUS_PEDIDOS_ABIERTOS
                ).multiply(1)
            )
            .groupby("opportunity_id", as_index=False)
            .agg(
                numero_pedidos=("sf_order_id", "nunique"),
                numero_pedidos_abiertos=("pedidos_abiertos", "sum"),
                fecha_primer_pedido=("created_date", "min"),
                fecha_ultimo_pedido=("created_date", "max"),
            )
        )
        summary_solicitudes = (solicitudes_credito
                    .assign(flag_contingencia=lambda x: x.tipo_conclusion_flujo_credito.eq('flujo contingente').multiply(1),
                            flag_folio = lambda x: x.folio.notna().multiply(1),
                            flag_aceptada = lambda x: (~x.status_solicitud.str.startswith('recha') & x.flag_folio.eq(1)).multiply(1))
                    .groupby('opportunity_id',as_index = False)
                    .agg(
                        n_simulaciones_credito=('simulation_name','nunique'),
                        n_simulaciones_con_folio=('flag_folio','sum'),
                        n_simulaciones_preaceptadas = ('flag_aceptada','sum'),
                        n_simulaciones_contingencia = ('flag_contingencia','sum')
                        )
                        
                    )
        historico_oportunidades_mod = (historico_oportunidades
                                .sort_values(by=['opportunity_id','created_date'],ascending=[False,False])
                                [lambda x: x.new_value=='cerrada (perdida)']
                                .drop_duplicates('opportunity_id')
                                .rename(columns={'old_value':'stage_antes_de_cierre',
                                                'created_by':'usuario_que_cerro'})
                                [['opportunity_id','stage_antes_de_cierre','usuario_que_cerro']]
                                )

        citas_comprador_aux = (
            citas_comprador
            .sort_values(by=['opportunity_id','created_date'],ascending=[False,True])
            .drop_duplicates(subset=['opportunity_id'],keep='first')
            [['opportunity_id','numero_cita']]
            )
        primer_booker_aux = (historico_citas
            [lambda x: x.numero_cita.isin(citas_comprador_aux.numero_cita.unique())]
            [lambda x: x.field=='status']
            [lambda x: x.old_value.isna() & (x.new_value=='scheduled')]
            .sort_values(by=['numero_cita','created_date'],ascending=[False,True])
            .drop_duplicates(subset=['numero_cita'],keep='first')
            .merge(catalogo_usuarios[['id','equipo']],left_on='created_by_id',right_on='id',how='left')
            .drop(columns=['id'])
            .rename(columns={'created_by':'primer_booker_comprador',
                            'equipo':'equipo_primer_booker_comprador'
                            })
            [['numero_cita','primer_booker_comprador','equipo_primer_booker_comprador']]
            )
        primer_booker_df = (citas_comprador_aux
            .merge(primer_booker_aux,on='numero_cita',how='left')
            .drop(columns=['numero_cita'])
            )
        reporte = (
            oportunidades
            .merge(catalogo_usuarios[['id', 'equipo']], left_on='owner_id', right_on='id', how='left')
            .rename(columns={'equipo': 'opportunity_owner_equipo',
                             })
            .drop(columns=["id"])
            .merge(origen_credito_aux, on="opportunity_id", how="left")
            .merge(
                casos_perfilamiento_sc[
                    [
                        "opportunity_id",
                        "case_id_perfilamiento_sc",
                        "case_status_perfilamiento_sc",
                        "case_owner_name_perf_sc",
                        "case_owner_id_perf_sc",
                    ]
                ],
                on="opportunity_id",
                how="left",
            )
            .merge(catalogo_usuarios[['id', 'equipo']], left_on='case_owner_id_perf_sc', right_on='id', how='left')
            .rename(columns={'equipo': 'case_owner_equipo_perf_sc'})
            .drop(columns=["id"])
            .merge(
                casos_perfilamiento_credito[
                    [
                        "opportunity_id",
                        "case_id_perfilamiento_credito",
                        "case_status_perfilamiento_credito",
                    ]
                ],
                on="opportunity_id",
                how="left",
            )
            .merge(asesor_perfilamiento_sc, on="case_id_perfilamiento_sc", how="left")
            .merge(catalogo_usuarios[['id', 'equipo']], left_on='asesor_perfilamiento_sc_id', right_on='id', how='left')
            .rename(columns={'equipo': 'equipo_asesor_caso_tomado_perf_sc',
                             })
            .drop(columns=["id"])
            .merge(
                asesor_perfilamiento_credito,
                on="case_id_perfilamiento_credito",
                how="left",
            )
            .merge(catalogo_usuarios[['id', 'equipo']], left_on='asesor_perfilamiento_credito_id', right_on='id', how='left')
            .rename(columns={'equipo': 'equipo_asesor_perfilamiento_credito',
                             })
            .drop(columns=["id"])
            .merge(summary_pedidos, on="opportunity_id", how="left")
            .merge(summary_solicitudes, on="opportunity_id", how="left")
            .merge(summary_citas_comprador, on="opportunity_id", how="left")
            .assign(
                opportunity_source_aux_1=lambda x: np.where(
                    (x.opportunity_source == ("credito am"))
                    & x.opportunity_source_aux.isna(),
                    "credito am api",
                    x.opportunity_source_aux,
                ),
                opportunity_source_aux=lambda x: np.where(x.opportunity_source == (
                    "apartado am"), np.nan, x.opportunity_source_aux_1),
                flag_caso_tomado_perf_sc=lambda x: x.fecha_caso_tomado_sc.notna()
                * 1,
                flag_perfilamento_credito=lambda x: (
                    x.fecha_asignacion_perfilamiento_credito.notna()
                )
                * 1,
                opportunity_created_date_day = lambda x: pd.to_datetime(x.opportunity_created_date, errors='coerce').dt.strftime('%Y-%m-%d'),
            )
            .merge(historico_oportunidades_mod, on='opportunity_id', how='left')
            .merge(primer_booker_df, on='opportunity_id', how='left')
            .drop(columns=["opportunity_source_aux_1"])
        )

        reporte = (
            reporte
            .sort_values(
                "fecha_asignacion_perfilamiento_credito",
                ascending=False,
                na_position="last",
            )
            .drop_duplicates(subset="opportunity_id", keep="first")
        )

        return self._select_existing_columns(
            reporte,
            self._dedupe_columns(CRM_REPORTE_OPORTUNIDADES_COLUMNS),
        )

    def proc_reporte_citas(
        self,
        citas_proc,
        oppss_proc,
        pedidos_proc,
        usuarios_proc,
        acclientes,
        hcitas_proc
    ):
        print('lineas iniciales en citas: ',len(citas_proc))
        # cuentas
        acclientes = acclientes.rename(columns={'nickname': 'nombre',
                                                     'phone': 'telefono'})
        acclientes['id_am'] = (pd.to_numeric(acclientes['id_am'], errors='coerce').astype('Int64'))

        # pedidos
        pedidos_proc = pedidos_proc[['commerce_order_id', 'id_am_vendedor', 'id_am_comprador', 'sf_order_id']].rename(
            columns={'id_am_vendedor': 'id_am_vendedor_aux', 'id_am_comprador': 'id_am_comprador_aux'})
        pedidos_proc['commerce_order_id'] = pd.to_numeric(
            pedidos_proc['commerce_order_id'], errors='coerce').astype('Int64')

        # oportunidades
        oppss_proc = oppss_proc[['opportunity_id', 'owner_id', 'opportunity_owner',
                                 'perf_bc_score', 'perf_intencion_pago', 'opportunity_stage']]

        # usuarios
        usuarios_proc = usuarios_proc.rename(
            columns={'id': 'id_usuario', 'name': 'nombre_usuario'}
        )
        usuarios_proc = usuarios_proc[[
            'id_usuario', 'nombre_usuario', 'equipo']]

        # historico de citas
        hcitas_proc = (hcitas_proc[lambda x: x['field']=='status'].
            rename(columns = {'created_by_id':'booker_id', 'created_by': 'booker_name'})
                    .reset_index(drop=True)
        )

        # generamos reporte consumible de citas
        citas_cons = citas_proc.copy().rename(
            columns={
                'created_date':'created_time',
                'territorio_cita': 'espacio_cita',
                'fecha_checkin':'check_in',
                'fecha_checkout':'check_out'}
        ).assign(
            rol = lambda x: np.select(
                                    [x['work_type_name'].str.lower().isin(CRM_WORK_TYPES_COMPRADOR), x['work_type_name'].str.lower().isin(CRM_WORK_TYPES_VENDEDOR)],
                                    ['comprador', 'vendedor'],
                                    default=''),
            created_date = lambda x: pd.to_datetime(x.created_time).dt.strftime('%Y-%m-%d')
        )

        # agregamos atributos del id de la persona referida en la linea de cita
        citas_cons['id_am'] = pd.to_numeric(citas_cons['id_am'], errors='coerce').astype('Int64')
        citas_cons = citas_cons.merge(
            acclientes[['id_am', 'nombre', 'email', 'telefono']], how='left', on='id_am')

        # agregamos rol comprador o vendedor
        citas_cons['commerce_order_id'] = pd.to_numeric(citas_cons['commerce_order_id'], errors='coerce').astype('Int64')
        citas_cons = citas_cons.merge(
            pedidos_proc, how='left', on='commerce_order_id'
        ).assign(
            rol_2=lambda x: np.select(
                [x['id_am'].eq(x['id_am_vendedor_aux']).fillna(False).to_numpy(dtype=bool),
                 x['id_am'].eq(x['id_am_comprador_aux']).fillna(False).to_numpy(dtype=bool)],
                ['vendedor',
                 'comprador'],
                default='desconocido'
            )

        ).drop(columns=['id_am_vendedor_aux', 'id_am_comprador_aux'])

        # agregamos id y nombre de owner, bc score y perfilamiento de sc y cc a partir del reporte de oportunidades
        citas_cons = citas_cons.merge(
            oppss_proc, how='left', on='opportunity_id'
        ).rename(columns={'owner_id': 'opportunity_owner_id'})

        # agregamos booker y booker id
        citas_cons = (citas_cons
                    .merge(hcitas_proc
                                .loc[lambda x: x['old_value'].isna()]
                                .loc[lambda x: x['new_value']=='scheduled']
                                .sort_values(by=['numero_cita', 'created_date'],ascending=[True, True])
                                .drop_duplicates(subset = ['numero_cita'], keep = 'first')
                                [['numero_cita', 'booker_id', 'booker_name']], 
                            how = 'left',
                            on = 'numero_cita'))

        # agregamos equipo del owner y del booker a partir del catálogo de usuarios
        citas_cons = (citas_cons
                    .merge(usuarios_proc, how = 'left', left_on='opportunity_owner_id', right_on = 'id_usuario')
                    .drop(columns = ['id_usuario', 'nombre_usuario']).rename(columns = {'equipo':'opportunity_owner_equipo'})
                    .merge(usuarios_proc, how = 'left', left_on = 'booker_id', right_on = 'id_usuario')
                    .drop(columns = ['id_usuario', 'nombre_usuario']).rename(columns = {'equipo':'booker_equipo'})
                     )

        # etiquetamos citas dummies
        citas_cons = (citas_cons.assign(
                                    flag_cita_agendada_comprador = (~(citas_cons.opportunity_id.notna() & citas_cons.booker_name.isna())
                                                                    &(citas_cons.rol.fillna('').isin(['','comprador']) & citas_cons.work_type_name.fillna('').isin(['','cita inicial visita comprador']))
                                                                    &(~citas_cons.duplicated(subset=['work_type_name','sf_order_id','created_date']))
                                                                   )*1
                                    )
                              .sort_values(by='numero_cita', ascending=False)
                     )        
        
        # agregamos etiqueta de show y status previo en historico
        citas_cons = (citas_cons
                        .merge(hcitas_proc
                                .loc[lambda x: x.new_value.isin(CRM_CRITERIOS_HISTSHOW_CITAS)]
                                .assign(kpi_citas_flag_histshow_compr = 1)
                                .drop_duplicates(subset=['numero_cita'])
                                .rename(columns = {'created_date':'kpi_citas_fecha_histshow_compr','old_value':'status_old_value','new_value':'status_new_value'})
                                [['numero_cita','status_old_value','status_new_value','kpi_citas_flag_histshow_compr','kpi_citas_fecha_histshow_compr']],
                        on='numero_cita',
                        how='left'
                        ).assign(
                            flag_cita_show_comprador = lambda x: (x.flag_cita_agendada_comprador & x.status.isin(CRM_CRITERIOS_SHOW_CITAS))*1,
                            kpi_citas_flag_histshow_compr = lambda x: x.kpi_citas_flag_histshow_compr.fillna(0),
                            sched_start_date = lambda x: pd.to_datetime(x.sched_start_time).dt.strftime('%Y-%m-%d'),
                            sched_end_date = lambda x: pd.to_datetime(x.sched_end_time).dt.strftime('%Y-%m-%d')
                        ))
        print('lineas finales en citas: ',len(citas_cons))

        return self._select_existing_columns(
            citas_cons,
            self._dedupe_columns(CRM_REPORTE_CITAS_COLUMNS),
        )



    def proc_reporte_simulaciones(self,solicitudes_credito):
        return self._select_existing_columns(
            solicitudes_credito,
            self._dedupe_columns(CRM_REPORTE_SIMULACIONES_COLUMNS)
        )

    def proc_reporte_historico_oportunidades(self, historico_oportunidades, oportunidades):
        """
        """
        reporte = (historico_oportunidades
            [lambda x: x.field=='stagename']
            .sort_values(by=['opportunity_id','created_date'],ascending=[True,True])
            .assign(created_by = lambda x: x.created_by.str.upper())
            [['opportunity_id','old_value','new_value','created_by','created_date']]
            .merge(oportunidades[['opportunity_id','opportunity_name']],on='opportunity_id',how='left')
            ) 
        return reporte



    def _validate_sales_center_kpi_input(self, reporte_oportunidades):
        """Require the canonical opportunities report grain and KPI inputs."""
        missing_columns = self.SALES_CENTER_KPI_REQUIRED_COLUMNS.difference(
            reporte_oportunidades.columns
        )
        if missing_columns:
            raise ValueError(
                "reporte_oportunidades: faltan columnas requeridas para KPIs "
                f"Sales Center: {sorted(missing_columns)}"
            )

        duplicate_ids = reporte_oportunidades["opportunity_id"].duplicated(
            keep=False
        )
        if duplicate_ids.any():
            sample_ids = (
                reporte_oportunidades.loc[duplicate_ids, "opportunity_id"]
                .dropna()
                .astype(str)
                .unique()[:10]
                .tolist()
            )
            raise ValueError(
                "No se pueden calcular KPIs Sales Center con opportunity_id "
                f"duplicados. Ejemplos: {sample_ids}"
            )

    def add_kpis_perfilamiento_sales_center(self, reporte_oportunidades):
        """
        Add Sales Center profiling flags and activity dates to opportunities.

        Assignment is identified when either the current owner of the Contact
        Center profiling case or the advisor who took that case belongs to a
        configured Sales Center team.
        """
        self._validate_sales_center_kpi_input(reporte_oportunidades)

        equipos_sales_center = [
            self._normalize_value(equipo)
            for equipo in CRM_EQUIPOS_SALES_CENTER
        ]
        
        reporte = reporte_oportunidades.copy()
        fecha_asignacion = pd.to_datetime(reporte["fecha_asignacion"],dayfirst=True).dt.strftime('%Y-%m-%d')
        fecha_caso_tomado = pd.to_datetime(reporte["fecha_caso_tomado_sc"]).dt.strftime('%Y-%m-%d')
        fecha_primer_contacto = (pd.to_datetime(
                                    reporte["perf_fecha_primer_contacto"],
                                    dayfirst=True
                                ).dt.strftime('%Y-%m-%d')
                                )
        resultado = (reporte.assign(
            fecha_asignacion=fecha_asignacion,
            perf_intencion_pago = lambda x: self._normalize_series(x.perf_intencion_pago),
            kpi_sales_center_flag_asignado=lambda x: (
                x.case_owner_equipo_perf_sc.isin(equipos_sales_center)
                | x.equipo_asesor_caso_tomado_perf_sc.isin(
                    equipos_sales_center
                )
            ).astype(int),
            kpi_sales_center_fecha_asignado=lambda x: (
                x.fecha_asignacion.fillna(fecha_caso_tomado).fillna(
                    fecha_primer_contacto
                )
            ),
            kpi_sales_center_flag_contactado=lambda x: (
                x.kpi_sales_center_flag_asignado.eq(1)
                & x.perf_contactado.eq('si')
            ).astype(int),
            kpi_sales_center_flag_intento_contacto=lambda x: (
                x.kpi_sales_center_flag_asignado.eq(1)
                & fecha_primer_contacto.notna()
            ).astype(int),
            kpi_sales_center_fecha_contactado=lambda x: pd.Series(
                np.where(
                    x.kpi_sales_center_flag_contactado.eq(1)
                    | x.kpi_sales_center_flag_intento_contacto.eq(1),
                    fecha_primer_contacto.fillna(fecha_caso_tomado),
                    np.nan,
                ),
                index=x.index,
                dtype="object",
            ),
            kpi_sales_center_flag_interesado=lambda x: (
                x.kpi_sales_center_flag_asignado.eq(1)
                & x.kpi_sales_center_flag_contactado.eq(1)
                & x.perf_interesado.eq('si')
            ).astype(int),
            kpi_sales_center_flag_perfilado=lambda x: (
                x.kpi_sales_center_flag_asignado.eq(1)
                & x.kpi_sales_center_flag_contactado.eq(1)
                & x.perf_intencion_pago.notna()
                & x.kpi_sales_center_flag_interesado.eq(1)
            ).astype(int),
            kpi_sales_center_flag_perfilado_credito=lambda x: (
                x.kpi_sales_center_flag_perfilado.eq(1)
                & x.perf_intencion_pago.eq("credito")
            ).astype(int),
            kpi_sales_center_flag_perfilado_contado=lambda x: (
                x.kpi_sales_center_flag_perfilado.eq(1)
                & x.perf_intencion_pago.eq("contado")
            ).astype(int),
        ))
        return resultado


    def vista_actividad_kpis_perfilamiento_sales_center(
        self, reporte_oportunidades
    ):
        """Aggregate Sales Center assignment and profiling activity by date."""
        kpi_sc = self.add_kpis_perfilamiento_sales_center(
            reporte_oportunidades
        )

        sc_asignados = (
            kpi_sc.groupby(
                "kpi_sales_center_fecha_asignado", as_index=False
            )
            .kpi_sales_center_flag_asignado.sum()
            .rename(
                columns={
                    "kpi_sales_center_fecha_asignado": "fecha_actividad"
                }
            )
        )
        metric_columns = [
            "kpi_sales_center_flag_intento_contacto",
            "kpi_sales_center_flag_contactado",
            "kpi_sales_center_flag_interesado",
            "kpi_sales_center_flag_perfilado",
            "kpi_sales_center_flag_perfilado_credito",
            "kpi_sales_center_flag_perfilado_contado",
        ]
        sc_contactados_perfilados = (
            kpi_sc.groupby(
                "kpi_sales_center_fecha_contactado", as_index=False
            )[metric_columns]
            .sum()
            .rename(
                columns={
                    "kpi_sales_center_fecha_contactado": "fecha_actividad"
                }
            )
        )

        return (
            sc_asignados.merge(
                sc_contactados_perfilados,
                on="fecha_actividad",
                how="outer",
            )
            .fillna(0)
            .sort_values("fecha_actividad")
            .reset_index(drop=True)
        )
    
##########################################################################################################################################
####################################################### PIPELINE  ########################################################################

    def _run_logged_processor(self, logger, step, table_name, func, rawdf):
        """
        Execute one proc_* method with consistent log events and error context.

        This wrapper keeps build_consumo_outputs readable while still recording
        input/output row counts and the exact table that failed.
        """
        if logger:
            logger.info(
                f"consumo.{step}.start",
                table_name=table_name,
                input_rows=len(rawdf),
                input_columns=len(rawdf.columns),
            )
        try:
            result = func(rawdf)
        except Exception as e:
            if logger:
                logger.error(
                    f"consumo.{step}.failed",
                    e,
                    table_name=table_name,
                    input_rows=len(rawdf),
                    input_columns=len(rawdf.columns),
                )
            raise
        if logger:
            logger.success(
                f"consumo.{step}.done",
                table_name=table_name,
                output_rows=len(result),
                output_columns=len(result.columns),
            )
        return result

    def build_consumo_outputs(self, raw_dfs, extra_dfs, logger=None):
        """
        Build all CRM consumption/intermediate outputs from raw DataFrames.

        Parameters
        ----------
        raw_dfs : dict
            Raw DataFrames keyed by CRM table name, typically from
            RawCrmAtlas.read_local_sources.
        extra_dfs : dict
            Existing Atlas Consumo DataFrames required to enrich CRM reports.
            Currently requires ``AcClientes`` for customer details in the
            appointments report.
        logger : CrmRunLogger, optional
            Captures one log event per table transform and report build.

        Returns
        -------
        dict
            Normalized intermediate tables plus final consumption outputs such
            as reporte_oportunidades and solicitudes_credito.
        """
        if "AcClientes" not in extra_dfs:
            raise ValueError(
                "Falta la dependencia externa 'AcClientes' para construir reporte_citas."
            )
        missing_client_columns = {
            "id_am", "nickname", "phone", "email"
        }.difference(extra_dfs["AcClientes"].columns)
        if missing_client_columns:
            raise ValueError(
                "AcClientes: faltan columnas requeridas: "
                f"{sorted(missing_client_columns)}"
            )

        pedidos = self._run_logged_processor(
            logger,
            "proc_pedidos",
            "pedidos",
            self.proc_pedidos,
            raw_dfs["pedidos"],
        )
        oportunidades = self._run_logged_processor(
            logger,
            "proc_oportunidades",
            "oportunidades",
            self.proc_oportunidades,
            raw_dfs["oportunidades"],
        )
        casos = self._run_logged_processor(
            logger,
            "proc_casos",
            "casos",
            self.proc_casos,
            raw_dfs["casos"],
        )
        citas = self._run_logged_processor(
            logger,
            "proc_citas",
            "citas",
            self.proc_citas,
            raw_dfs["citas"],
        )
        solicitudes_credito = self._run_logged_processor(
            logger,
            "proc_solicitudes_credito",
            "solicitudes_credito",
            self.proc_solicitudes_credito,
            raw_dfs["solicitudes_credito"],
        )
        historico_casos = self._run_logged_processor(
            logger,
            "proc_historico_casos",
            "historico_casos",
            self.proc_historico_casos,
            raw_dfs["historico_casos"],
        )
        historico_citas = self._run_logged_processor(
                    logger,
                    "proc_historico_citas",
                    "historico_citas",
                    self.proc_historico_citas,
                    raw_dfs["historico_citas"],
                )
        historico_oportunidades = self._run_logged_processor(
                    logger,
                    "proc_historico_oportunidades",
                    "historico_oportunidades",
                    self.proc_historico_oportunidades,
                    raw_dfs["historico_oportunidades"],
                )

        catalogo_usuarios = self._run_logged_processor(
            logger,
            "proc_catalogo_usuarios",
            "catalogo_usuarios",
            self.proc_catalogo_usuarios,
            raw_dfs["catalogo_usuarios"])

        # cuentas =  
        if logger:
            logger.info("consumo.proc_reporte_oportunidades.start")
        try:
            reporte_oportunidades = self.proc_reporte_oportunidades(
                oportunidades=oportunidades,
                pedidos=pedidos,
                casos=casos,
                citas=citas,
                solicitudes_credito=solicitudes_credito,
                historico_casos=historico_casos,
                catalogo_usuarios=catalogo_usuarios,
                historico_oportunidades=historico_oportunidades,
                historico_citas=historico_citas
            )
        except Exception as e:
            if logger:
                logger.error("consumo.proc_reporte_oportunidades.failed", e)
            raise
        if logger:
            logger.success(
                "consumo.proc_reporte_oportunidades.done",
                output_rows=len(reporte_oportunidades),
                output_columns=len(reporte_oportunidades.columns),
            )

        if logger:
            logger.info("consumo.proc_reporte_simulaciones.start")
        try:
            reporte_simulaciones = self.proc_reporte_simulaciones(
                solicitudes_credito=solicitudes_credito
            )
        except Exception as e:
            if logger:
                logger.error("consumo.proc_reporte_simulaciones.failed", e)
            raise
        if logger:
            logger.success(
                "consumo.proc_reporte_simulaciones.done",
                output_rows=len(reporte_simulaciones),
                output_columns=len(reporte_simulaciones.columns),
            )

        if logger:
            logger.info("consumo.proc_reporte_historico_oportunidades.start")
        try:
            reporte_historico_oportunidades = self.proc_reporte_historico_oportunidades(
                historico_oportunidades=historico_oportunidades,
                oportunidades=oportunidades
            )
        except Exception as e:
            if logger:
                logger.error("consumo.proc_reporte_historico_oportunidades.failed", e)
            raise
        if logger:
            logger.success(
                "consumo.proc_reporte_simulaciones.done",
                output_rows=len(reporte_historico_oportunidades),
                output_columns=len(reporte_historico_oportunidades.columns),
            )            

        if logger:
            logger.info("consumo.proc_reporte_citas.start")
        try:
            reporte_citas = self.proc_reporte_citas(
                citas_proc=citas,
                oppss_proc=oportunidades,
                pedidos_proc=pedidos,
                usuarios_proc=catalogo_usuarios,
                acclientes=extra_dfs["AcClientes"],
                hcitas_proc=historico_citas,
            )
        except Exception as e:
            if logger:
                logger.error("consumo.proc_reporte_citas.failed", e)
            raise
        if logger:
            logger.success(
                "consumo.proc_reporte_citas.done",
                output_rows=len(reporte_citas),
                output_columns=len(reporte_citas.columns),
            )            

        return {
            "pedidos": pedidos,
            "oportunidades": oportunidades,
            "casos": casos,
            "citas": citas,
            "catalogo_usuarios": catalogo_usuarios,
            "historico_casos": historico_casos,
            "solicitudes_credito": solicitudes_credito,
            "historico_citas": historico_citas,
            "historico_oportunidades": historico_oportunidades,
            "reporte_simulaciones": reporte_simulaciones,
            "reporte_oportunidades": reporte_oportunidades,
            "reporte_historico_oportunidades": reporte_historico_oportunidades,
            "reporte_citas": reporte_citas,
        }
