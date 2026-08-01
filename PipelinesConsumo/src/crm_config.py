"""
Configuration for the Salesforce CRM ingestion into Atlas Consumo.

The dictionaries live here so field changes from Salesforce can be reviewed in
one place, instead of being hidden inside notebook cells.
"""

CRM_SOURCE_FOLDER_ID = "17jg82rYHkuGf2Vbx_HIvEWNjQuRZvulv"

# Set this from the notebook or constants once the traceability folder exists.
CRM_RAW_SNAPSHOT_FOLDER_ID = "1m5LMB1ushE6VzhbuRNBJV9OE6TSDbcBm"

# Optional folder for the "latest" raw CRM snapshot. Files here keep stable
# names like RawCrmPedidos.csv and are overwritten/upserted on each run.
CRM_RAW_LATEST_FOLDER_ID = "1GIjtJ52Epb_aYXGoCE-QFXTnEreptQiE"

# Optional folder for timestamped TXT execution logs. If None, the pipeline
# functions can receive log_folder_id explicitly from the notebook.
CRM_LOG_FOLDER_ID = "1N3cUWCQ1B8CmaY8loaaGl81NOWjpboTD"

CRM_SHEET_NAME = "Sheet1"

CRM_SOURCE_FILES = {
    "pedidos": {
        "source_file": "Pedidos_2.xlsx",
        "sheet_name": CRM_SHEET_NAME,
        "raw_snapshot_name": "RawCrmPedidos",
    },
    "oportunidades": {
        "source_file": "Oportunidades_2.xlsx",
        "sheet_name": CRM_SHEET_NAME,
        "raw_snapshot_name": "RawCrmOportunidades",
    },
    "casos": {
        "source_file": "Casos_2.xlsx",
        "sheet_name": CRM_SHEET_NAME,
        "raw_snapshot_name": "RawCrmCasos",
    },
    "citas": {
        "source_file": "Citas_2.xlsx",
        "sheet_name": CRM_SHEET_NAME,
        "raw_snapshot_name": "RawCrmCitas",
    },
    "solicitudes_credito": {
        "source_file": "Creditos_2.xlsx",
        "sheet_name": CRM_SHEET_NAME,
        "raw_snapshot_name": "RawCrmSolicitudesCredito",
    },
    "historico_casos": {
        "source_file": "Historico casos_2.xlsx",
        "sheet_name": CRM_SHEET_NAME,
        "raw_snapshot_name": "RawCrmHistoricoCasos",
    },
    "historico_citas": {
        "source_file": "Historico citas_2.xlsx",
        "sheet_name": CRM_SHEET_NAME,
        "raw_snapshot_name": "RawCrmHistoricoCitas",
    },
}

CRM_RENAME_DICTS = {
    "pedidos": {
        "OrderNumber": "sf_order_id",
        "MX_ATN_CommerceId__c": "commerce_order_id",
        "AccountId": "account_id_sf",
        "Account.Name": "nombre_vendedor",
        "Account.MX_ATN_CommerceId__c": "id_am_vendedor",
        "Account.MX_ATN_Status__c": "status_vendedor",
        "MX_ATN_BuyerId__r.Name": "nombre_comprador",
        "MX_ATN_BuyerId__r.MX_ATN_CommerceId__c": "id_am_comprador",
        "Status": "status",
        "TotalAmount": "total_amount",
        "CreatedDate": "created_date",
        "MX_ATN_Anticipo__c": "anticipo",
        "MX_ATN_AssetId__r.Product2.Name": "descripcion_auto",
        "MX_ATN_AssetId__r.MX_ATN_VehiclesId__r.MX_ATN_ChassisNumber__c": "vin",
        "OpportunityId": "opportunity_id",
        "Opportunity.Name": "opportunity_name",
    },
    "oportunidades": {
        "Id": "opportunity_id",
        "Name": "opportunity_name",
        "CreatedDate": "opportunity_created_date",
        "MX_ATN_Fecha_Asignacion__c": "fecha_asignacion",
        "Owner.Name": "opportunity_owner",
        "OwnerId": "owner_id",
        "Account.Name": "nombre_comprador",
        "Account.MX_ATN_CommerceId__c": "id_am_comprador",
        "LastModifiedDate": "last_modified_date",
        "LeadSource": "opportunity_source",
        "MX_ATN_Puntaje_Buro__c": "perf_bc_score",
        "MX_ATN_Intencion_Pago__c": "perf_intencion_pago",
        "MX_ATN_Contactado__c": "perf_contactado",
        "MX_ATN_Cliente_Interesado__c": "perf_interesado",
        "MX_ATN_Comentarios_Perfilamiento__c": "perf_comentarios",
        "MX_ATN_Canal_Respuesta_Contacto__c": "perf_canal_respuesta",
        "MX_ATN_Fecha_Primer_Contacto__c": "perf_fecha_primer_contacto",
        "MX_ATN_Fecha_Ultimo_Contacto__c": "perf_fecha_ultimo_contacto",
        "MX_ATN_Contacto_Otro_Momento__c": "perf_contacto_otro_momento",
        "MX_ATN_Fecha_Siguiente_Contacto__c": "perf_fecha_sig_contacto",
        "MX_ATN_Contactado_Credito__c": "cred_contactado",
        "MX_ATN_Perfilamiento_Credito__c": "cred_perfilamiento_credit",
        "MX_ATN_Canal_Respuesta_Credito__c": "cred_canal_respuesta_credito",
    },
    "casos": {
        "Id": "case_id",
        "Subject": "case_subject",
        "Owner.Name": "case_owner_name",
        "OwnerId": "case_owner_id",
        "CaseNumber": "case_number",
        "CreatedDate": "case_created_date",
        "ClosedDate": "case_closed_date",
        "MX_ATN_Order__c": "order_c",
        "MX_ATN_Order__r.OrderNumber": "sf_order_id",
        "MX_ATN_Order__r.MX_ATN_CommerceId__c": "commerce_order_id",
        "MX_ATN_Oportunidad__c": "opportunity_id",
        "Status": "case_status",
        "MX_ATN_Oportunidad__r.Name": "opportunity_name",
    },
    "citas": {
        "CreatedDate": "created_date",
        "AppointmentNumber": "numero_cita",
        "MX_ATN_OrderId__r.MX_ATN_CommerceId__c": "commerce_order_id",
        "MX_ATN_OrderId__r.OpportunityId": "opportunity_id",
        "MX_ATN_vehicle__c": "marca",
        "Account.MX_ATN_CommerceId__c": "id_am",
        "SchedStartTime": "sched_start_time",
        "SchedEndTime": "sched_end_time",
        "MX_ATN_WorkTypeName__c": "work_type_name",
        "Status": "status",
        "MX_ATN_OrderId__r.Opportunity.Name": "opportunity_name",
    },
    "solicitudes_credito": {
        "MX_ATN_Oportunidad__c": "opportunity_id",
        "MX_ATN_Oportunidad__r.Name": "opportunity_name",
        "MX_ATN_Tipo_Credito__c": "tipo_credito",
        "MX_ATN_Proveedor__c": "proveedor_credito",
        "MX_ATN_creditId__c": "folio",
        "MX_ATN_Tipo_Conclusion_Flujo_Credito__c": "tipo_conclusion_flujo_credito",
        "Name": "simulation_name",
        "MX_ATN_Status__c": "status_solicitud",
        "MX_ATN_Account__c": "asesor_id",
        "MX_ATN_Account__r.Name": "asesor",
        "MX_ATN_Account__r.MX_ATN_CommerceId__c": "id_am_comprador",
        # "MX_ATN_Tipo_Solicitud__c": "tipo_solicitud", 
        "OwnerId": "owner_id",
        "Owner.Name": "owner_name",
        "CreatedDate": "created_date",
        "MX_ATN_Id_Simulacion__c": "simulation_id",
        "MX_ATN_No_Mensualidades__c": "mensualidades",
        "MX_ATN_Folio_Cotizacion__c": "folio_cotizacion",
        "MX_ATN_Mensualidad__c": "mensualidad",
        "MX_ATN_interestedRate__c": "tasa_interes",
        "MX_ATN_CreditoAprobado__c": "credito_aprobado",
        "MX_ATN_Engache__c": "enganche",
        "MX_ATN_SeguroDanos__c": "seguro_danos",
        "MX_ATN_SeguroVida__c": "seguro_vida",
        "MX_ATN_NombreConta__c": "nombre_conta",
        "MX_ATN_Comision_de_Apertura__c": "comision_apertura",
        "MX_ATN_Fecha_Oferta__c": "fecha_oferta",
        "MX_ATN_damageInsuranceTypePayment__c": "tipo_pago_seguro_danos",
        "MX_ATN_Metodo_Pago_Seguro_Vida__c": "tipo_pago_seguro_vida",
        "MX_ATN_Ingreso_Solicitud_BBVA__c": "ingreso_solicitud",
        "MX_ATN_Fecha_Ingreso_BBVA__c": "fecha_ingreso_solicitud",
        "MX_ATN_Fecha_Vigencia__c": "fecha_vigencia",
        "MX_ATN_Fecha_Dictamen_BBVA__c": "fecha_dictamen",
        "MX_ATN_Fecha_Cierre_Credito__c": "fecha_cierre_credito",
        "MX_ATN_Tipo_Inicio_Flujo_Credito__c": "tipo_inicio_flujo_credito",
        "MX_ATN_Motivo_Cierre_Credito__c": "motivo_cierre_credito",
        "MX_ATN_Activo__c": "activo_c",
        "MX_ATN_Reactivacion_Credito__c": "reactivacion_credito",
        "MX_ATN_Comentario__c": "comentario",
        "MX_ATN_Documentacion__c": "documentacion",
        "MX_ATN_Ajustado__c": "ajustado",
        "MX_ATN_Pedido__r.MX_ATN_CommerceId__c": "commerce_order_id",
        "MX_ATN_Activo__r.MX_ATN_CommerceId__c": "sku",
    },
    "historico_casos": {
        "Id": "id_log",
        "IsDeleted": "is_deleted",
        "CaseId": "case_id",
        "CreatedById": "created_by_id",
        "CreatedDate": "created_date",
        "Field": "field",
        "DataType": "data_type",
        "OldValue": "old_value",
        "NewValue": "new_value",
        "CreatedBy.Name": "created_by",
    },
}

CRM_REQUIRED_COLUMNS = {
    table_name: list(rename_dict.keys())
    for table_name, rename_dict in CRM_RENAME_DICTS.items()
}

###### REGLAS DE NEGOCIO PARA FILTRAR DATOS DE CRM ######

CRM_WORK_TYPES_COMPRADOR = [
    "cita inicial visita comprador",
    "cita final busca vehiculo",
]

CRM_WORK_TYPES_VENDEDOR = [
    "cita recogida vehiculo",
    "cita final entrega vehiculo",
    "revision mecanica inicial",
    "revision mecanica final",
]

CRM_STATUS_CITA_COMPLETA = ["completa"]

CRM_STATUS_PEDIDOS_ABIERTOS = [
    "revision de auto",
    "auto en cita",
    "negociacion de precio",
    "acuerdo de compraventa",
    "formalizacion de compra",
    "cita para entrega",
]

############### CONFIGURACION DE OUTPUTS ########################

CRM_CONSUMO_OUTPUT_IDS = {
    # Current draft outputs from ConstruccionReportesCrm.ipynb.
    # Replace these IDs when the final Atlas Consumo files are created.
    "reporte_oportunidades": "108tBedxAlNDLdhmE-sctxkZK37woxLxWuY7blMbNzpM",
    "solicitudes_credito": "1bS-WZOVBBVR77jLbH5kZTIdlAMWGpcHyxA35zP-8skw",
}

CRM_CONSUMO_SHEET_NAMES = {
    "reporte_oportunidades": "Hoja 1",
    "solicitudes_credito": "Hoja 1",
}

CRM_REPORTE_OPORTUNIDADES_COLUMNS = [
    "opportunity_id",
    "opportunity_name",
    "opportunity_created_date",
    "fecha_asignacion",
    "opportunity_owner",
    "owner_id",
    "nombre_comprador",
    "id_am_comprador",
    "last_modified_date",
    "opportunity_source",
    "opportunity_source_aux",
    "flag_perfilamento_sc",
    "asesor_sc",
    "perf_bc_score",
    "perf_intencion_pago",
    "perf_contactado",
    "perf_interesado",
    "perf_comentarios",
    "perf_canal_respuesta",
    "perf_fecha_primer_contacto",
    "perf_fecha_ultimo_contacto",
    "perf_contacto_otro_momento",
    "perf_fecha_sig_contacto",
    "flag_perfilamento_credito",
    "cred_contactado",
    "cred_perfilamiento_credit",
    "cred_canal_respuesta_credito",
    "case_id_perfilamiento_sc",
    "case_status_perfilamiento_sc",
    "case_id_perfilamiento_credito",
    "case_status_perfilamiento_credito",
    "fecha_asignacion_perfilamiento_sc",
    "asesor_credito",
    "fecha_asignacion_perfilamiento_credito",
    "numero_pedidos",
    "numero_pedidos_abiertos",
    "fecha_primer_pedido",
    "fecha_ultimo_pedido",
    "numero_citas_comprador",
    "fecha_primera_cita_visita_comp",
    "fecha_ultima_cita_visita_comp",
    "citas_completas",
]
