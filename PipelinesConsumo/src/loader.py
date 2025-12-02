from unidecode import unidecode
import pandas as pd
from datetime import timedelta, datetime
import sys
sys.path.append('../src')
sys.path.append('../..')
from src.constants import mexico_tz
from utils.utils import (add_year_week,
                   process_columns,
                   custom_read)
import pytz

app_step_rename_dict = {}


class Loader:
    appstep_tab = None
    salesforce_tab = None
    clientes_tab = None
    vehicle_status_tab = None
    visitantes_diarios_tab = None
    funnel_comprador_tab = None
    pedidos_tab = None
    pedidos_logs_tab = None
    terminos_de_busqueda_tab = None
    canales_adobe_tab = None
    product_views_tab = None
    retroalimentacion_tab =  None
    envios_medios_propios_df = None
    citas_tab = None
    unique_visitors_adobe_df = None
    salud_inventario_df = None
    cta_adobe_df = None
    cancelaciones_df = None


    def __init__(self):
        self.today = datetime.now(tz=pytz.timezone(mexico_tz)).strftime("%Y-%m-%d")

    def load_appstep_tab(self, **custom_read_args):
        """ """
        if self.appstep_tab is None:
            df = custom_read(**custom_read_args)
            columnas_eliminar = df.filter(like='Column').columns
            rename_dict = {
                "id": "id_adobe",
                "customer_id_<v37>_(evar37)": "id_am",
                "app_on_click_start_(serial)_<e40>_(event40)": "app_click_start",
                "app_page_visits_(serial)_<e41>_(event41)": "app_page_visit",
                "apps_started_(serial)_<e42>_(event42)": "app_started",
                "app_step_2_(serial)_<e43>_(event43)": "app_step_2",
                "app_step_3_(serial)_<e44>_(event44)": "app_step_3",
                "app_step_4_(serial)_<e45>_(event45)": "app_step_4",
                "app_step_5_(serial)_<e46>_(event46)": "app_step_5",
                "app_step_6_(serial)_<e47>_(event47)": "app_step_6",
                "app_step_7_(serial)_<e48>_(event48)": "app_step_7",
                "app_step_8_(serial)_<e49>_(event49)": "app_step_8",
                'app_step_9_(serial)_<e50>_(event50)': 'app_step_9',
                'app_step_10_(serial)_<e51>_(event51)': 'app_step_10',
                'app_step_11_(serial)_<e52>_(event52)': 'app_step_11',
                'app_step_12_(serial)_<e53>_(event53)': 'app_step_12',
                'app_step_13_(serial)_<e54>_(event54)': 'app_step_13',
                "apps_completed_(serial)_<e55>_(event55)": "app_completed",
            }
            res = (
                process_columns(df.drop(columns=columnas_eliminar))
                .rename(columns=rename_dict)
                .assign(
                    id_am=lambda x: pd.to_numeric(x.id_am, errors="coerce").astype(
                        "Int64"
                    ),
                    date=lambda x: pd.to_datetime(x.date),
                    month=lambda x: x.date.dt.month,
                    year=lambda x: x.date.dt.year,
                )
            )
            self.appstep_tab = res
        return self.appstep_tab

    def load_salesforce_tab(self, **custom_read_args):
        """ """
        if self.salesforce_tab is None:

            df = custom_read(**custom_read_args)
            res = (
                process_columns(df)
                .rename(
                    columns={
                        "id_comercio_externo": "id_am",
                        "created_date": "fecha_de_creacion",
                    }
                )
                .assign(
                    id_am=lambda x: pd.to_numeric(x.id_am, errors="coerce").astype("Int64"),
                    telefono=lambda x: pd.to_numeric(x.phone, errors="coerce").astype("Int64"),
                )
            )
            self.salesforce_tab = res
        return self.salesforce_tab

    def load_clientes_tab(self, **custom_read_args):
        """ """
        if self.clientes_tab is None:
            df = custom_read(**custom_read_args)
            columns_subset = [
                "id",
                "zip",
                "country",
                "state/province",
                "customer_since",
                "date_of_birth",
                "tax_vat_number",
                "city",
                "status",
                "customer_type",
                "integrated",
                "phone_number_otp_validated",
                "email_otp_validated",
            ]
            rename_dict = {
                "id": "id_am",
                "state/province": "state_province",
            }
        
            res = (
                process_columns(df)[columns_subset]
                .rename(columns=rename_dict)
                .assign(
                    id_am=lambda x: pd.to_numeric(x.id_am, errors="coerce").astype("Int64"),
                    customer_since=lambda x: pd.to_datetime(x.customer_since).dt.strftime("%Y-%m-%d"),
                    # date_of_birth=lambda x: pd.to_datetime(x.date_of_birth, format="%m/%d/%Y").dt.strftime("%Y-%m-%d"),
                    phone_number_otp_validated=lambda x: x.phone_number_otp_validated.fillna(0),
                    email_otp_validated=lambda x: x.email_otp_validated.fillna(0),)
                    [lambda x: x.email_otp_validated.eq(1)| x.phone_number_otp_validated.eq(1)]
            )
            self.clientes_tab = res
        return self.clientes_tab

    def load_vehicle_status_tab(self, **custom_read_args):
        """ 
        """
        if self.vehicle_status_tab is None:
            df = custom_read(**custom_read_args)
            rename_dict = {
                "customer_id": "id_am",
            }
            res = (
                process_columns(df)
                .rename(columns=rename_dict)
                .assign(
                    id_am=lambda x: pd.to_numeric(x.id_am, errors="coerce").astype("Int64"),
                    published_date=lambda x: pd.to_datetime(x.published_at),
                    published_year=lambda x: x.published_date.dt.year,
                    published_week=lambda x: x.published_date.dt.isocalendar().week,
                    published_year_week=lambda x: x.published_year.astype("Int64").astype(str)
                                        + "_"
                                        + x.published_week.astype(str).str.zfill(2),
                    created_date=lambda x: pd.to_datetime(x.created_at).dt.date,
                )
                .assign(published_date=lambda x: x.published_date.dt.date)
                .drop(columns=["published_at", "created_at"])
                .rename(columns={"niv/vin": "vin"})
            )
            self.vehicle_status_tab = res
        return self.vehicle_status_tab

    def load_visitantes_diarios_tab(self, **custom_read_args):
        """
        """
        if self.visitantes_diarios_tab is None:
            df = custom_read(**custom_read_args)
            res = process_columns(df).assign(
                date=lambda x: pd.to_datetime(x.date),
                year=lambda x: x.date.dt.year,
                week=lambda x: x.date.dt.isocalendar().week,
                year_week=lambda x: x.year.astype("Int64").astype(str)
                                    + "_"
                                    + x.week.astype(str).str.zfill(2),
            )
            self.visitantes_diarios_tab = res
        return self.visitantes_diarios_tab

    def load_funnel_comprador_tab(self, **custom_read_args):
        """ 
        """
        if self.funnel_comprador_tab is None:
            rename_dict = {
                "Page name <v1> (evar1)": "page_name",
                "Customer ID <v37> (evar37)": "id_am",
            }
            df = custom_read(**custom_read_args)
            res = (
                df.rename(columns=rename_dict)
                .pipe(process_columns)
                .assign(date=lambda x: pd.to_datetime(x.date))
                .pipe(add_year_week)
                .assign(page_name=lambda x: x.page_name.fillna("NAN"))
                #  [lambda x: x.page_name.fillna('null').str.contains('vendedor:landing')]
            )
            self.funnel_comprador_tab = res
        return self.funnel_comprador_tab

    def load_pedidos_tab(self,date_col="fecha_de_creacion", **custom_read_args):
        """ """
        if self.pedidos_tab is None:
            rename_dict = {
                "Vendedor: Id Comercio Externo": "id_am_vendedor",
                "Comprador: Id Comercio Externo": "id_am_comprador",
            }
            df = custom_read(**custom_read_args)
            columnas_eliminar = df.filter(like='Column').columns
            res = (
                df.rename(columns=rename_dict)
                .drop(columns = columnas_eliminar)
                .pipe(process_columns)
                .assign(date=lambda x: pd.to_datetime(x[date_col]))
                .pipe(add_year_week)                
            )
            self.pedidos_tab = res
        return self.pedidos_tab
    

    def load_pedidos_logs_tab(self, **custom_read_args):
        """
        """
        if self.pedidos_logs_tab is None:
            rename_dict = {
                "Order.OrderNumber": "numero_pedido",
                "Order.MX_ATN_CommerceId__c": "id_am",
                "CreatedDate": "date",
                "NewValue": "status",
            }
            df = custom_read(**custom_read_args)
            res = (
                df.rename(columns=rename_dict)
                .pipe(process_columns)
                .assign(date=lambda x: (pd.to_datetime(x.date, format="%d/%m/%Y %H:%M:%S")))
                .pipe(add_year_week)
                .assign(date=lambda x: x.date.dt.strftime("%Y-%m-%d"))
            )
            self.pedidos_logs_tab = res
        return self.pedidos_logs_tab

    def load_terminos_de_busqueda_tab(self, **custom_read_args):
        """
        """
        if self.terminos_de_busqueda_tab is None:

            rename_dict = {
                "Internal search enter term <v19> (evar19)": "termino_busqueda",
            }
            df = custom_read(**custom_read_args)
            res = (
                df.rename(columns=rename_dict)
                .pipe(process_columns)
                .assign(date=lambda x: (pd.to_datetime(x.date)))
                .pipe(add_year_week)
                .assign(
                    date=lambda x: x.date.dt.strftime("%Y-%m-%d"),
                )
            )
            self.terminos_de_busqueda_tab = res
        return self.terminos_de_busqueda_tab

    def load_product_views_tab(self, **custom_read_args):
        """ 
        """
        if self.product_views_tab is None:
            rename_dict = {
                "Customer ID <v37> (evar37)": "id_am",
                "Page url <c13> (prop13)": "url",
                "Products": "sku",
            }
            df = custom_read(**custom_read_args)
            res = (
                df.rename(columns=rename_dict)
                .pipe(process_columns)
                .assign(date=lambda x: (pd.to_datetime(x.date,format='mixed')))
                .pipe(add_year_week)
                .assign(
                    date=lambda x: x.date.dt.strftime("%Y-%m-%d"),
                )
            )
            self.product_views_tab = res
        return self.product_views_tab

    def load_canales_adobe_tab(self, **custom_read_args):
        """
        """
        if self.canales_adobe_tab is None:
            df = custom_read(**custom_read_args)
            res = (
                df.pipe(process_columns)
                .assign(date=lambda x: (pd.to_datetime(x.date)))
                .pipe(add_year_week)
                .assign(
                    date=lambda x: x.date.dt.strftime("%Y-%m-%d"),
                )
            )
            self.canales_adobe_tab = res
        return self.canales_adobe_tab

    def load_retroalimentacion_tab(self, **custom_read_args):
        """
        """
        if self.retroalimentacion_tab is None:
            df = custom_read(**custom_read_args)
            rename_dict = {"Fecha de ultimo pago transaccion2": "date"}
            if 'Estatus de la operación' in df.columns:
                df = df.rename(columns={'Estatus de la operación':'status_de_la_operacion'})
                
            res = (
                df.rename(columns=rename_dict)
                .pipe(process_columns)
                .assign(date=lambda x: (pd.to_datetime(x.date)))
                .pipe(add_year_week)
                .assign(
                    date=lambda x: x.date.dt.strftime("%Y-%m-%d"),
                    status_de_la_operacion=lambda x: x.status_de_la_operacion.str.lower(),
                    formato_de_pago=lambda x: x.formato_de_pago.str.lower(),
                    flag_credito=lambda x: (x.formato_de_pago == "financiamiento")
                                            & (x.status_de_la_operacion == "finalizada"),
                )
            )
            self.retroalimentacion_tab=res
        return self.retroalimentacion_tab
    
    def load_envios_medios_propios(self, **custom_read_args):
        """
        """
        if self.envios_medios_propios_df is None:
            df = custom_read(**custom_read_args)
            self.envios_medios_propios_df =df
        return self.envios_medios_propios_df
    
    def load_unique_visitors_adobe(self,**custom_read_args):
        """
        """
        if self.unique_visitors_adobe_df is None:
            df = (custom_read(**custom_read_args)
                  .rename(columns={'Page name <v1> (evar1)':'pages'})
                .pipe(process_columns)
                .assign(date = lambda x: pd.to_datetime(x.date))
                )
            self.unique_visitors_adobe_df = df
        return self.unique_visitors_adobe_df
    
    def load_citas_tab(self,**custom_read_args):
        """lee la tab Citas del datawarehouse. Se toma Scheduled Start como columna data
            para contar citas.
        """
        rename_dict = {'Id Comercio Externo':'id_am',
                       'Scheduled Start':'date'}
        if self.citas_tab is None:
            df = custom_read(**custom_read_args)
            res = (
                df.rename(columns=rename_dict)
                .pipe(process_columns)
                .assign(date=lambda x: (pd.to_datetime(x.date)))
                .pipe(add_year_week)
                .assign(
                    date=lambda x: x.date.dt.strftime("%Y-%m-%d"),
                    numero_de_pedido = lambda x: x.numero_de_pedido.astype('Int64'),
                    status = lambda x: x.status.str.lower()
                    )
            )
            self.citas_tab=res
        return self.citas_tab
    
    def load_salud_inventario(self, **custom_read_args):
        """
        Carga los datos de salud y realiza algunas transformaciones, como renombrar la columna y formatear las fechas.
        """
        if self.salud_inventario_df is None:
            df = custom_read(**custom_read_args)
            df.columns = df.columns.str.lower().str.replace(' ', '_')

            rename_dict = {
                "fecha_creacion" : "date"
            }
            df = df.rename(columns=rename_dict)
            df = df.pipe(process_columns)

            df['date'] = pd.to_datetime(df['date'], format = "%m/%d/%Y",errors='coerce')
            df['max_seq_date_at'] = pd.to_datetime(df['max_seq_date_at'], format = "%m/%d/%Y" ,errors='coerce')
            df['fecha_de_modificacion'] = pd.to_datetime(df['fecha_de_modificacion'], format = "%m/%d/%Y" ,errors='coerce')
            df['fecha_cierre'] = pd.to_datetime(df['fecha_cierre'], format = "%m/%d/%Y" ,errors='coerce')

            df = df.assign(
                date=lambda x: x['date'].dt.strftime("%Y-%m-%d"),
                max_seq_date_at=lambda x: x['max_seq_date_at'].dt.strftime("%Y-%m-%d"),
                fecha_de_modificacion=lambda x: x['fecha_de_modificacion'].dt.strftime("%Y-%m-%d"),
                fecha_cierre=lambda x: x['fecha_cierre'].dt.strftime("%Y-%m-%d"),
                nivel_comercialidad = lambda x: x.nivel_comercialidad.str.strip()
            )

            self.salud_inventario_df = df

        return self.salud_inventario_df
    
    def load_cta_adobe(self,**custom_read_args):
        """
        """
        if self.cta_adobe_df is None:
            df = custom_read(**custom_read_args)
            rename_dict = {
                "application_step_<v45>_(evar45)": "application_step",
                "cta_<v59>_(evar59)": "cta",
                "app_on_click_start_(serial)_<e40>_(event40)": "app_on_click_start",
                "app_page_visits_(serial)_<e41>_(event41)" : "app_page_visit",
                "apps_completed_(serial)_<e55>_(event55)" : "apps_completed"
            }

            res = (
                process_columns(df)
                .rename(columns=rename_dict)
                .pipe(process_columns)
                .assign(
                    date = lambda x: pd.to_datetime(x.date))
                )
            self.cta_adobe_df = res
        return self.cta_adobe_df
    
    def load_cancelaciones(self, **custom_read_args):
        """
        """
        if self.cancelaciones_df is None:
            df = custom_read(**custom_read_args)
            df_mod = (df
                    .pipe(process_columns)
                    .assign(cancelled_at = lambda x: pd.to_datetime(x.cancelled_at).dt.date)
                    
                    )
            if df_mod.shape[0]>df_mod['sku'].nunique():
                df_mod = df_mod.drop_duplicates(subset=['sku'])
            self.cancelaciones_df=df_mod
        return self.cancelaciones_df




