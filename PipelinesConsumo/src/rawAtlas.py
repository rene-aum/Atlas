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
from utils.drive_toolbox import read_from_google_sheets
import pytz

app_step_rename_dict = {}


class RawAtlas:
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

    def t1_raw_vehicle_status(self, **custom_read_args):
        """ 
        """
        if self.vehicle_status_tab is None:
            vs_raw = (custom_read(**custom_read_args)
                  .pipe(process_columns)
                  )
            rename_dict = {
                "customer_id": "id_am",
                "niv/vin":"vin"
            }
            columns_to_drop = vs_raw.filter(regex='^column').columns
            res = (
                vs_raw
                .drop(columns=columns_to_drop)
                .rename(columns=rename_dict)
                
            )
            self.vehicle_status_tab = res
        return self.vehicle_status_tab
    
    def t2_raw_pedidos(self,date_col="fecha_de_creacion", **custom_read_args):
        """ """
        if self.pedidos_tab is None:
            rename_dict = {
                "vendedor_id_comercio_externo": "id_am_vendedor",
                "comprador_id_comercio_externo": "id_am_comprador",
                "activo_vehiculo_id_numero_de_identificacion_vehicular_(niv)":"vin",
                "activo_producto_nombre_del_producto":"descripcion_vehiculo"

            }
            df = custom_read(**custom_read_args)
            columnas_eliminar = df.filter(like='Column').columns
            res = (
                df
                .drop(columns=columnas_eliminar)
                .pipe(process_columns)
                .rename(columns=rename_dict)        
            )
            self.pedidos_tab = res
        return self.pedidos_tab
    
    def t3_raw_clientes(self, **custom_read_args):
        """ """
        if self.clientes_tab is None:
            df = custom_read(**custom_read_args)
            rename_dict = {
                "id": "id_am",
                "state/province": "state_province",
            }

            res = (
                process_columns(df)
                .rename(columns=rename_dict)
            )
            self.clientes_tab = res
        return self.clientes_tab


    def t4_raw_appstep(self, **custom_read_args):
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
            )
            self.appstep_tab = res
        return self.appstep_tab
    
    def t5_raw_unique_visitors(self,**custom_read_args):
        """
        """
        if self.unique_visitors_adobe_df is None:
            df = (custom_read(**custom_read_args)
                  .rename(columns={'Page name <v1> (evar1)':'pages'})
                .pipe(process_columns)
                )
            self.unique_visitors_adobe_df = df
        return self.unique_visitors_adobe_df


    def t6_raw_product_views(self, **custom_read_args):
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
                df
                .rename(columns=rename_dict)
                .pipe(process_columns)
                  )
            self.product_views_tab = res
        return self.product_views_tab
    
    def t7_raw_cancelaciones(self, **custom_read_args):
        """
        """
        if self.cancelaciones_df is None:
            df = custom_read(**custom_read_args)
            df_mod = (df
                    .pipe(process_columns)
                    .assign(cancelled_at = lambda x: pd.to_datetime(x.cancelled_at).dt.date)
                    )
            self.cancelaciones_df=df_mod
        return self.cancelaciones_df


    def t8_raw_cta_adobe(self,**custom_read_args):
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
                )
            self.cta_adobe_df = res
        return self.cta_adobe_df
    
    def t9_raw_consolidado_bauto(self, drive, gc, drive_folder_id):
        """actualmente esta info se lee directo de drive.
        """
        file_list = drive.ListFile(
            {'q': f"'{drive_folder_id}' in parents and trashed=false"}).GetList()
        file_id_dict = {}
        for file in file_list:
            file_id_dict[file['title']] = file['id']
        bases_ids = {k: file_id_dict[k]
                     for k in file_id_dict.keys() if k.startswith('base')}

        number_to_month = {'ene': 1, 'feb': 2, 'mar': 3, 'abr': 4,
                           'may': 5, 'jun': 6, 'jul': 7, 'ago': 8,
                           'sep': 9, 'oct': 10, 'nov': 11, 'dic': 12}
        consolidado_raw = (pd.concat(
            [read_from_google_sheets(gc, bases_ids[k])
             .assign(month_base=number_to_month.get(k[-5:-2].lower()),
                     year_base=int(k[-2:]))
             for k in bases_ids.keys()]
                            )
                            )
        return consolidado_raw




