from unidecode import unidecode
import pandas as pd
import numpy as np
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


class ProcessedAtlas:


    def __init__(self):
        self.today = datetime.now(tz=pytz.timezone(mexico_tz)).strftime("%Y-%m-%d")
    
    def proc_publicaciones(self, rawdf):
        """
        """
        subset_columnas = ['id_am','sku','product_name','status','status_product','plate',
                   'order_id','order_created_at',
                   'vin','engine_type','created_at','published_at','km','showroom']
        rename_dict = {'vs_extra_url_key':'url',
                    'order_id':'last_commerce_order_id'}

        publicacion_df = (rawdf
        .sort_values(by=['sku','order_created_at'], ascending=[True,False])
        [subset_columnas]
        .groupby('sku').head(1)
        .rename(columns=rename_dict)
        .assign(order_created_at = lambda x: pd.to_datetime(x['order_created_at']).dt.strftime('%Y-%m-%d'),
                published_at = lambda x: pd.to_datetime(x['published_at']).dt.strftime('%Y-%m-%d'),
                created_at = lambda x: pd.to_datetime(x['created_at']).dt.strftime('%Y-%m-%d'),
                id_am = lambda x: pd.to_numeric(x['id_am'], errors='coerce').astype('Int64'),
                last_commerce_order_id = lambda x: pd.to_numeric(x['last_commerce_order_id'], errors='coerce').astype('Int64')
                )
        .assign(flag_publicacion_exito = lambda x: x.published_at.notna()*1,
                flag_publicacion_activa = lambda x: (x.status_product.isin(['reserved','published']) 
                        & (x.status=='Enabled')
                        & (x.published_at.notna()))*1)
        .reset_index(drop=True)
        )
        return publicacion_df
    
    def proc_pedidos(self,rawdf):
        """
        """
        multiapartado_days_window = 20
        subset_columnas = ['numero_de_pedido', 'pedido_id_comercio_externo',
       'nombre_de_la_cuenta', 'id_am_vendedor',
       'comprador_nombre_de_la_cuenta', 'id_am_comprador', 'estado',
       'precio_de_publicacion', 'fecha_de_creacion', 'descripcion_vehiculo', 'vin']
        rename_dict = {'numero_de_pedido':'sf_order_id',
                    "pedido_id_comercio_externo":"commerce_order_id",
                    "nombre_de_la_cuenta":"nombre_vendedor",
                    "comprador_nombre_de_la_cuenta":"nombre_comprador",
                    "estado":"sf_order_status",
                    }
        pedidos = (rawdf
                [subset_columnas]
                .rename(columns=rename_dict)
                .assign(sf_order_id = lambda x: pd.to_numeric(x['sf_order_id'], errors='coerce').astype('Int64'),
                        commerce_order_id = lambda x: pd.to_numeric(x['commerce_order_id'], errors='coerce').astype('Int64'),
                        id_am_comprador = lambda x: pd.to_numeric(x['id_am_comprador'], errors='coerce').astype('Int64'),
                        id_am_vendedor = lambda x: pd.to_numeric(x['id_am_vendedor'], errors='coerce').astype('Int64'),
                        fecha_de_creacion = lambda x: pd.to_datetime(x['fecha_de_creacion']),
                        )
                .sort_values(by=['id_am_comprador','fecha_de_creacion'],ascending=[True,True])
                .assign(days_since_last_order = lambda x: ((x.groupby('id_am_comprador')['fecha_de_creacion'].diff().dt.days)),
                        orders_by_id_comprador = lambda x: x.groupby('id_am_comprador')['sf_order_id'].transform('nunique'),
                        multiapartado = lambda x: np.where(x.days_since_last_order<=multiapartado_days_window,1,0),
                        fecha_de_creacion = lambda x: x['fecha_de_creacion'].dt.strftime('%Y-%m-%d')
                        )
                .sort_values(by=['fecha_de_creacion','sf_order_id'],ascending=[True,True])
                    )
        return pedidos
    
    def proc_clientes(self,rawdf):
        """
        """
        subset_columnas = [
                "id_am",
                "billing_firstname",
                "billing_lastname",
                "nickname",
                "email",
                "phone",
                "zip",
                "country",
                "state_province",
                "customer_since",
                "date_of_birth",
                "phone_number_otp_validated",
                "email_otp_validated",
            ]
        clientes = (rawdf
                    [subset_columnas]
                    .assign(id_am = lambda x: pd.to_numeric(x['id_am'], errors='coerce').astype('Int64'),
                            phone = lambda x: np.where(x.phone.notna(),
                                                    pd.to_numeric(x['phone'], errors='coerce').astype('Int64').astype(str),
                                                    None),
                            billing_firstname = lambda x: x['billing_firstname'].str.upper(),
                            billing_lastname = lambda x: x['billing_lastname'].str.upper(),
                            nickname = lambda x: x['nickname'].str.upper(),
                            customer_since = lambda x: pd.to_datetime(x['customer_since']).dt.strftime('%Y-%m-%d'),
                            date_of_birth = lambda x: pd.to_datetime(x['date_of_birth']).dt.strftime('%Y-%m-%d'),
                            phone_number_otp_validated = lambda x: x['phone_number_otp_validated'].fillna(0).astype('Int64'),
                            email_otp_validated=lambda x: x['email_otp_validated'].fillna(0).astype('Int64'),
                            zip = lambda x: np.where(x.zip.notna(),
                                                    x.zip.astype('Int64').astype(str).str.zfill(5),
                                                    None),
                            )
                    [lambda x: x.email_otp_validated.eq(1)| x.phone_number_otp_validated.eq(1)]
                    )
        return clientes
    
    def proc_adobe_funnel_comprador(self,rawdf, tipo='total'):
        """tipo = 'total' o 'usuario'. 'total' devuelve appstep sin filtros, 'usuario' deduplica por usuario unico fech
                y quita entradas sin usuario automarket.
        """
        subset_columnas = ['date', 'id_am', 'application_name',
                           'app_click_start', 'app_page_visit',
                           'app_completed']
        rename_dict = {'app_click_start': 'fc_app_click_start',
                       'app_page_visit': 'fc_app_page_visit',
                       'app_completed': 'fc_app_completed'
                       }
        funnel_comprador_digital = (rawdf
                                    [subset_columnas]
                                    [lambda x: x.application_name ==
                                        'pago de apartado']
                                    .rename(columns=rename_dict)
                                    .assign(id_am=lambda x: pd.to_numeric(x.id_am, errors='coerce').astype('Int64'),
                                            date=lambda x: pd.to_datetime(
                                                x.date, errors='coerce').dt.strftime('%Y-%m-%d')
                                            )
                                    .reset_index(drop=True)
                                        )
        if tipo=='usuario':
            funnel_comprador_digital =(funnel_comprador_digital
                                       [lambda x: x.id_am.notna()]
                                    .groupby(['id_am', 'date', 'application_name'], as_index=False).sum()
                                    .assign(fc_app_click_start=lambda x: x.fc_app_click_start.gt(0)*1,
                                            fc_app_page_visit=lambda x: x.fc_app_page_visit.gt(
                                        0)*1,
                                            fc_app_completed=lambda x: x.fc_app_completed.gt(0)*1
                                        )
                                        )
            assert funnel_comprador_digital.groupby(["id_am","date"]).size().max()==1, "Dataframe con usuario-fecha duplicado!"
        return funnel_comprador_digital
    
    def proc_adobe_funnel_vendedor(self,rawdf, tipo='total'):
        """
        """
        subset_columnas = ['date','id_am','application_name',
                   'app_click_start','app_page_visit',
                   'app_step_2','app_step_3', 'app_step_4',
                   'app_step_6', 'app_step_8','app_step_9','app_step_10',
                   'app_completed']
        rename_dict = {'app_click_start':'fv_app_click_start',
                        'app_page_visit':'fv_app_page_visit',
                        'app_step_2':'fv_app_step_2',
                        'app_step_3':'fv_app_step_3',
                        'app_step_4':'fv_app_step_4',
                        'app_step_6':'fv_app_step_6',
                        'app_step_8':'fv_app_step_8',
                        'app_step_9':'fv_app_step_9',
                        'app_step_10':'fv_app_step_10'
                    }
        funnel_digital_vendedor = (rawdf
                                [subset_columnas]
                                [lambda x:x.application_name=='vender mi auto']
                                .rename(columns=rename_dict)
                                .assign(id_am = lambda x: pd.to_numeric(x.id_am,errors='coerce').astype('Int64'),
                                        date = lambda x: pd.to_datetime(x.date,errors='coerce').dt.strftime('%Y-%m-%d')
                                        )
                                .reset_index(drop=True)
                                
                                )
        
        if tipo=='usuario':
            funnel_digital_vendedor = (funnel_digital_vendedor
                                       [lambda x: x.id_am.notna()]
                                       .groupby(['id_am', 'date', 'application_name'], as_index=False)
                                       .sum()
                                       .assign(**{y: lambda x, col=y: x[col].gt(0)*1 
                                                  for y in rename_dict.values()}
                                               )
                                               )
        return funnel_digital_vendedor
    
    def proc_visitas_unicas(self,rawdf):
        """
        """
        subset_columns = ['date','pages',
                  'unique_visitors','visits']
        rename_dict={'pages':'page_name'}
        mapping_page_names = {'escritorio:publica:comprador:landing':'fc_visitas_unicas_landing_comprador',
            'escritorio:publica:comprador:pdp':'fc_visitas_unicas_pdp',
            'escritorio:publica:comprador:plp':'fc_visitas_unicas_plp',
            'escritorio:publica:vendedor:landing':'fv_visitas_unicas_landing_vendedor',
            'escritorio:privada:vendedor:landing':'fv_visitas_unicas_landing_vendedor',
            'escritorio:publica:comprador:plp:todos los vehículos':None,
            'escritorio:privada:comprador:pdp':None,
            'escritorio:publica:personas:landing':None}

        unique_visits = (rawdf
                        [subset_columns]
                        .rename(columns=rename_dict)
                        .assign(date = lambda x: pd.to_datetime(x.date).dt.strftime('%Y-%m-%d'),
                                visit_type = lambda x: x.page_name.map(mapping_page_names))
                        [lambda x: x.visit_type.notna()]
                        .groupby(['date','visit_type'])
                        ['unique_visitors'].sum()
                        .unstack('visit_type')
                        .fillna(0)
                        .reset_index()
        )
        return unique_visits
    
    def proc_pdp_views(self,rawdf):
        """
        """
        pdp = (rawdf
                .assign(id_am = lambda x:pd.to_numeric(x.id_am,errors='coerce').astype('Int64'),
                        date = lambda x: pd.to_datetime(x.date),
                        page_views = lambda x: pd.to_numeric(x.page_views,errors='coerce').astype('Int64'),
                        view_type = lambda x: np.where(x.id_am.isna(),'unidentified_user_views','identified_user_views'))
                .groupby(['date','sku','view_type'])
                ['page_views'].sum()
                .unstack('view_type').fillna(0)
                .reset_index()
                [lambda x: pd.to_datetime(x.date)>=(pd.to_datetime(self.today)-timedelta(days=90))]
                )
        return pdp

    def proc_consolidado_bauto(self,rawdf):
        """
        """
        res = (rawdf
                .sort_values(by=['folio','year_base','month_base'],ascending=[False,False,False])
                .assign(folio = lambda x: x.folio.astype('Int64'),
                        fecha_creacion = lambda x: pd.to_datetime(x.fecha_creacion).dt.strftime('%Y-%m-%d'),
                        telefono = lambda x: np.where(x.telefono.notna(),x.telefono.astype('Int64').astype(str),x.telefono),
                        month_fecha_creacion = lambda x: pd.to_datetime(x.fecha_creacion).dt.month.astype('Int64'),
                        year_fecha_creacion = lambda x: pd.to_datetime(x.fecha_creacion).dt.year.astype('Int64'),
                        )
                .groupby('folio').head(1)
                )
        
        return res
    
    def proc_cancelaciones(self,rawdf):
        """
        Docstring para proc_cancelaciones
        
        :param self: Descripción
        :param rawdf: Descripción
        """
        res = (rawdf
                .pipe(process_columns)
                )
        return res
    
    def proc_reporte_ventas(self,rawdf):
        """
        Docstring para proc_reporte_ventas
        
        :param self: Descripción
        :param rawdf: Descripción
        """
        res = (rawdf
                .rename(columns = {'Año':'anio',
                                    'ID comprador':'id_am_comprador',
                                    'ID pedido':'sf_order_id'})
                .pipe(process_columns)
                .assign(fecha_de_apartado = lambda x: pd.to_datetime(x.fecha_de_apartado,format='%d/%m/%Y').dt.strftime('%Y-%m-%d'),
                        fecha_de_entrega = lambda x: pd.to_datetime(x.fecha_de_entrega,format='%d/%m/%Y').dt.strftime('%Y-%m-%d'),
                        seguro = lambda x: x.seguro.fillna('no').str.lower(),
                        garantia = lambda x: x.garantia.fillna('no').str.lower(),
                        tipo_de_venta = lambda x: x.tipo_de_venta.str.lower(),
                        espacio_am = lambda x: x.espacio_am.str.replace('ESPACIO ', '').str.lower().str.strip(),
                        sf_order_id = lambda x: x.sf_order_id.astype('Int64'),
                        id_am_comprador = lambda x: x.id_am_comprador.astype('Int64'),
                )
                )
        return res
    
    def proc_edas(self,rawdf):
        """
        Docstring para proc_edas
        
        :param self: Descripción
        :param rawdf: Descripción
        """
        subset_columns=['folio','fecha_ref',
                'nombre_completo_del_empleado','usuario_m',
                'nombre_de_cliente','espacio',
                'telefono_celular_del_cliente',
                    # 'correo_email_del_cliente',
                # 'numero_de_cliente', 
                # 'observaciones_de_contactacion',
                'numero_del_credito_formalizado_y_desembolsado']
        edas_mod = (rawdf
                    .pipe(process_columns)
                    [lambda x: x['folio_preautorizado'].notna()]
                    .assign(fecha_ref= lambda x: pd.to_datetime(x['marca_temporal'].str[0:10].str.strip().str.zfill(10),format='%d/%m/%Y').dt.strftime('%Y-%m-%d'),
                            folio = lambda x: pd.to_numeric(x['folio_preautorizado'],errors='coerce').astype('Int64')
                            )
                    .sort_values(by='fecha_ref',ascending=False)
                    .groupby('folio').head(1)
                    [subset_columns]
                    .reset_index(drop=True)
                    )
        return edas_mod
                                            
    




    


