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


    def __init__(self,ra):
        self.today = datetime.now(tz=pytz.timezone(mexico_tz)).strftime("%Y-%m-%d")
        self.ra = ra
    
    def proc_publicaciones(self):
        """
        """
        subset_columnas = ['id_am','sku','product_name','status','status_product','plate',
                   'order_id','order_created_at',
                   'vin','engine_type','published_at','km','showroom','vs_extra_url_key']
        rename_dict = {'vs_extra_url_key':'url',
                    'order_id':'last_commerce_order_id'}

        publicacion_df = (self.ra.t1.copy()
        .sort_values(by=['sku','order_created_at'], ascending=[True,False])
        [subset_columnas]
        .groupby('sku').head(1)
        .rename(columns=rename_dict)
        .assign(order_created_at = lambda x: pd.to_datetime(x['order_created_at']).dt.strftime('%Y-%m-%d'),
                published_at = lambda x: pd.to_datetime(x['published_at']).dt.strftime('%Y-%m-%d'),
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
    
    def proc_pedidos(self):
        """
        """
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
        pedidos = (self.ra.t2.copy()
                [subset_columnas]
                .rename(columns=rename_dict)
                .assign(sf_order_id = lambda x: pd.to_numeric(x['sf_order_id'], errors='coerce').astype('Int64'),
                        commerce_order_id = lambda x: pd.to_numeric(x['commerce_order_id'], errors='coerce').astype('Int64'),
                        id_am_comprador = lambda x: pd.to_numeric(x['id_am_comprador'], errors='coerce').astype('Int64'),
                        id_am_vendedor = lambda x: pd.to_numeric(x['id_am_vendedor'], errors='coerce').astype('Int64'),
                        fecha_de_creacion = lambda x: pd.to_datetime(x['fecha_de_creacion']).dt.strftime('%Y-%m-%d'),
                        )
                )
        return pedidos
    
    def proc_clientes(self):
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
        clientes = (self.ra.t3.copy()
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





