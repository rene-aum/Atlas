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
                   last_column_with_one,
                   custom_read)
import pytz
from calendar import monthrange

app_step_rename_dict = {}
status_autos_transaccionables  = [
            'Entrega de auto (finalizado)',
            'Pedido cancelado por negociación no exitosa por parte del comprador',
            'Pedido cancelado por no-show del comprador', 
            'Acuerdo de compraventa',
            'Negociación de precio',
            'Cita para Entrega'
            ]

class Transformed:
    """En esta clase estan los metodos para transformar las tablas raw del datawarehouse.
    """


    def __init__(self,lo):
        """lo es un objeto de la clase loader con las tablas necesarias cargadas
        """
        self.today = datetime.now(tz=pytz.timezone(mexico_tz)).strftime("%Y-%m-%d")
        self.lo = lo

    def t_visitas_diarias_by_date(self):
        """
        """
        df = (self.lo.load_visitantes_diarios_tab()
              .groupby('date', as_index=False).visits.sum()
              .sort_values(by='date')
              )
        return df
    
    def t_visitas_last_touch_by_date(self):
        """
        """
        df = (self.lo.load_canales_adobe_tab()
              .groupby(['date', 'last_touch_channel'])['visits'].sum()
              .sort_index(level=0)
              .unstack('last_touch_channel')
              .fillna(0).reset_index()
              .fillna(0)
              .assign(date=lambda x: pd.to_datetime(x.date).dt.strftime('%Y-%m-%d'))
              )
        return df
    
    def t_visitas_landing_by_date(self):
        """
        """
        df = (self.lo.load_funnel_comprador_tab()
                [lambda x: x.page_name.fillna('null').str.contains('vendedor:landing')
                | x.page_name.fillna('null').str.contains('comprador:landing')]
                .assign(type_visit=lambda x: np.where(x.page_name.fillna('null').str.contains('vendedor:landing'),
                                                    'visitas_landing_vendedor',
                                                    'visitas_landing_comprador'))
                .groupby(['date', 'type_visit']).visits.sum()
                .unstack('type_visit')
                .sort_index()
                .fillna(0).reset_index()
                )
        return df

    def t_new_users_by_date(self):
        """
        """
        df = (self.lo.load_clientes_tab()
                .rename(columns={'customer_since':'date'}) #customer since
                .assign(date = lambda x: pd.to_datetime(x.date))
                .groupby('date',as_index=False).id_am.nunique()
                .sort_values(by='date')
                .fillna(0)
                .rename(columns={'id_am':'new_users'})
                )
        return df

    def t_inicio_publicacion_by_date(self):
        """
        """
        df = (self.lo.load_appstep_tab()
            [lambda x: x.application_name=='vender mi auto']
            .groupby('date').agg({'app_page_visit':'sum'})
            .sort_index().reset_index()
            .sort_values(by='date')
            .rename(columns={'app_page_visit':'inicio_publicacion'})
            .fillna(0)
            )
        return df
    
    def t_publicacion_by_date(self):
        """
        """
        df = (self.lo.load_vehicle_status_tab()
                [lambda x: x.status_product.isin(['reserved','published']) 
                 & (x.status=='Enabled')
                 & (x.published_date.notna())]
                [['published_date','sku']]
                .rename(columns={'published_date':'date'})
                .assign(date = lambda x: pd.to_datetime(x.date))
                # .merge(all_dates,on='date',how='right')
                .groupby('date',as_index=False).sku.nunique()
                .sort_values(by='date')
                .rename(columns={'sku':'publicaciones'})
                .fillna(0)
                )
        return df
    
    def t_publicacion_historica_by_date(self):
        """
        """
        df = (self.lo.load_vehicle_status_tab()
                [['published_date','sku']]
                .rename(columns={'published_date':'date'})
                .assign(date = lambda x: pd.to_datetime(x.date))
                # .merge(all_dates,on='date',how='right')
                .groupby('date',as_index=False).sku.nunique()
                .sort_values(by='date')
                .rename(columns={'sku':'publicaciones_all'})
                .fillna(0)
                # .assign(date=lambda x: pd.to_datetime(x.date).dt.strftime('%Y-%m-%d'))
                )
        return df
    
    def t_apartados_activos_by_date(self):
        """
        """
        df = (self.lo.load_pedidos_tab()
              [lambda x: x.estado.isin(['Revisión de auto', 'Acuerdo de compraventa',
                                       'Negociación de precio', 'Auto en cita', 'Cita para entrega'])]
              [lambda x: x.estatus == 'Activo']
              .groupby('date').size().reset_index().rename(columns={0: 'apartados_activos'})
              .sort_values(by='date')
              .assign(date=lambda x: pd.to_datetime(x.date))
              [lambda x: x.date >= (datetime.now()-timedelta(days=45))]
              )
        return df
    
    def t_apartados_historico_by_date(self,order_col='numero_de_pedido'):
        """
        """
        df = (self.lo.load_pedidos_tab()
              .groupby('date')[order_col].nunique().reset_index()
                .sort_values(by='date')
                .rename(columns={order_col:'apartados_all'})
                    )
        return df
    
    def t_product_views_by_date(self):
        """
        """
        df = (self.lo.load_product_views_tab()
              .groupby('date').agg({'page_views': 'sum', 'sku': 'nunique'})
              .rename(columns={'page_views': 'page_views_date', 'sku': 'viewed_skus'})
              .reset_index()
              .assign(date=lambda x: pd.to_datetime(x.date))
              .sort_values(by='date')
              )
        return df

    def t_terminos_de_busqueda(self):
        """
        """
        df = (self.lo.load_terminos_de_busqueda_tab()
            [['date','termino_busqueda','visits']]
            .dropna(subset=['termino_busqueda'])
            .sort_values(by=['date','visits'],ascending=[True,False])
            .groupby(['date']).head(30)
            )
        return df
    
    def t_inventory_df(self):
        """ Devuelve un df con la información de cada auto publicado.
        """
        price_buckets = [-1,150000,250e3,350e3,450e3,600e3,800e3,np.inf]
        brand_dict = {'LAND':'LAND ROVER',
                    'GENERAL':'CHEVROLET',
                    'ALFA':'ALFA ROMEO'}
        labels_buckets = ['<150k', '150k-250k', '250k-350k',
                          '350k-450k', '450k-600k', '600k-800k',
                            '800k+']
        days_bucket = [-1, 15, 30, 45, 60, 90, 120, 180, 250, 300, 360, np.inf]
        labels_days = ['0-15', '15-30', '30-45',
                        '45-60', '60-90', '90-120',
                        '120-180', '180-250', '250-300',
                        '300-360', '360+']
        
        inventory_df = (self.lo.load_vehicle_status_tab()
                        [lambda x: x.status_product.isin(['reserved', 'published']) 
                         & (x.status == 'Enabled')]
                        [['published_date', 'product_price', 'product_name', 'sku']]
                        .assign(price_bucket=lambda x: pd.cut(x.product_price, bins=price_buckets, labels=labels_buckets),
                                price_bucket_sort=lambda x: pd.cut(x.product_price, bins=price_buckets, labels=False)
                                )
                        )
        inventory_df[['brand', 'model', 'year']] = (inventory_df['product_name']
                                                    .str.extract(r'(\w+) (.+?) (\d{4})')
                                                    )
        inventory_df['brand_model'] = inventory_df['product_name'].str[:-5]

        for k in brand_dict:
            inventory_df['brand'] = inventory_df['brand'].replace(k, brand_dict[k])
        inventory_df = (inventory_df
                        .assign(days_published=lambda x: (datetime.now()-pd.to_datetime(x.published_date)).dt.days,
                                days_published_bucket=lambda x: pd.cut(
                                    x.days_published, bins=days_bucket, labels=labels_days),
                                days_published_bucket_sort=lambda x: pd.cut(x.days_published, bins=days_bucket, labels=False))
                        .drop_duplicates()
                        )
        
        return inventory_df
    
    def t_page_views_mod(self):
        """
        """
        brand_dict = {'LAND':'LAND ROVER',
              'GENERAL':'CHEVROLET',
              'ALFA':'ALFA ROMEO'}
        price_buckets = [-1,150000,250e3,350e3,450e3,600e3,800e3,np.inf]
        labels_buckets = ['<150k','150k-250k','250k-350k','350k-450k','450k-600k','600k-800k','800k+']
        page_views_mod =(self.lo.load_product_views_tab()
        [['date','id_am','page_views','sku']]
        .merge(
        self.lo.load_vehicle_status_tab()
        [['sku','product_name','product_price']]
        .drop_duplicates(),
        on='sku',
        how='left')
        .assign(price_bucket = lambda x: pd.cut(x.product_price,bins=price_buckets,labels=labels_buckets))
        )
        page_views_mod[['brand', 'model', 'year']] = page_views_mod['product_name'].str.extract(r'(\w+) (.+?) (\d{4})')
        page_views_mod['brand_model'] = page_views_mod['product_name'].str[:-5]
        for k in brand_dict:
            page_views_mod['brand'] = page_views_mod['brand'].replace(k,brand_dict[k])

        return page_views_mod
    
    def t_envios_medios_propios(self):
        """
        """
        try:
            df = (self.lo.load_envios_medios_propios()
                .rename(columns={'fecha_de_envio':'date'})
                .assign(date = lambda x: pd.to_datetime(x.date))
                .sort_values(by='date')
                [['date','canal','delivered','funnel_objetivo']]
                .groupby(['date','canal','funnel_objetivo']).sum()
                .unstack('canal').unstack('funnel_objetivo').delivered.fillna(0)
                )
            df.columns = [x[0]+'_'+x[1] for x in df.columns]
        except:
            print('No se pudo generar la tabla transformada de envios de medios propios. Se usara un default')
            df  = pd.DataFrame(columns=['date', 'push', 'email'])
        return df
    
    def t_ventas_by_date(self, reporte_ventas_raw):
        """ ventas por fecha, tomando fecha como la fecha de entrega.
        """
        rename_dict = {'fecha_de_entrega':'date'}
        df = (reporte_ventas_raw
            .pipe(process_columns)
            .rename(columns=rename_dict)
            .assign(date=lambda x: pd.to_datetime(x.date,format="%d/%m/%Y"),
                    tipo_de_venta = lambda x: x.tipo_de_venta.str.lower())
            )
        df_mod_1= (df
                .groupby(['date','tipo_de_venta'])
                .size()
                .unstack('tipo_de_venta')
                .fillna(0)
                .assign(ventas = lambda x: x.sum(axis=1))
                .rename(columns = {'contado':'ventas_contado',
                                'financiamiento':'ventas_financiamiento'})
                .reset_index()
                )
        df_mod_2 = (df
                    .assign(espacio_aux = lambda x: x.espacio_am.str.lower().str.replace('espacio ','')
                    )
                    .groupby(['date','espacio_aux'])
                    .size()
                    .unstack('espacio_aux')
                    .fillna(0)
                    .rename(columns = {'samara':'ventas_samara',
                                       'patriotismo':'ventas_patriotismo',
                                       'torre':'ventas_torre'
                                       })
                    .reset_index()

                    )
        df_mod = (df_mod_1
                .merge(df_mod_2,on='date',how='outer')
                )
        return df_mod
    
    def t_ventas_garantia_by_date(self, reporte_ventas_raw):
        """ ventas por fecha, tomando fecha como la fecha de entrega.
        """
        rename_dict = {'fecha_de_entrega':'date'}
        df = (reporte_ventas_raw
              .pipe(process_columns)
              .rename(columns=rename_dict)
              .assign(date=lambda x: pd.to_datetime(x.date,format="%d/%m/%Y"),
                      tipo_de_venta = lambda x: x.tipo_de_venta.str.lower())
              )
        gara= (df
                 [lambda x: x.garantia=='Si']
                .groupby(['date'])
                .size()
                .fillna(0)
                .reset_index()
                .rename(columns = {0:'ventas_garantia'})
                )
        return gara
    
    def t_seguros_by_date(self, reporte_ventas_raw):
        """ ventas por fecha, tomando fecha como la fecha de entrega.
        """
        rename_dict = {'fecha_de_entrega':'date'}
        df = (reporte_ventas_raw
              .assign(Seguro = lambda x: x.Seguro.fillna(0).replace('Si',1))
              .pipe(process_columns)
              .rename(columns=rename_dict)
              .assign(date=lambda x: pd.to_datetime(x.date,format="%d/%m/%Y"),
                      tipo_de_venta = lambda x: x.tipo_de_venta.str.lower())
              )
        seg= (df
                 [lambda x: x.seguro.eq(1)]
                .groupby(['date'])
                .size()
                .fillna(0)
                .reset_index()
                .rename(columns = {0:'seguro'})
                )
        return seg
    
    def t_inicio_apartado_by_date(self):
        """
        """
        df = (self.lo.load_appstep_tab()
            [lambda x: x.application_name=='pago de apartado']
            .groupby('date')
            ['app_page_visit'].sum()
            .sort_index().reset_index()
            .sort_values(by='date')
            .rename(columns={'app_page_visit':'inicio_apartado'})
            .fillna(0)
            )
        return df
    
    def t_unique_visitors_adobe_by_date(self):
        """
        """
        df = (self.lo.load_unique_visitors_adobe()
            .assign(funnel = lambda x: np.where(x.pages.str.contains('vendedor:landing'),'vendedor',
                                                np.where(x.pages.str.contains('comprador:landing'),'comprador','otro')
                                                )
                                                )
            [lambda x: x.funnel!='otro']
            .groupby(['date','funnel'])
            .unique_visitors.sum()
            .unstack('funnel')
            .sort_index().fillna(0)
            .reset_index()
            .rename(columns={'comprador':'visitas_unicas_comprador',
                             'vendedor':'visitas_unicas_vendedor'})
            )
        return df

    def t_apartados_transaccionables(self,order_col='numero_de_pedido'):
        """
        """
        df = (
                self.lo.load_pedidos_tab()
                [lambda x: x.estado.isin(status_autos_transaccionables)]
                .groupby('date')[order_col].nunique().reset_index()
                .rename(columns = {order_col : 'apartados_transaccionables'})
                )
            
        return df
    
    def t_citas_efectivas_by_date(self):
        """
        """
        status_citas_efectivas = ['completa', 'completed']
        df = (self.lo.load_citas_tab()
            [lambda x: x.status.isin(status_citas_efectivas)]
            .groupby('date',as_index=False).appointment_number.nunique()
            .assign(date = lambda x: pd.to_datetime(x.date))
            .rename(columns={'appointment_number':'citas_efectivas'})
            )
        return df
    
    def t_clientes_con_apartado_month(self):
        """
        """
        apartados_efec = (self.t_apartados_transaccionables()
                          .pipe(add_year_week)
                          .assign(month = lambda x: x.date.dt.month,
                                  last_day = lambda x: x.apply(lambda y: monthrange(y['year'],y['month'])[-1],axis=1),
                                    last_day_of_month = lambda x: x.year.astype(str)+'-'
                                                                    +x.month.astype(str).str.zfill(2)+'-'
                                                                    + x.last_day.astype(str).str.zfill(2))
            .drop(columns=['year','month','last_day'])
            .groupby('last_day_of_month').apartados_transaccionables.sum()
            .reset_index()
                          )
        apartados_all = (self.t_apartados_historico_by_date()
                          .pipe(add_year_week)
                          .assign(month = lambda x: x.date.dt.month,
                                  last_day = lambda x: x.apply(lambda y: monthrange(y['year'],y['month'])[-1],axis=1),
                                    last_day_of_month = lambda x: x.year.astype(str)+'-'
                                                                    +x.month.astype(str).str.zfill(2)+'-'
                                                                    + x.last_day.astype(str).str.zfill(2))
            .drop(columns=['year','month','last_day'])
            .groupby('last_day_of_month').apartados_all.sum()
            .reset_index()
                          )


        df = (self.lo.load_pedidos_tab()
                .assign(tipo_apartado = lambda x: np.where(x.estado.isin(status_autos_transaccionables),'transaccionable','normal'),
                        month = lambda x: pd.to_datetime(x.date).dt.month)
                .groupby(['year','month','id_am_comprador','tipo_apartado']).numero_de_pedido.nunique()
                .unstack('tipo_apartado').fillna(0).gt(0).multiply(1)
                .assign(flag_apartado = lambda x: x.sum(axis=1).gt(0)*1)
                .reset_index().groupby(['year','month'],as_index=False)[['transaccionable','normal','flag_apartado']].sum()
                .rename(columns={'flag_apartado':'clientes_con_apartado',
                                'transaccionable':'clientes_con_apartado_transaccionable',
                                'normal':'clientes_con_apartado_no_transaccionable'}
                                )
                .assign(last_day = lambda x: x.apply(lambda y: monthrange(y['year'].astype(int),y['month'].astype(int))[-1],axis=1),
                    last_day_of_month = lambda x: x.year.astype(str)+'-'
                                                    +x.month.astype(str).str.zfill(2)+'-'
                                                    + x.last_day.astype(str).str.zfill(2))
            .drop(columns=['year','month','last_day'])
            .merge(apartados_efec, on='last_day_of_month', how='left')
            .merge(apartados_all, on='last_day_of_month', how='left')
                )
        if df.last_day_of_month.max()>self.lo.today:
            df.at[df.shape[0]-1,'last_day_of_month'] = (pd.to_datetime(self.lo.today)-timedelta(days=1)).strftime('%Y-%m-%d')
    
            
        return df
    
    def t_registros_tipo_by_date(self):
        """
        """
        df=(self.lo.load_cta_adobe()
            [lambda x: (x.application_name=='crear cuenta')&
             (x['application_step']=='crear cuenta:app completed:pagina exitosa')
             &(x.cta.fillna('').str.startswith('crear cuenta:app on click start:registro'))]
            .assign(type_registro = lambda x: x['cta'].apply(lambda y: y.split(':')[2]))
            .groupby(['date','type_registro'])['apps_completed'].sum()
            .unstack('type_registro')
            .fillna(0).sort_index()
            .reset_index()
            .pipe(process_columns)
            )
        return df
    
    def t_salud_inventario(self):
        """
        """
        cols_salud_inventario = ['sku_key',
                                'id_producto',
                                'date',
                                'fecha_de_modificacion',
                                'estatus_final',
                                'estatus_final_producto',
                                'dias_en_inventario',
                                'semana_anio_cierre',
                                'semana_cierre',
                                'anio_cierre',
                                'fecha_cierre',
                                'mes_cierre',
                                'mes_cierre_nombre',
                                'nivel_comercialidad',
                                'mercado',
                                'dias_en_inventario_rangos',
                                'product_id',
                                'published_date',
                                'sunday_of_week_fecha_de_modificacion',
                                'sunday_of_week_published_date'
                                ]
        vs_commerce = (self.lo.load_vehicle_status_tab()
                    [['product_id', 'published_date']]
                    )
        #vs_commerce['sku_com'] = vs_commerce['sku'].str.split('-', n=2).str[:2].str.join('-')

        salud_inventario = self.lo.load_salud_inventario()
        salud_inventario = salud_inventario.merge(vs_commerce[['product_id', 'published_date']], 
                                                left_on='id_producto', 
                                                right_on='product_id', 
                                                how='left')
        
        salud_inventario['fecha_de_modificacion'] = pd.to_datetime(salud_inventario['fecha_de_modificacion'])
        salud_inventario['published_date'] = pd.to_datetime(salud_inventario['published_date'])

        salud_inventario['sunday_of_week_fecha_de_modificacion'] = (salud_inventario['fecha_de_modificacion'] - 
                                        pd.to_timedelta(salud_inventario['fecha_de_modificacion'].dt.weekday + 1, unit='d'))

        salud_inventario['sunday_of_week_fecha_de_modificacion'] = (salud_inventario
                            .apply(
                            lambda row: row['fecha_de_modificacion'] if row['fecha_de_modificacion'].weekday(
                            ) == 6 else row['sunday_of_week_fecha_de_modificacion'],
                            axis=1
                                )
        )
        salud_inventario['sunday_of_week_published_date'] = (salud_inventario['published_date'] - 
                        pd.to_timedelta(salud_inventario['published_date'].dt.weekday + 1, unit='d'))

        salud_inventario['sunday_of_week_published_date'] = (salud_inventario
                            .apply(
                            lambda row: row['published_date'] if row['published_date'].weekday(
                            ) == 6 else row['sunday_of_week_published_date'],
                            axis=1
                                )
        )

        salud_inventario['fecha_de_modificacion'] = pd.to_datetime(salud_inventario['fecha_de_modificacion']).dt.strftime("%Y-%m-%d")
        salud_inventario['published_date'] = pd.to_datetime(salud_inventario['published_date']).dt.strftime("%Y-%m-%d")
        salud_inventario['sunday_of_week_fecha_de_modificacion'] = pd.to_datetime(salud_inventario['sunday_of_week_fecha_de_modificacion']).dt.strftime("%Y-%m-%d")
        salud_inventario['sunday_of_week_published_date'] = pd.to_datetime(salud_inventario['sunday_of_week_published_date']).dt.strftime("%Y-%m-%d")
        
        
        return salud_inventario[cols_salud_inventario]
    
    def t_registros_funnel_vendedor(self):
        """
        """
        res = (self.lo.load_appstep_tab()
                [lambda x: x.application_name=='vender mi auto']
                .groupby('date',as_index=False)['app_step_4'].sum()
                .rename(columns={'app_step_4':'registros_funnel_vendedor_apst4'})
                .sort_values(by='date')
                )
        return res
    
    def t_business_case_vs_kpis_month(self,master_table,business_case_df):
        """
        """
        bc_cols = ['date','ventas','ventas_financiamiento','ventas_contado','ventas_garantia','seguro',
           'publicaciones_all','apartados_all','inicio_publicacion','inicio_apartado',
           'new_users',
           'visitas_unicas_comprador','visitas_unicas_vendedor']
        month_master_table = (master_table
        [bc_cols]
        .assign(month = lambda x: pd.to_datetime(x.date).apply(lambda x: x.strftime('%Y-%m-01')))
        .drop(columns='date')
        .groupby('month',as_index=False).sum()
        )
        month_master_table.columns = ['obs_'+c if c!='month' else c for c in month_master_table.columns ]
        res = (business_case_df
        .assign(month = lambda x: pd.to_datetime(x.Fecha,format = '%d/%m/%Y').dt.strftime('%Y-%m-%d'))
        .merge(month_master_table,on='month',how='left')
        )
        return res
    
    def t_business_case_by_date(self,business_case_df):
        """
        """
        bcdf = business_case_df.copy()
        bc_cols = ['Fecha',
          'Visitas Únicas Vendedor',
          'Inicio de funnel Vendedor ',
          'Publicaciones',
          'Apartados / Publicaciones',
          'Apartados',
          'Transacciones',
          'Registro',
          'Visitas Únicas Comprador',
          'Inicio de funnel Comprador',
          'Financiamientos',
          'Garantías Estéticas',
          'Garantías Extendidas',
          'Seguros de auto'
        ]
        aux = (bcdf[bc_cols]
            .pipe(process_columns)
            .assign(days_in_month = lambda x: pd.to_datetime(x.fecha,format="%d/%m/%Y").dt.days_in_month,
                    year_month = lambda x: pd.to_datetime(x.fecha,format="%d/%m/%Y").dt.strftime('%Y-%m'))
            
            )
        aux = (aux
            .assign(**{c:aux[c]/aux['days_in_month'] for c in aux.columns if c not in ['fecha','days_in_month','year_month']})
            .drop(columns=['fecha','days_in_month'])
            )
        aux.columns = ['bc_' + c if c!='year_month' else c for c in aux.columns]
        return aux
    
    def t_app_on_click_start_apartado(self):
        """
        """
        df = (self.lo.load_appstep_tab()
            [lambda x: x.application_name=='pago de apartado']
            .groupby('date')
            ['app_click_start'].sum()
            .sort_index().reset_index()
            .sort_values(by='date')
            .rename(columns={'app_click_start':'start_apartado'})
            .fillna(0)
            )
        return df

    def t_unique_visitors_pdp(self):
        """
        """
        df = (self.lo.load_unique_visitors_adobe()
            [lambda x: x.pages=='escritorio:publica:comprador:pdp']
            .groupby('date').agg({'unique_visitors':'sum'})
            .sort_index().reset_index()
            .sort_values(by='date')
            .rename(columns={'unique_visitors':'unique_pdp'})
            .fillna(0)
            )
        return df
            

    def t_pedidos_intermedia(self):
        """
        """
        multiapartado_days_window = 20
        pedidos_intermedia = (self.lo.load_pedidos_tab()
                                [['numero_de_pedido','pedido_id_comercio_externo','id_am_comprador',
                                'id_am_vendedor','fecha_de_creacion','estado','estatus',
                                'activo_vehiculo_id_numero_de_identificacion_vehicular_(niv)']]
                                .assign(transaccionable = lambda x: (x.estado.isin(status_autos_transaccionables))*1)
                                .rename(columns = {'pedido_id_comercio_externo':'order_id',
                                                    'activo_vehiculo_id_numero_de_identificacion_vehicular_(niv)':'vin'})
                                .sort_values(by=['id_am_comprador','fecha_de_creacion'],ascending=[True,True])
                                .assign(days_since_last_order = lambda x: ((x.groupby('id_am_comprador')['fecha_de_creacion'].diff().dt.days)),
                                        orders_by_id_comprador = lambda x: x.groupby('id_am_comprador')['numero_de_pedido'].transform('nunique'),
                                        multiapartado = lambda x: np.where(x.days_since_last_order<=multiapartado_days_window,1,0))
                                .sort_values(by=['fecha_de_creacion','order_id'],ascending=[True,True])
                                )
        return pedidos_intermedia


    def t_multiapartados_by_date(self):
        """Multiapartado es un apartado en el cual el número de días desde el apartado anterior del mismo usuario, es menor o igual a 60 días
        """
        intermedia = self.t_pedidos_intermedia()
        res = (intermedia
               .rename(columns = {'fecha_de_creacion':'date'})
               .groupby('date')[['multiapartado']].sum()
               .sort_index()
               .reset_index()
               )
        return res
    
    def t_publicaciones_por_espacio(self):
        """
        """
        publicaciones_por_espacio=(self.lo.load_vehicle_status_tab()
                [['id_am','product_id','published_date',
                  'product_price','sku','vin','plate','product_name','showroom']]
                .assign(showroom = lambda x: x.showroom.str.lower())
                .assign(showroom = lambda x: np.where(x.showroom.str.contains('triotismo'),
                                                                              'patriotismo',
                                                        np.where(x.showroom.str.contains('eforma'),
                                                                 'torre bbva',
                                                                 np.where(x.showroom.str.contains('amara'),
                                                                          'samara',
                                                                          x.showroom))
                                                          )
                                                      )
                        
                .rename(columns={'published_date':'date'})
                .groupby(['date','showroom']).sku.nunique()
                .unstack(level=1).fillna(0)
                .reset_index()
                .pipe(process_columns)
                .assign(date = lambda x: pd.to_datetime(x.date))
                )
        publicaciones_por_espacio.columns = ['publicaciones_'+x if x!='date' else x for x in publicaciones_por_espacio.columns ]
        publicaciones_por_espacio = publicaciones_por_espacio[['date','publicaciones_patriotismo','publicaciones_torre_bbva','publicaciones_samara']]
        return publicaciones_por_espacio
    
    def t_cancelaciones_by_date(self):
        """
        """
        df = self.lo.load_cancelaciones()
        res = (df
            .rename(columns={'cancelled_at':'date'})
            .groupby('date').sku.nunique()
            .sort_index()
            .reset_index()
            .assign(date = lambda x: pd.to_datetime(x.date))
            .rename(columns={'sku':'cancelaciones'})
            )
        return res
    
    def t_unique_visitors_plp(self):
        """
        """
        df = (self.lo.load_unique_visitors_adobe()
            [lambda x: x.pages=='escritorio:publica:comprador:plp']
            .groupby('date').agg({'unique_visitors':'sum'})
            .sort_index().reset_index()
            .rename(columns={'unique_visitors':'unique_plp'})
            .assign(date = lambda x: pd.to_datetime(x.date))
            .fillna(0)
            )
        return df
