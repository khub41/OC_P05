# -*- coding: utf-8 -*-
"""
Created on Fri May 21 11:20:55 2021

@author: S053567
"""

import pandas as pd
pd.set_option("mode.chained_assignment", None)
import numpy as np
import json


def get_orders(row, customers, orders):
    
    
    """
    A partir d'un id client unique la fonction retourne la liste des commandes correspondantes,
    La table des commandes doit au préalable être filtrée en fonction de la date virtuelle de l'étude
    Args:
        client_unique_id
    """
    
    
    # Avec l'id unique du client on récupère les id clients qui sont liés à chaque commande
    ids_client = customers[customers['customer_unique_id'] == row.name].customer_id.values
    # On peut ensuite récupérer les id des commandes correspondant à ces ids clients
    orders_client = orders[orders.customer_id.isin(ids_client)]
    # On récupère juste les ids
    ids_orders_client = orders_client.index.values
    return ids_orders_client

def get_nb_days(row, virtual_date, orders):
    """
    Cette fonction calcule le nombre de jours écoulés depuis la première et la dernière 
    commande en prenant pour référence la date virtuelle de l'étude.
    Args :
        row: Pandas DataFrame row (Series) avec un attribut "orders_ids"
        Timestamp 
        Pandas DataFrame : avec les données des commandes
    Returns :
        Tuple
    """
    orders_ids = row.orders_ids
    
    date_first_order = orders.loc[orders_ids[0], 'order_purchase_timestamp']
    date_last_order = orders.loc[orders_ids[-1], 'order_purchase_timestamp']
    
    since_first = (virtual_date - date_first_order).days
    since_last = (virtual_date - date_last_order).days
    return since_first, since_last

def get_frequency(row):
    """
    Calcul du nombre de commandes par mois entre la première commande
    et la date virtuelle.
    """
    return row.nb_orders / row.days_since_first_order * 365.25 / 12


def get_sum_orders(row, order_items):
    """
    Calcule la somme des montants des commandes
    """
    return order_items[order_items.index.isin(row.orders_ids)].price.sum()


def get_most_frequent_categ(row, order_items, products):
    """
    Retourne la catégorie qui revient le plus souvent dans les achats. 
    En cas d'égalité on prend la première donnée par value_counts()
    """
    

    products_ids = order_items[order_items.index.isin(row.orders_ids)].product_id
    products_client = products[products.index.isin(products_ids)]

    try:
        return products_client.product_category_name.value_counts().index[0]
    except:
        # print('fail')
        if len(products_client.product_category_name.value_counts().index) > 1:
            return products_client.product_category_name.value_counts().index[1]
        else :
            return np.nan
        
def get_nb_reviews_and_avg(row, reviews):
    """
    Recupère le nombre de reviews et la note moyenne
    """
    orders_ids = row.orders_ids
    client_reviews = reviews[reviews.order_id.isin(orders_ids)]
    if len(client_reviews) > 0:
        return len(client_reviews), client_reviews.review_score.mean()
    else:
        return 0, np.nan

def get_favourite_payment_method(row, payments):
    """
    Retourne le type de paiement préféré du client
    """
    orders_ids = row.orders_ids
    payment_methods = payments[payments.order_id.isin(orders_ids)].payment_type.value_counts()
    return payment_methods.index[0]

def get_delivery_time_and_delays(row, orders):
    """
    Pour les clients ayant eu des commandes ayant abouties, on calcule le taux d'avance, de retard et le temps moyen de livraison
    """
    client_orders = orders[orders.index.isin(row.orders_ids)]
    client_orders = client_orders[client_orders.order_status == 'delivered']
    
    if client_orders.shape[0] > 0:
        was_delayed_counts = client_orders.was_delayed.value_counts(normalize=True)
        if True in was_delayed_counts.index:
            delay_rate = was_delayed_counts.loc[True]
        else:
            delay_rate = 0
        was_in_advance_counts = client_orders.was_in_advance.value_counts(normalize=True)
        if True in was_in_advance_counts.index:
            advance_rate = was_in_advance_counts.loc[True]
        else:
            advance_rate = 0
        return client_orders.delivery_time.mean(), delay_rate, advance_rate
    else:
        np.nan, np.nan, np.nan

def get_cancelation_rate(row, orders):
    client_orders = orders[orders.index.isin(row.orders_ids)]
    status_counts = client_orders.order_status.value_counts(normalize=True)
    if 'canceled' in status_counts.index:
        return status_counts.loc['canceled']
    else:
        return 0
    
def main(virtual_date='default'):
    
    # Import data
    orders = pd.read_csv('data/olist_orders_dataset.csv', index_col='order_id',parse_dates=['order_purchase_timestamp', 
                                                                                        'order_approved_at',	
                                                                                        'order_delivered_carrier_date',	
                                                                                        'order_delivered_customer_date',
                                                                                        'order_estimated_delivery_date'])
    order_items = pd.read_csv('data/olist_order_items_dataset.csv',  index_col='order_id')
    products = pd.read_csv('data/olist_products_dataset.csv', index_col='product_id', na_values=np.nan)
    customers = pd.read_csv('data/olist_customers_dataset.csv')
    reviews = pd.read_csv('data/olist_order_reviews_dataset.csv')
    geolocs = pd.read_csv('data/olist_geolocation_dataset.csv')
    payments = pd.read_csv('data/olist_order_payments_dataset.csv')
    products_categ_traduction = pd.read_csv('data/product_category_name_translation.csv')
    # Récupération traduction en anglais
    products = products.merge(products_categ_traduction, how='left').set_axis(products.index)

    # On se fixe à une date et on filtre les données après celle ci
    if virtual_date == 'default':
        virtual_date = orders.order_purchase_timestamp.max()
        
    orders = orders[orders.order_purchase_timestamp <= virtual_date]

    orders = orders.merge(customers[['customer_id', 'customer_unique_id']], how='left').set_axis(orders.index)
    
    # Tri par date
    orders.sort_values('order_purchase_timestamp', inplace=True)

    # Initialisation d'un nouveau dataframe
    data = pd.DataFrame(orders.groupby('customer_unique_id').count().customer_id)
    data.columns = ['nb_orders']
    
    # On filtre les clients n'ayant fait qu'une commande
    data = data[data.nb_orders > 1]
    
    # Recuperation des ids des commandes sous forme de listes
    data['orders_ids'] = data.apply(get_orders, 
                                    customers=customers,
                                    orders=orders,
                                    axis=1)
    # Temps depuis la première et la dernière commande ⏲️
    data[['days_since_first_order', 
          'days_since_last_order']] = data.apply(get_nb_days, 
                                                 virtual_date=virtual_date, 
                                                 orders=orders, 
                                                 axis=1, 
                                                 result_type='expand')

    # Fréquence des commandes
    data['frequency'] = data.apply(get_frequency, 
                                   axis=1)
                                      
    # Montant total des achats 💰
    data['sum_orders'] = data.apply(get_sum_orders, 
                                    order_items=order_items,
                                    axis=1)

    # Catégogie la plus fréquente 🗂️
    # Reduction cardinalité
    
    data['favourite_category'] = data.apply(get_most_frequent_categ,
                                            order_items=order_items,
                                            products=products,
                                            axis=1)
    data = data.merge(products_categ_traduction, 
                      how='left', 
                      left_on='favourite_category', 
                      right_on='product_category_name').set_axis(data.index)
    
    data.drop(columns=['product_category_name'], inplace=True)
    data.rename(columns={'product_category_name_english':'favourite_category_english'}, inplace=True)
    
    reduce_cardinality = open("reduce_cardinality.json", "r")
    reduce_cardinality = reduce_cardinality.read()
    reduce_cardinality = json.loads(reduce_cardinality)
    
    for i in range(4):
        # A faire plusieurs fois pour qu'il soit totalement efficace
        data.favourite_category_english = data.favourite_category_english.replace(reduce_cardinality)
    
    
    # Nombre d'avis postés et note moyenne⭐
    data[['nb_reviews', 'average_review_score']] = data.apply(get_nb_reviews_and_avg,
                                                              reviews=reviews,
                                                              axis=1, 
                                                              result_type='expand')

    
    # Mode de paiement préféré 💳
    data['favourite_payment_type'] = data.apply(get_favourite_payment_method, 
                                                payments=payments,
                                                axis=1).replace('boleto', 'cash')
    
    # Temps et retards de livraison 🚚
    ## Prétraitenement de la base orders
    orders['delivery_time'] = orders.order_delivered_customer_date - orders.order_approved_at
    orders['delay'] = orders.order_delivered_customer_date - orders.order_estimated_delivery_date
    orders['was_delayed'] = orders.delay.map(lambda x : x.days > 1)
    orders['was_in_advance'] = orders.delay.map(lambda x : x.days < -1)
    
    ## Recupération des données
    data[['average_delivery_time', 
          'delay_rate', 
          'advance_rate']] = data.apply(get_delivery_time_and_delays,
                                        orders=orders,
                                        axis=1, 
                                        result_type='expand')
    ## Traitement des valeurs non renseingées                                    
    data.delay_rate.fillna(0, inplace=True)
    data.advance_rate.fillna(0, inplace=True)                               
    data.average_delivery_time.fillna(data.average_delivery_time.mean(), inplace=True)

    # Commandes annulées ❌
    data['cancelation_rate'] = data.apply(get_cancelation_rate, 
                                          orders=orders,
                                          axis=1)
    
    
    # Simplification de la donnée temporelle (Average delivery time)
    data.average_delivery_time = data.average_delivery_time.map(lambda x : x.round('d').days)
    
    # Conversion des données catégoriques en données numériques
    data = pd.concat([data, 
                      pd.get_dummies(data.favourite_category_english), 
                      pd.get_dummies(data.favourite_payment_type)
                      ],
                     axis=1)
    
    # Suppression des colonnes qui ne sont pas utiles pour la modélisation
    data.drop(columns=['orders_ids', 
                       'favourite_category', 
                       'favourite_category_english', 
                       'favourite_payment_type'],
              inplace=True)
    
    return data
    
    
if __name__ == '__main__':
    main()    
    
    
    
    
    
    
    
    
    
    
    
    