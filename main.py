import os
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from datetime import datetime, timedelta
import pprint

load_dotenv()
uri = os.getenv('MONGO_URI')
client = MongoClient(uri, server_api=ServerApi('1'))
db = client["sample_analytics"]

try:
    client.admin.command('ping')
    print("Conectado a MongoDB")
except Exception as e:
    print(f"Error: {e}")

customers = db["customers"]
accounts = db["accounts"]
transactions_col = db["transactions"]  # Coleccion separada de transacciones

#  FECHA MAXIMA GLOBAL 
print("\nCalculando fecha maxima de transacciones...")
pipeline_fecha_max = [
    {"$unwind": "$transactions"},
    {"$group": {"_id": None, "max_fecha": {"$max": "$transactions.date"}}}
]
result_fecha = list(transactions_col.aggregate(pipeline_fecha_max))
if result_fecha:
    fecha_max_global = result_fecha[0]["max_fecha"]
    print(f"   Fecha mas reciente: {fecha_max_global}")
else:
    fecha_max_global = datetime.now()
    print("   No se encontraron transacciones, se usara fecha actual.")

fecha_limite_6m = fecha_max_global - timedelta(days=180)
fecha_limite_12m = fecha_max_global - timedelta(days=365)

# EJERCICIO 2.1 
print("\n" + "="*50)
print("2.1 Transacciones por cliente")
print("="*50)

pipeline_21 = [
    {"$unwind": "$transactions"},
    {"$group": {
        "_id": "$account_id",
        "num_transacciones_cuenta": {"$sum": 1},
        "suma_total_cuenta": {"$sum": {"$toDouble": "$transactions.total"}}
    }},
    {"$lookup": {
        "from": "customers",
        "localField": "_id",
        "foreignField": "accounts",
        "as": "clientes"
    }},
    {"$unwind": {"path": "$clientes", "preserveNullAndEmptyArrays": True}},
    {"$group": {
        "_id": "$clientes._id",
        "nombre_completo": {"$first": "$clientes.name"},
        "direccion": {"$first": "$clientes.address"},
        "num_transacciones_total": {"$sum": "$num_transacciones_cuenta"},
        "suma_total_cliente": {"$sum": "$suma_total_cuenta"}
    }},
    {"$addFields": {
        "promedio_monto": {
            "$cond": {
                "if": {"$gt": ["$num_transacciones_total", 0]},
                "then": {"$divide": ["$suma_total_cliente", "$num_transacciones_total"]},
                "else": 0
            }
        },
        "ciudad": {
            "$cond": {
                "if": {"$isArray": {"$split": ["$direccion", ","]}},
                "then": {
                    "$trim": {
                        "input": {
                            "$arrayElemAt": [
                                {"$split": ["$direccion", ","]},
                                {"$max": [
                                    {"$subtract": [
                                        {"$size": {"$split": ["$direccion", ","]}}, 
                                        2
                                    ]}, 
                                    0
                                ]}
                            ]
                        }
                    }
                },
                "else": "DESCONOCIDA"
            }
        }
    }},
    {"$project": {
        "nombre_completo": 1,
        "ciudad": 1,
        "num_transacciones": "$num_transacciones_total",
        "promedio_monto": {"$round": ["$promedio_monto", 2]}
    }},
    {"$sort": {"num_transacciones": -1}}
]

resultados_21 = list(transactions_col.aggregate(pipeline_21))
print(f"Total clientes con transacciones: {len(resultados_21)}")
for r in resultados_21[:10]:
    print(f"{r['nombre_completo']} - {r['ciudad']} - Transacciones: {r['num_transacciones']} - Promedio: ${r['promedio_monto']:.2f}")

# EJERCICIO 2.2 
print("\n" + "="*50)
print("2.2 Clasificacion de clientes por balance total")
print("="*50)

pipeline_22 = [
    {"$unwind": "$transactions"},
    {"$group": {
        "_id": "$account_id",
        "balance_cuenta": {"$sum": {"$toDouble": "$transactions.total"}}
    }},
    {"$lookup": {
        "from": "customers",
        "localField": "_id",
        "foreignField": "accounts",
        "as": "cliente"
    }},
    {"$unwind": "$cliente"},
    {"$group": {
        "_id": "$cliente._id",
        "nombre": {"$first": "$cliente.name"},
        "balance_total": {"$sum": "$balance_cuenta"}
    }},
    {"$addFields": {
        "categoria": {
            "$switch": {
                "branches": [
                    {"case": {"$lt": ["$balance_total", 5000]}, "then": "Bajo"},
                    {"case": {"$and": [
                        {"$gte": ["$balance_total", 5000]},
                        {"$lte": ["$balance_total", 20000]}
                    ]}, "then": "Medio"},
                    {"case": {"$gt": ["$balance_total", 20000]}, "then": "Alto"}
                ],
                "default": "Sin clasificar"
            }
        }
    }},
    {"$project": {
        "nombre": 1,
        "categoria": 1,
        "balance_total": {"$round": ["$balance_total", 2]}
    }},
    {"$sort": {"balance_total": -1}}
]

resultados_22 = list(transactions_col.aggregate(pipeline_22))
print(f"Total clientes clasificados: {len(resultados_22)}")
for r in resultados_22[:15]:
    print(f"{r['nombre']} - Categoria: {r['categoria']} - Balance: ${r['balance_total']:,.2f}")

# EJERCICIO 2.3 
print("\n" + "="*50)
print("2.3 Cliente con mayor balance por ciudad")
print("="*50)

pipeline_23 = [
    {"$unwind": "$transactions"},
    {"$group": {
        "_id": "$account_id",
        "balance_cuenta": {"$sum": {"$toDouble": "$transactions.total"}}
    }},
    {"$lookup": {
        "from": "customers",
        "localField": "_id",
        "foreignField": "accounts",
        "as": "cliente"
    }},
    {"$unwind": "$cliente"},
    {"$group": {
        "_id": "$cliente._id",
        "nombre": {"$first": "$cliente.name"},
        "direccion": {"$first": "$cliente.address"},
        "balance_total": {"$sum": "$balance_cuenta"}
    }},
    {"$addFields": {
        "ciudad": {
            "$cond": {
                "if": {"$isArray": {"$split": ["$direccion", ","]}},
                "then": {
                    "$trim": {
                        "input": {
                            "$arrayElemAt": [
                                {"$split": ["$direccion", ","]},
                                {"$max": [
                                    {"$subtract": [
                                        {"$size": {"$split": ["$direccion", ","]}}, 
                                        2
                                    ]}, 
                                    0
                                ]}
                            ]
                        }
                    }
                },
                "else": "DESCONOCIDA"
            }
        }
    }},
    {"$sort": {"ciudad": 1, "balance_total": -1}},
    {"$group": {
        "_id": "$ciudad",
        "nombre": {"$first": "$nombre"},
        "total": {"$first": "$balance_total"}
    }},
    {"$project": {
        "ciudad": "$_id",
        "nombre": 1,
        "total": {"$round": ["$total", 2]},
        "_id": 0
    }},
    {"$sort": {"total": -1}}
]

resultados_23 = list(transactions_col.aggregate(pipeline_23))
print(f"Total ciudades con clientes: {len(resultados_23)}")
for r in resultados_23[:15]:
    print(f"Ciudad: {r['ciudad']} - Cliente: {r['nombre']} - Total: ${r['total']:,.2f}")

# EJERCICIO 2.4 
print("\n" + "="*50)
print("2.4 Top 10 transacciones ultimos 6 meses (desde fecha maxima)")
print("="*50)

pipeline_24 = [
    {"$unwind": "$transactions"},
    {"$match": {"transactions.date": {"$gte": fecha_limite_6m}}},
    {"$addFields": {"monto_num": {"$toDouble": "$transactions.total"}}},
    {"$sort": {"monto_num": -1}},
    {"$limit": 10},
    {"$lookup": {
        "from": "customers",
        "localField": "account_id",
        "foreignField": "accounts",
        "as": "cliente"
    }},
    {"$unwind": "$cliente"},
    {"$project": {
        "monto": {"$round": ["$monto_num", 2]},
        "fecha": "$transactions.date",
        "tipo": "$transactions.transaction_code",
        "simbolo": "$transactions.symbol",
        "cliente_nombre": "$cliente.name",
        "cliente_email": "$cliente.email",
        "cuenta": "$account_id",
        "_id": 0
    }}
]

resultados_24 = list(transactions_col.aggregate(pipeline_24))
print(f"Total transacciones encontradas: {len(resultados_24)}")
for i, r in enumerate(resultados_24, 1):
    print(f"{i}. ${r['monto']:,.2f} - {r['fecha'].strftime('%Y-%m-%d')} - {r['tipo']} - {r['simbolo']}")
    print(f"   Cliente: {r['cliente_nombre']} - {r['cliente_email']}\n")

# EJERCICIO 2.6 
print("\n" + "="*50)
print("2.6 Transacciones por mes y tipo")
print("="*50)

pipeline_26 = [
    {"$unwind": "$transactions"},
    {"$addFields": {
        "mes": {"$dateToString": {"format": "%Y-%m", "date": "$transactions.date"}},
        "tipo": "$transactions.transaction_code",
        "monto_num": {"$toDouble": "$transactions.total"}
    }},
    {"$group": {
        "_id": {"mes": "$mes", "tipo": "$tipo"},
        "total_monto": {"$sum": "$monto_num"},
        "num_transacciones": {"$sum": 1}
    }},
    {"$addFields": {
        "promedio_monto": {"$divide": ["$total_monto", "$num_transacciones"]}
    }},
    {"$project": {
        "mes": "$_id.mes",
        "tipo": "$_id.tipo",
        "total_monto": {"$round": ["$total_monto", 2]},
        "num_transacciones": 1,
        "promedio_monto": {"$round": ["$promedio_monto", 2]},
        "_id": 0
    }},
    {"$sort": {"mes": 1, "tipo": 1}}
]

resultados_26 = list(transactions_col.aggregate(pipeline_26))
print(f"Total grupos: {len(resultados_26)}")
for r in resultados_26:
    print(f"Mes: {r['mes']} - Tipo: {r['tipo']}")
    print(f"   Total: ${r['total_monto']:,.2f} - Promedio: ${r['promedio_monto']:.2f} - #{r['num_transacciones']} transacciones")

# EJERCICIO 2.7 
print("\n" + "="*50)
print("2.7 Clientes inactivos")
print("="*50)


cuentas_con_transacciones = transactions_col.aggregate([
    {"$unwind": "$transactions"},
    {"$group": {"_id": "$account_id"}}
])
cuentas_activas = [doc["_id"] for doc in cuentas_con_transacciones]

pipeline_27 = [
    {"$match": {}},
    {"$addFields": {
        "tiene_transacciones": {
            "$anyElementTrue": {
                "$map": {
                    "input": "$accounts",
                    "as": "cuenta",
                    "in": {"$in": ["$$cuenta", cuentas_activas]}
                }
            }
        }
    }},
    {"$match": {"tiene_transacciones": False}},
    {"$project": {
        "username": 1,
        "name": 1,
        "email": 1,
        "address": 1,
        "accounts": 1,
        "_id": 1
    }}
]

inactivos = list(customers.aggregate(pipeline_27))
db["inactive_customers"].drop()
if inactivos:
    db["inactive_customers"].insert_many(inactivos)
    print(f" {len(inactivos)} clientes inactivos guardados en 'inactive_customers'")
    for c in inactivos[:5]:
        print(f"  - {c['name']} - {c['email']} - Cuentas: {c['accounts']}")
else:
    print("No se encontraron clientes inactivos")

#  EJERCICIO 2.8 
print("\n" + "="*50)
print("2.8 Resumen por tipo de cuenta")
print("="*50)

pipeline_28 = [
    {"$unwind": "$products"},
    {"$lookup": {
        "from": "transactions",
        "let": {"cuenta_id": "$account_id"},
        "pipeline": [
            {"$unwind": "$transactions"},
            {"$match": {"$expr": {"$eq": ["$account_id", "$$cuenta_id"]}}},
            {"$group": {
                "_id": None,
                "balance": {"$sum": {"$toDouble": "$transactions.total"}}
            }}
        ],
        "as": "balance_info"
    }},
    {"$addFields": {
        "balance": {
            "$ifNull": [{"$arrayElemAt": ["$balance_info.balance", 0]}, 0]
        }
    }},
    {"$group": {
        "_id": "$products",
        "total_cuentas": {"$sum": 1},
        "balance_total": {"$sum": "$balance"}
    }},
    {"$addFields": {
        "balance_promedio": {"$divide": ["$balance_total", "$total_cuentas"]}
    }},
    {"$project": {
        "tipo_cuenta": "$_id",
        "total_cuentas": 1,
        "balance_promedio": {"$round": ["$balance_promedio", 2]},
        "balance_total": {"$round": ["$balance_total", 2]},
        "_id": 0
    }}
]

resumen_tipos = list(accounts.aggregate(pipeline_28))
db["account_summaries"].drop()
if resumen_tipos:
    db["account_summaries"].insert_many(resumen_tipos)
    print(" Resumen guardado en 'account_summaries'")
    for r in resumen_tipos:
        print(f"Tipo: {r['tipo_cuenta']}")
        print(f"   Cuentas: {r['total_cuentas']} - Balance Total: ${r['balance_total']:,.2f} - Promedio: ${r['balance_promedio']:,.2f}")
else:
    print("No se encontraron tipos de cuenta")

#  EJERCICIO 2.9 
print("\n" + "="*50)
print("2.9 Clientes de alto valor")
print("="*50)

pipeline_29 = [
    {"$unwind": "$transactions"},
    {"$group": {
        "_id": "$account_id",
        "balance_cuenta": {"$sum": {"$toDouble": "$transactions.total"}},
        "transacciones_cuenta": {"$sum": 1}
    }},
    {"$lookup": {
        "from": "customers",
        "localField": "_id",
        "foreignField": "accounts",
        "as": "cliente"
    }},
    {"$unwind": "$cliente"},
    {"$group": {
        "_id": "$cliente._id",
        "nombre": {"$first": "$cliente.name"},
        "email": {"$first": "$cliente.email"},
        "balance_total": {"$sum": "$balance_cuenta"},
        "transacciones_total": {"$sum": "$transacciones_cuenta"}
    }},
    {"$match": {
        "balance_total": {"$gt": 30000},
        "transacciones_total": {"$gt": 5}
    }},
    {"$project": {
        "nombre": 1,
        "email": 1,
        "balance_total": {"$round": ["$balance_total", 2]},
        "transacciones_total": 1,
        "_id": 0
    }},
    {"$sort": {"balance_total": -1}}
]

alto_valor = list(transactions_col.aggregate(pipeline_29))
db["high_value_customers"].drop()
if alto_valor:
    db["high_value_customers"].insert_many(alto_valor)
    print(f" {len(alto_valor)} clientes de alto valor guardados en 'high_value_customers'")
    for c in alto_valor[:10]:
        print(f"  {c['nombre']} - {c['email']} - Balance: ${c['balance_total']:,.2f} - Transacciones: {c['transacciones_total']}")
else:
    print("No se encontraron clientes de alto valor")

#  EJERCICIO 2.10 
print("\n" + "="*50)
print("2.10 Promedio mensual ultimo año y clasificacion")
print("="*50)

pipeline_210 = [
    {"$unwind": "$transactions"},
    {"$match": {"transactions.date": {"$gte": fecha_limite_12m}}},
    {"$addFields": {
        "mes": {"$dateToString": {"format": "%Y-%m", "date": "$transactions.date"}}
    }},
    {"$group": {
        "_id": {"account_id": "$account_id", "mes": "$mes"},
        "trans_mes": {"$sum": 1}
    }},
    {"$group": {
        "_id": "$_id.account_id",
        "total_trans": {"$sum": "$trans_mes"}
    }},
    {"$addFields": {
        "promedio_mensual": {"$divide": ["$total_trans", 12]}
    }},
    {"$lookup": {
        "from": "customers",
        "localField": "_id",
        "foreignField": "accounts",
        "as": "cliente"
    }},
    {"$unwind": "$cliente"},
    {"$addFields": {
        "categoria": {
            "$switch": {
                "branches": [
                    {"case": {"$lt": ["$promedio_mensual", 2]}, "then": "infrequent"},
                    {"case": {"$and": [
                        {"$gte": ["$promedio_mensual", 2]},
                        {"$lte": ["$promedio_mensual", 5]}
                    ]}, "then": "regular"},
                    {"case": {"$gt": ["$promedio_mensual", 5]}, "then": "frequent"}
                ],
                "default": "sin datos"
            }
        }
    }},
    {"$project": {
        "nombre_completo": "$cliente.name",
        "promedio_mensual": {"$round": ["$promedio_mensual", 1]},
        "categoria": 1,
        "total_trans_anual": "$total_trans",
        "_id": 0
    }},
    {"$sort": {"promedio_mensual": -1}}
]

resultados_210 = list(transactions_col.aggregate(pipeline_210))

print(f"Total clientes con actividad en ultimo año: {len(resultados_210)}")
print("\nDistribucion por categoria:")
categorias = {"infrequent": 0, "regular": 0, "frequent": 0, "sin datos": 0}
for r in resultados_210:
    categorias[r['categoria']] += 1

for cat, count in categorias.items():
    if count > 0:
        print(f"  {cat}: {count} clientes ({count/len(resultados_210)*100:.1f}%)")

print("\nEjemplos:")
for r in resultados_210[:15]:
    print(f"{r['nombre_completo']} - Promedio: {r['promedio_mensual']} trans/mes - Categoria: {r['categoria']} (Total anual: {r['total_trans_anual']})")

#  CERRAR CONEXIoN 
client.close()
print("\n Conexion cerrada")
