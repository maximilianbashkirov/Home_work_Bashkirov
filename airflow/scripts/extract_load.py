import os
import gc
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from datetime import datetime

DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'postgres'),
    'port': os.getenv('POSTGRES_PORT', '5432'),
    'database': os.getenv('POSTGRES_DB', 'delivery_db'),
    'user': os.getenv('POSTGRES_USER', 'airflow'),
    'password': os.getenv('POSTGRES_PASSWORD', 'airflow_password')
}

PARQUET_PATH = os.getenv('PARQUET_PATH', '/opt/airflow/data/parquet')
BATCH_SIZE = 5000

def get_db_engine():
    connection_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    return create_engine(connection_string, pool_size=10, max_overflow=20)

def parse_address(address_text):
    if not address_text or pd.isna(address_text):
        return None, None, None
    parts = str(address_text).split(',')
    city = parts[0].strip() if len(parts) >= 1 else None
    street = parts[1].strip() if len(parts) >= 2 else None
    building = parts[2].strip() if len(parts) >= 3 else None
    return city, street, building

def parse_store_address(store_address):
    if not store_address or pd.isna(store_address):
        return None, None, None, None
    parts = str(store_address).split(',')
    name = parts[0].strip() if len(parts) >= 1 else None
    city = parts[1].strip() if len(parts) >= 2 else None
    street = parts[2].strip() if len(parts) >= 3 else None
    building = parts[3].strip() if len(parts) >= 4 else None
    return name, city, street, building

def create_tables_if_not_exists(conn):
    print("Создание таблиц...")
    conn.execute(text('CREATE EXTENSION IF NOT EXISTS "uuid-ossp"'))
    
    tables = [
        ("dim_users", """CREATE TABLE IF NOT EXISTS dim_users (user_id BIGINT PRIMARY KEY, user_phone VARCHAR(50) NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"""),
        ("dim_addresses", """CREATE TABLE IF NOT EXISTS dim_addresses (address_id SERIAL PRIMARY KEY, address_text TEXT NOT NULL, city VARCHAR(100), street VARCHAR(200), building VARCHAR(50), created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, UNIQUE (address_text))"""),
        ("dim_stores", """CREATE TABLE IF NOT EXISTS dim_stores (store_id BIGINT PRIMARY KEY, store_name VARCHAR(200), store_address TEXT NOT NULL, city VARCHAR(100), street VARCHAR(200), building VARCHAR(50), created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"""),
        ("dim_categories", """CREATE TABLE IF NOT EXISTS dim_categories (category_id SERIAL PRIMARY KEY, category_name VARCHAR(100) NOT NULL UNIQUE, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"""),
        ("dim_items", """CREATE TABLE IF NOT EXISTS dim_items (item_id BIGINT PRIMARY KEY, item_title VARCHAR(200) NOT NULL, category_id INTEGER REFERENCES dim_categories(category_id), created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"""),
        ("dim_drivers", """CREATE TABLE IF NOT EXISTS dim_drivers (driver_id BIGINT PRIMARY KEY, driver_phone VARCHAR(50) NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"""),
        ("dim_payment_types", """CREATE TABLE IF NOT EXISTS dim_payment_types (payment_type_id SERIAL PRIMARY KEY, payment_type_name VARCHAR(50) NOT NULL UNIQUE, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"""),
        ("fact_orders", """CREATE TABLE IF NOT EXISTS fact_orders (order_id BIGINT PRIMARY KEY, user_id BIGINT NOT NULL REFERENCES dim_users(user_id), address_id INTEGER REFERENCES dim_addresses(address_id), store_id BIGINT REFERENCES dim_stores(store_id), payment_type_id INTEGER REFERENCES dim_payment_types(payment_type_id), created_at TIMESTAMP, paid_at TIMESTAMP, delivery_started_at TIMESTAMP, delivered_at TIMESTAMP, canceled_at TIMESTAMP, cancellation_reason VARCHAR(255), order_discount NUMERIC(5,2) DEFAULT 0, delivery_cost NUMERIC(10,2) DEFAULT 0, order_status VARCHAR(50), created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"""),
        ("fact_order_items", """CREATE TABLE IF NOT EXISTS fact_order_items (order_item_id BIGSERIAL PRIMARY KEY, order_id BIGINT NOT NULL REFERENCES fact_orders(order_id), item_id BIGINT NOT NULL REFERENCES dim_items(item_id), replaced_item_id BIGINT REFERENCES dim_items(item_id), quantity INTEGER NOT NULL DEFAULT 1, price NUMERIC(10,2) NOT NULL, canceled_quantity INTEGER DEFAULT 0, item_discount NUMERIC(5,2) DEFAULT 0, created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"""),
        ("fact_delivery_assignments", """CREATE TABLE IF NOT EXISTS fact_delivery_assignments (assignment_id BIGSERIAL PRIMARY KEY, order_id BIGINT NOT NULL REFERENCES fact_orders(order_id), driver_id BIGINT NOT NULL REFERENCES dim_drivers(driver_id), assigned_at TIMESTAMP NOT NULL, completed_at TIMESTAMP, is_current BOOLEAN DEFAULT FALSE, created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
    ]

    for name, query in tables:
        conn.execute(text(query))
        print(f"Таблица {name} готова.")

    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_fact_orders_user_id ON fact_orders(user_id)",
        "CREATE INDEX IF NOT EXISTS idx_fact_orders_store_id ON fact_orders(store_id)",
        "CREATE INDEX IF NOT EXISTS idx_fact_orders_address_id ON fact_orders(address_id)",
        "CREATE INDEX IF NOT EXISTS idx_fact_orders_created_at ON fact_orders(created_at)",
        "CREATE INDEX IF NOT EXISTS idx_fact_order_items_order_id ON fact_order_items(order_id)",
        "CREATE INDEX IF NOT EXISTS idx_fact_order_items_item_id ON fact_order_items(item_id)",
        "CREATE INDEX IF NOT EXISTS idx_fact_delivery_assignments_order_id ON fact_delivery_assignments(order_id)",
        "CREATE INDEX IF NOT EXISTS idx_fact_delivery_assignments_driver_id ON fact_delivery_assignments(driver_id)"
    ]
    for idx_query in indexes:
        conn.execute(text(idx_query))

    conn.execute(text("""INSERT INTO dim_payment_types (payment_type_name) VALUES ('Сразу'), ('постоплата курьеру') ON CONFLICT (payment_type_name) DO NOTHING"""))
    print("Справочник типов оплаты заполнен.")

def load_dimension_data_batched(df_iter, conn):
    
    users_set = set()
    addresses_set = set()
    stores_set = set()
    categories_set = set()
    items_set = set()
    drivers_set = set()

    users_buf, addresses_buf, stores_buf = [], [], []
    categories_buf, items_buf, drivers_buf = [], [], []

    print("Загрузка справочников (потоковая)...")

    for chunk_df in df_iter:
        # Очистка NaN сразу в чанке
        chunk_df = chunk_df.replace({np.nan: None, pd.NA: None})

        for _, row in chunk_df[['user_id', 'user_phone']].drop_duplicates(subset=['user_id']).iterrows():
            if row['user_id'] and row['user_id'] not in users_set:
                users_set.add(row['user_id'])
                users_buf.append({
                    'user_id': int(row['user_id']),
                    'user_phone': row['user_phone'],
                    'created_at': datetime.now(),
                    'updated_at': datetime.now()
                })
        
        for addr in chunk_df['address_text'].dropna().unique():
            if addr not in addresses_set:
                addresses_set.add(addr)
                c, s, b = parse_address(addr)
                addresses_buf.append({
                    'address_text': addr, 'city': c, 'street': s, 'building': b,
                    'created_at': datetime.now()
                })

        for _, row in chunk_df[['store_id', 'store_address']].drop_duplicates(subset=['store_id']).iterrows():
            if row['store_id'] and row['store_id'] not in stores_set:
                stores_set.add(row['store_id'])
                n, c, s, b = parse_store_address(row['store_address'])
                stores_buf.append({
                    'store_id': int(row['store_id']), 'store_name': n, 'store_address': row['store_address'],
                    'city': c, 'street': s, 'building': b,
                    'created_at': datetime.now(), 'updated_at': datetime.now()
                })

        for cat in chunk_df['item_category'].dropna().unique():
            if cat not in categories_set:
                categories_set.add(cat)
                categories_buf.append({'category_name': cat, 'created_at': datetime.now()})

        for _, row in chunk_df[['item_id', 'item_title', 'item_category']].drop_duplicates(subset=['item_id']).iterrows():
            if row['item_id'] and row['item_id'] not in items_set:
                items_set.add(row['item_id'])
                items_buf.append({
                    'item_id': int(row['item_id']), 'item_title': row['item_title'],
                    'item_category': row['item_category'], # Позже заменим на ID
                    'created_at': datetime.now(), 'updated_at': datetime.now()
                })

        for _, row in chunk_df[['driver_id', 'driver_phone']].drop_duplicates(subset=['driver_id']).iterrows():
            if row['driver_id'] and row['driver_id'] not in drivers_set:
                drivers_set.add(row['driver_id'])
                drivers_buf.append({
                    'driver_id': int(row['driver_id']), 'driver_phone': row['driver_phone'],
                    'created_at': datetime.now(), 'updated_at': datetime.now()
                })

        if len(users_buf) > BATCH_SIZE:
            conn.execute(text("""INSERT INTO dim_users (user_id, user_phone, created_at, updated_at) VALUES (:user_id, :user_phone, :created_at, :updated_at) ON CONFLICT (user_id) DO UPDATE SET user_phone=EXCLUDED.user_phone, updated_at=EXCLUDED.updated_at"""), users_buf)
            users_buf = []
        
        if len(addresses_buf) > BATCH_SIZE:
            conn.execute(text("""INSERT INTO dim_addresses (address_text, city, street, building, created_at) VALUES (:address_text, :city, :street, :building, :created_at) ON CONFLICT (address_text) DO NOTHING"""), addresses_buf)
            addresses_buf = []
            
        if len(stores_buf) > BATCH_SIZE:
            conn.execute(text("""INSERT INTO dim_stores (store_id, store_name, store_address, city, street, building, created_at, updated_at) VALUES (:store_id, :store_name, :store_address, :city, :street, :building, :created_at, :updated_at) ON CONFLICT (store_id) DO UPDATE SET store_name=EXCLUDED.store_name, store_address=EXCLUDED.store_address, city=EXCLUDED.city, street=EXCLUDED.street, building=EXCLUDED.building, updated_at=EXCLUDED.updated_at"""), stores_buf)
            stores_buf = []

        if len(categories_buf) > BATCH_SIZE:
            conn.execute(text("""INSERT INTO dim_categories (category_name, created_at) VALUES (:category_name, :created_at) ON CONFLICT (category_name) DO NOTHING"""), categories_buf)
            categories_buf = []
            
        if len(items_buf) > BATCH_SIZE:
             pass 

        if len(drivers_buf) > BATCH_SIZE:
            conn.execute(text("""INSERT INTO dim_drivers (driver_id, driver_phone, created_at, updated_at) VALUES (:driver_id, :driver_phone, :created_at, :updated_at) ON CONFLICT (driver_id) DO UPDATE SET driver_phone=EXCLUDED.driver_phone, updated_at=EXCLUDED.updated_at"""), drivers_buf)
            drivers_buf = []
        
        gc.collect()

    if users_buf: conn.execute(text("""INSERT INTO dim_users (user_id, user_phone, created_at, updated_at) VALUES (:user_id, :user_phone, :created_at, :updated_at) ON CONFLICT (user_id) DO UPDATE SET user_phone=EXCLUDED.user_phone, updated_at=EXCLUDED.updated_at"""), users_buf)
    if addresses_buf: conn.execute(text("""INSERT INTO dim_addresses (address_text, city, street, building, created_at) VALUES (:address_text, :city, :street, :building, :created_at) ON CONFLICT (address_text) DO NOTHING"""), addresses_buf)
    if stores_buf: conn.execute(text("""INSERT INTO dim_stores (store_id, store_name, store_address, city, street, building, created_at, updated_at) VALUES (:store_id, :store_name, :store_address, :city, :street, :building, :created_at, :updated_at) ON CONFLICT (store_id) DO UPDATE SET store_name=EXCLUDED.store_name, store_address=EXCLUDED.store_address, city=EXCLUDED.city, street=EXCLUDED.street, building=EXCLUDED.building, updated_at=EXCLUDED.updated_at"""), stores_buf)
    if categories_buf: conn.execute(text("""INSERT INTO dim_categories (category_name, created_at) VALUES (:category_name, :created_at) ON CONFLICT (category_name) DO NOTHING"""), categories_buf)
    if drivers_buf: conn.execute(text("""INSERT INTO dim_drivers (driver_id, driver_phone, created_at, updated_at) VALUES (:driver_id, :driver_phone, :created_at, :updated_at) ON CONFLICT (driver_id) DO UPDATE SET driver_phone=EXCLUDED.driver_phone, updated_at=EXCLUDED.updated_at"""), drivers_buf)

    print("связь с категориями")
    result = conn.execute(text("SELECT category_id, category_name FROM dim_categories"))
    cat_map = {row[1]: row[0] for row in result}
    
    items_final = []
    for item in items_buf:
        cid = cat_map.get(item['item_category'])
        items_final.append({
            'item_id': item['item_id'], 'item_title': item['item_title'],
            'category_id': cid, 'created_at': item['created_at'], 'updated_at': item['updated_at']
        })
        if len(items_final) > BATCH_SIZE:
            conn.execute(text("""INSERT INTO dim_items (item_id, item_title, category_id, created_at, updated_at) VALUES (:item_id, :item_title, :category_id, :created_at, :updated_at) ON CONFLICT (item_id) DO UPDATE SET item_title=EXCLUDED.item_title, category_id=EXCLUDED.category_id, updated_at=EXCLUDED.updated_at"""), items_final)
            items_final = []
    
    if items_final:
        conn.execute(text("""INSERT INTO dim_items (item_id, item_title, category_id, created_at, updated_at) VALUES (:item_id, :item_title, :category_id, :created_at, :updated_at) ON CONFLICT (item_id) DO UPDATE SET item_title=EXCLUDED.item_title, category_id=EXCLUDED.category_id, updated_at=EXCLUDED.updated_at"""), items_final)

    print("Справочники загружены.")

def load_fact_data_batched(df_iter, conn):
    print("Загрузка фактов (потоковая)...")
    
    addr_res = conn.execute(text("SELECT address_id, address_text FROM dim_addresses"))
    addr_map = {row[1]: row[0] for row in addr_res}
    
    pay_res = conn.execute(text("SELECT payment_type_id, payment_type_name FROM dim_payment_types"))
    pay_map = {row[1]: row[0] for row in pay_res}

    orders_buf = []
    assignments_buf = []
    items_buf = []
    
    processed_orders = set()
    processed_items = set()

    for chunk_df in df_iter:
        chunk_df = chunk_df.replace({np.nan: None, pd.NA: None})
        
        orders_agg = chunk_df.groupby('order_id').agg({
            'user_id': 'first', 'address_text': 'first', 'store_id': 'first',
            'payment_type': 'first', 'created_at': 'first', 'paid_at': 'first',
            'delivery_started_at': 'first', 'delivered_at': 'first',
            'canceled_at': 'first', 'order_cancellation_reason': 'first',
            'order_discount': 'first', 'delivery_cost': 'first',
            'driver_id': lambda x: list(x.dropna().unique())
        }).reset_index()

        for _, row in orders_agg.iterrows():
            oid = row['order_id']
            if oid in processed_orders: continue
            processed_orders.add(oid)
            
            aid = addr_map.get(row['address_text'])
            pid = pay_map.get(row['payment_type'])
            
            status = 'created'
            if row['canceled_at']: status = 'canceled'
            elif row['delivered_at']: status = 'delivered'
            elif row['delivery_started_at']: status = 'in_delivery'
            elif row['paid_at']: status = 'paid'

            orders_buf.append({
                'order_id': int(oid), 'user_id': int(row['user_id']), 'address_id': aid,
                'store_id': int(row['store_id']) if row['store_id'] else None,
                'payment_type_id': pid,
                'created_at': row['created_at'], 'paid_at': row['paid_at'],
                'delivery_started_at': row['delivery_started_at'], 'delivered_at': row['delivered_at'],
                'canceled_at': row['canceled_at'], 'order_cancellation_reason': row['order_cancellation_reason'],
                'order_discount': float(row['order_discount']) if row['order_discount'] else 0,
                'delivery_cost': float(row['delivery_cost']) if row['delivery_cost'] else 0,
                'order_status': status
            })

            drivers = row['driver_id'] if isinstance(row['driver_id'], list) else [row['driver_id']]
            for idx, did in enumerate(drivers):
                if did:
                    assignments_buf.append({
                        'order_id': int(oid), 'driver_id': int(did),
                        'assigned_at': row['created_at'],
                        'completed_at': row['delivered_at'] if (idx == len(drivers)-1 and row['delivered_at']) else None,
                        'is_current': (idx == len(drivers) - 1)
                    })

        items_chunk = chunk_df[['order_id', 'item_id', 'item_quantity', 'item_price', 'item_canceled_quantity', 'item_discount', 'item_replaced_id']].drop_duplicates(subset=['order_id', 'item_id'])
        for _, irow in items_chunk.iterrows():
            key = (irow['order_id'], irow['item_id'])
            if key in processed_items: continue
            processed_items.add(key)
            
            items_buf.append({
                'order_id': int(irow['order_id']), 'item_id': int(irow['item_id']),
                'replaced_item_id': int(irow['item_replaced_id']) if irow['item_replaced_id'] else None,
                'quantity': int(irow['item_quantity']) if irow['item_quantity'] else 1,
                'price': float(irow['item_price']) if irow['item_price'] else 0,
                'canceled_quantity': int(irow['item_canceled_quantity']) if irow['item_canceled_quantity'] else 0,
                'item_discount': float(irow['item_discount']) if irow['item_discount'] else 0
            })

        if len(orders_buf) > BATCH_SIZE:
            conn.execute(text("""INSERT INTO fact_orders (order_id, user_id, address_id, store_id, payment_type_id, created_at, paid_at, delivery_started_at, delivered_at, canceled_at, cancellation_reason, order_discount, delivery_cost, order_status, created_timestamp, updated_timestamp) VALUES (:order_id, :user_id, :address_id, :store_id, :payment_type_id, :created_at, :paid_at, :delivery_started_at, :delivered_at, :canceled_at, :order_cancellation_reason, :order_discount, :delivery_cost, :order_status, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) ON CONFLICT (order_id) DO UPDATE SET user_id=EXCLUDED.user_id, address_id=EXCLUDED.address_id, store_id=EXCLUDED.store_id, payment_type_id=EXCLUDED.payment_type_id, paid_at=EXCLUDED.paid_at, delivery_started_at=EXCLUDED.delivery_started_at, delivered_at=EXCLUDED.delivered_at, canceled_at=EXCLUDED.canceled_at, cancellation_reason=EXCLUDED.cancellation_reason, order_discount=EXCLUDED.order_discount, delivery_cost=EXCLUDED.delivery_cost, order_status=EXCLUDED.order_status, updated_timestamp=CURRENT_TIMESTAMP"""), orders_buf)
            orders_buf = []

        if len(assignments_buf) > BATCH_SIZE:
            conn.execute(text("""INSERT INTO fact_delivery_assignments (order_id, driver_id, assigned_at, completed_at, is_current, created_timestamp) VALUES (:order_id, :driver_id, :assigned_at, :completed_at, :is_current, CURRENT_TIMESTAMP)"""), assignments_buf)
            assignments_buf = []

        if len(items_buf) > BATCH_SIZE:
            conn.execute(text("""INSERT INTO fact_order_items (order_id, item_id, replaced_item_id, quantity, price, canceled_quantity, item_discount, created_timestamp, updated_timestamp) VALUES (:order_id, :item_id, :replaced_item_id, :quantity, :price, :canceled_quantity, :item_discount, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"""), items_buf)
            items_buf = []
        
        gc.collect()

    if orders_buf: conn.execute(text("""INSERT INTO fact_orders (order_id, user_id, address_id, store_id, payment_type_id, created_at, paid_at, delivery_started_at, delivered_at, canceled_at, cancellation_reason, order_discount, delivery_cost, order_status, created_timestamp, updated_timestamp) VALUES (:order_id, :user_id, :address_id, :store_id, :payment_type_id, :created_at, :paid_at, :delivery_started_at, :delivered_at, :canceled_at, :order_cancellation_reason, :order_discount, :delivery_cost, :order_status, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) ON CONFLICT (order_id) DO UPDATE SET user_id=EXCLUDED.user_id, address_id=EXCLUDED.address_id, store_id=EXCLUDED.store_id, payment_type_id=EXCLUDED.payment_type_id, paid_at=EXCLUDED.paid_at, delivery_started_at=EXCLUDED.delivery_started_at, delivered_at=EXCLUDED.delivered_at, canceled_at=EXCLUDED.canceled_at, cancellation_reason=EXCLUDED.cancellation_reason, order_discount=EXCLUDED.order_discount, delivery_cost=EXCLUDED.delivery_cost, order_status=EXCLUDED.order_status, updated_timestamp=CURRENT_TIMESTAMP"""), orders_buf)
    
    if assignments_buf: conn.execute(text("""INSERT INTO fact_delivery_assignments (order_id, driver_id, assigned_at, completed_at, is_current, created_timestamp) VALUES (:order_id, :driver_id, :assigned_at, :completed_at, :is_current, CURRENT_TIMESTAMP)"""), assignments_buf)
    
    if items_buf: conn.execute(text("""INSERT INTO fact_order_items (order_id, item_id, replaced_item_id, quantity, price, canceled_quantity, item_discount, created_timestamp, updated_timestamp) VALUES (:order_id, :item_id, :replaced_item_id, :quantity, :price, :canceled_quantity, :item_discount, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"""), items_buf)

    print("Факты загружены.")

def extract_and_load(**kwargs):
    print("Начало процесса Extract & Load (Optimized)...")
    engine = get_db_engine()

    parquet_files = sorted([f for f in os.listdir(PARQUET_PATH) if f.endswith('.parquet')])
    print(f"Найдено файлов: {len(parquet_files)}")

    if not parquet_files:
        print("Нет файлов.")
        return

    def file_generator():
        for file in parquet_files:
            print(f"Чтение файла: {file}")
            df = pd.read_parquet(os.path.join(PARQUET_PATH, file))
            yield df
            del df
            gc.collect()

    try:
        with engine.begin() as conn:
            create_tables_if_not_exists(conn)

            print("=== PASS 1: Dimensions ===")
            load_dimension_data_batched(file_generator(), conn)
            
            print("=== PASS 2: Facts ===")
            load_fact_data_batched(file_generator(), conn)

        print("Успешно")
        return {'status': 'success'}
    except Exception as e:
        print(f"Ошибка: {e}")
        raise e