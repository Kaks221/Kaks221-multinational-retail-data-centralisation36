import pandas as pd

class DataCleaning:
    @staticmethod
    def clean_user_data(df):
        df = df.dropna()  
        df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], errors='coerce') 
        df['join_date'] = pd.to_datetime(df['join_date'], errors='coerce')
        return df

    @staticmethod
    def clean_card_data(df):
        df['expiry_date'] = pd.to_datetime(df['expiry_date'], format='%m/%y', errors='coerce') 
        df['date_payment_confirmed'] = pd.to_datetime(df['date_payment_confirmed'], errors='coerce')
        df = df.dropna().drop_duplicates() 
        return df

    @staticmethod  
    def convert_product_weights(products_df):
        def convert_to_kg(weight):
            try:
                if isinstance(weight, float):
                    return weight
                weight_str = str(weight).lower().replace('kg', '').replace('g', '').strip()
                if 'ml' in weight_str:
                    return float(weight_str.replace('ml', '').strip()) * 0.001
                if 'x' in weight_str:
                    parts = weight_str.split('x')
                    if len(parts) == 2:
                        return float(parts[0].strip()) * float(parts[1].strip()) * 0.001
                return float(weight_str)
            except ValueError:
                return None  
        products_df['weight'] = products_df['weight'].apply(convert_to_kg)
        return products_df

    @staticmethod
    def clean_products_data(df):
        df = df.dropna(subset=['product_name', 'product_price', 'weight', 'date_added', 'uuid'])
        df['product_price'] = df['product_price'].replace('[£,]', '', regex=True).astype(float)
        df['date_added'] = pd.to_datetime(df['date_added'], errors='coerce')
        df['weight'] = df['weight'].astype(float)  
        df = df[(df['product_price'] >= 0) & (df['product_price'] < 10000)]
        return df
    
    @staticmethod
    def clean_orders_data(df):
        df['date_uuid'] = pd.to_datetime(df['date_uuid'], errors='coerce')
        df['product_code'] = df['product_code'].astype(str)
        df['store_code'] = df['store_code'].astype(str)
        df.drop(columns=['first_name', 'last_name', '1'], inplace=True, errors='ignore')
        return df
    
    @staticmethod
    def clean_sales_data(df):
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        df = df.dropna(subset=['timestamp'])
        return df