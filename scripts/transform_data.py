from extract_and_save_data import connect_mongo, create_connect_db, create_connect_collection
import pandas as pd

def visualize_collection(collection):
    docs = []
    for doc in collection.find():
        print(doc)
        docs.append(doc)
    return docs


def rename_column(collection, col_name, new_col_name):
    print(f"Renaming column {col_name} to {new_col_name}.")
    collection.update_many({}, {"$rename": {col_name: new_col_name}})
    return collection


def select_category(collection, category):
    print(f"Filtering products from the {category} category.")
    prducts_in_category = []
    query = collection.find({"Categoria do Produto": category})
    for doc in query:
        prducts_in_category.append(doc)
    return prducts_in_category


def make_regex(collection, col, regex):
    print(f"Selecting records with regular expression {regex}.")
    select_date = {col: {"$regex": regex}}
    lista_produtos  = []
    for doc in collection.find(select_date):
        lista_produtos.append(doc)
    return lista_produtos


def create_dataframe(lista):
    print("Creating a DataFrame with the data.")
    df = pd.DataFrame(lista)
    return df


def format_date(df, column):
    print(f"Formatting date values in column {column}.")
    df[column] = pd.to_datetime(df[column], format="%d/%m/%Y")
    df[column] = df[column].dt.strftime("%Y-%m-%d")
    return df

def save_csv(df, path):
    print(f"Saving transformed data to file {path}")
    df.to_csv(path, index=False)


uri = "mongodb+srv://victorsantos:12345@cluster-pipeline.wakdkz3.mongodb.net/?retryWrites=true&w=majority&appName=Cluster-Pipeline"
db_name = "db_produtos"
collection_name = "produtos"

client = connect_mongo(uri)
db = create_connect_db(client, db_name)
collection = create_connect_collection(db, collection_name)

rename_column(collection, "lat", "Latitude")
rename_column(collection, "lon", "Longitude")
# visualize_collection(collection)

eletronicos = select_category(collection, "eletronicos")
# print(eletronicos)

date_pos_2022 = make_regex(collection, "Data da Compra", "/202[2-9]")
# print(date_regex)

df_eletronicos = create_dataframe(eletronicos)
df_date_pos_2022 = create_dataframe(date_pos_2022)

df_eletronicos = format_date(df_eletronicos, "Data da Compra")
df_date_pos_2022 = format_date(df_date_pos_2022, "Data da Compra")

# print(df_eletronicos.head())
# print(df_date_pos_2022.head())

save_csv(df_eletronicos, "../data/eletronicos.csv")
save_csv(df_date_pos_2022, "../data/vendas_pos_2022.csv")

client.close()
