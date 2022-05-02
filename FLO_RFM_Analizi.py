##Görev 1: Veriyi Anlama ve Hazırlama

import datetime as dt
import pandas as pd
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 2000)
# pd.set_option('display.max_rows', None)
pd.set_option('display.float_format', lambda x: '%.3f' % x)


# master_id: Eşsiz müşteri numarası
# order_channel: Alışveriş yapılan platforma ait hangi kanalın kullanıldığı (Android, ios, Desktop, Mobile)
# last_order_channel: En son alışverişin yapıldığı kanal
# first_order_date: Müşterinin yaptığı ilk alışveriş tarihi
# last_order_date: Müşterinin yaptığı son alışveriş tarihi
# last_order_date_online: Müşterinin online platformda yaptığı son alışveriş tarihi
# last_order_date_offline: Müşterinin offline platformda yaptığı son alışveriş tarihi
# order_num_total_ever_online: Müşterinin online platformda yaptığı toplam alışveriş sayısı
# order_num_total_ever_offline: Müşterinin offline'da yaptığı toplam alışveriş sayısı
# customer_value_total_ever_offline: Müşterinin offline alışverişlerinde ödediği toplam ücret
# customer_value_total_ever_online: Müşterinin online alışverişlerinde ödediği toplam ücret
# interested_in_categories_12: Müşterinin son 12 ayda alışveriş yaptığı kategorilerin listesi

# Adım1: flo_data_20K.csv verisini okuyunuz.Dataframe’in kopyasını oluşturunuz.

df_ = pd.read_csv("/Users/eminebozkurt/Desktop/vbo/week3/hw1/FLO_RFM_Analizi/flo_data_20k.csv")
df_.head()
df = df_.copy()
df.head()
df.shape
df.isnull().sum()


# Adım2: Verisetinde
# a. İlk 10 gözlem,
df.head(10)
# b. Değişken isimleri,
df.columns
# c. Betimsel istatistik,
df.describe().T
# d. Boş değer,
df.isnull().sum()
# e. Değişken tipleri, incelemesi yapınız.
df.info()
df.dtypes

# Adım 3: Omnichannel müşterilerin hem online'dan hemde offline platformlardan alışveriş yaptığını ifade etmektedir.
# Her bir müşterinin toplam alışveriş sayısı ve harcaması için yeni değişkenler oluşturunuz.

df["order_num_total_ever_omnichannel"] = df["order_num_total_ever_online"] + df["order_num_total_ever_offline"]
df["customer_value_total_ever_omnichannel"] = df["customer_value_total_ever_online"] + df["customer_value_total_ever_offline"]
df.head()

# Adım 4: Değişken tiplerini inceleyiniz. Tarih ifade eden değişkenlerin tipini date'e çeviriniz.
df.info()
df.dtypes

for col in df.columns:
    if "date" in col:
        df[col] = df.loc[:, df.columns.str.contains("date")].astype("datetime64[ns]")

df.info()

df.loc[:, ["first_order_date", "last_order_date", "last_order_date_online", "last_order_date_offline"]] = df.loc[:, df.columns.str.contains("date")].astype("datetime64[ns]")


date_cols = [col for col in df.columns if "date" in col.lower()]

#Kullanım şekli: df['Date']= pd.to_datetime(df['Date'])
df[date_cols] = df[date_cols].astype('datetime64[ns]')
df.dtypes


# Adım 5: Alışveriş kanallarındaki müşteri sayısının, toplam alınan ürün sayısının ve toplam harcamaların dağılımına bakınız.

rfm = df.groupby('order_channel').agg({'master_id': lambda num: num.nunique(),
                                     "order_num_total_ever_omnichannel": lambda order: order.sum(),
                                     "customer_value_total_ever_omnichannel": lambda value: value.sum()})

df.groupby("order_channel").agg({"master_id": "count",
                                 "order_num_total_ever_omnichannel": "sum",
                                 "customer_value_total_ever_omnichannel": "sum"}).head()

rfm.head()

# Adım 6: En fazla kazancı getiren ilk 10 müşteriyi sıralayınız.
df.sort_values(by="customer_value_total_ever_omnichannel", ascending=False)[["master_id", "customer_value_total_ever_omnichannel"]].head(10)

# Adım 7: En fazla siparişi veren ilk 10 müşteriyi sıralayınız.
df.sort_values(by="order_num_total_ever_omnichannel", ascending=False)[["master_id", "order_num_total_ever_omnichannel"]].head(10)
df.head()


# Adım 8: Veri ön hazırlık sürecini fonksiyonlaştırınız.

def create_rfm(df):

    # VERIYI HAZIRLAMA
    df["order_num_total_ever_omnichannel"] = df["order_num_total_ever_online"] + df["order_num_total_ever_offline"]
    df["customer_value_total_ever_omnichannel"] = df["customer_value_total_ever_online"] + df["customer_value_total_ever_offline"]
    df.dropna(inplace=True)

    df.loc[:, ["first_order_date", "last_order_date", "last_order_date_online", "last_order_date_offline"]] = df.loc[:, df.columns.str.contains("date")].astype("datetime64[ns]")
    return df

df = df_.copy()
df.head()
rfm_new = create_rfm(df)

# Görev 2: RFM Metriklerinin Hesaplanması
# Adım 1: Recency, Frequency ve Monetary tanımlarını yapınız.
# Adım 2: Müşteri özelinde Recency, Frequency ve Monetary metriklerini hesaplayınız.
# Adım 3: Hesapladığınız metrikleri rfm isimli bir değişkene atayınız.
# Adım 4: Oluşturduğunuz metriklerin isimlerini recency, frequency ve monetary olarak değiştiriniz.

df["last_order_date"].max()# Timestamp('2021-05-30 00:00:00')
today_date = dt.datetime(2021, 6, 2)

rfm = df.groupby('master_id').agg({'last_order_date': lambda last_order_date: (today_date - last_order_date.max()).days,
                                   "order_num_total_ever_omnichannel": lambda order: order.sum(),
                                   "customer_value_total_ever_omnichannel": lambda value: value.sum()})

df[df["master_id"].value_counts() > 1]
df["master_id"].value_counts().sort_values(ascending=False).head()
# x = df.groupby('master_id').agg({"master_id": "count"}).rename(columns={"master_id": "adet"})
# x[x["adet"] > 1]
type(df["master_id"].value_counts() > 1)



#
rfm = pd.DataFrame({"CustomerId": df["master_id"],
                   "Recency": (today_date - df["last_order_date"]).dt.days,
                   "Frequency": df["order_num_total_ever_omnichannel"],
                   "Monetary": df["customer_value_total_ever_omnichannel"]})

import datetime as dt

df.head()
rfm.columns = ['Recency', 'Frequency', 'Monetary']
rfm


# Görev 3: RF Skorunun Hesaplanması
# Adım 1: Recency, Frequency ve Monetary metriklerini qcut yardımı ile 1-5 arasında skorlara çeviriniz.
# Adım 2: Bu skorları recency_score, frequency_score ve monetary_score olarak kaydediniz.
# Adım 3: recency_score ve frequency_score’u tek bir değişken olarak ifade ediniz ve RF_SCORE olarak kaydediniz.

rfm["recency_score"] = pd.qcut(rfm['Recency'], 5, labels=[5, 4, 3, 2, 1])

rfm["frequency_score"] = pd.qcut(rfm['Frequency'].rank(method="first"), 5, labels=[1, 2, 3, 4, 5])

rfm["monetary_score"] = pd.qcut(rfm['Monetary'], 5, labels=[1, 2, 3, 4, 5])

rfm["RF_SCORE"] = rfm['recency_score'].astype(str) + rfm['frequency_score'].astype(str)

#rfm.index = rfm.reset_index()
rfm.head()
# rfm = rfm.reset_index()
# rfm.drop(columns="index", axis=1, inplace=True)

# Görev 4: RF Skorunun Segment Olarak Tanımlanması
# Adım 1: Oluşturulan RF skorları için segment tanımlamaları yapınız. Yukarıda
# Adım 2: Aşağıdaki seg_map yardımı ile skorları segmentlere çeviriniz.

# RFM isimlendirmesi
seg_map = {
    r'[1-2][1-2]': 'hibernating',#1. elemanında 1 ya da 2, 2. elemanında 1 ya da 2
    r'[1-2][3-4]': 'at_Risk',
    r'[1-2]5': 'cant_loose',
    r'3[1-2]': 'about_to_sleep',
    r'33': 'need_attention',
    r'[3-4][4-5]': 'loyal_customers',
    r'41': 'promising',# 1. elemanında 4, 2. elemanında 1 görürsen
    r'51': 'new_customers',
    r'[4-5][2-3]': 'potential_loyalists',
    r'5[4-5]': 'champions'
}

rfm['segment'] = rfm['RF_SCORE'].replace(seg_map, regex=True)# key value mapi ile değiştirir.
rfm.head()


# Görev 5: Aksiyon Zamanı !
# Adım 1: Segmentlerin recency, frequency ve monetary ortalamalarını inceleyiniz.
rfm[["segment", "Recency", "Frequency", "Monetary"]].groupby("segment").agg(["mean"])

# Adım 2: RFM analizi yardımıyla aşağıda verilen 2 case için ilgili profildeki müşterileri bulun ve müşteri id'lerini csv olarak kaydediniz.
# a. FLO bünyesine yeni bir kadın ayakkabı markası dahil ediyor. Dahil ettiği markanın ürün fiyatları genel müşteri tercihlerinin üstünde.
# Bu nedenle markanın tanıtımı ve ürün satışları için ilgilenecek profildeki müşterilerle özel olarak iletişime geçmek isteniliyor.
# Sadık müşterilerinden(champions, loyal_customers) ve kadın kategorisinden alışveriş yapan kişiler özel olarak iletişim kurulacak müşteriler.
# Bu müşterilerin id numaralarını csv dosyasına kaydediniz.

# def convert_date(df):
#     for col in df.columns:
#         if "date" in col:
#             df[col] = pd.to_datetime(df[col])

#func
def create_rfm(df):
    # VERIYI HAZIRLAMA
    df["order_num_total_ever_omnichannel"] = df["order_num_total_ever_online"] + df["order_num_total_ever_offline"]
    df["customer_value_total_ever_omnichannel"] = df["customer_value_total_ever_online"] + df["customer_value_total_ever_offline"]
    df.dropna(inplace=True)

    # convert_date(df)
    df.loc[:, ["first_order_date", "last_order_date", "last_order_date_online", "last_order_date_offline"]]  = df.loc[:, df.columns.str.contains("date")].astype("datetime64[ns]")

    # RFM METRIKLERININ HESAPLANMASI
    #df["last_order_date"].max()  # Timestamp('2021-05-30 00:00:00')
    today_date = dt.datetime(2021, 6, 2)

    rfm = pd.DataFrame({"CustomerId": df["master_id"],
                        "Recency": (today_date - df["last_order_date"]).dt.days,
                        "Frequency": df["order_num_total_ever_omnichannel"],
                        "Monetary": df["customer_value_total_ever_omnichannel"]})

    # rfm.columns = ["CustomerId", 'recency', 'frequency', 'monetary']

    # rfm = rfm[(rfm['monetary'] > 0)]

    # RFM SKORLARININ HESAPLANMASI
    rfm["Recency_score"] = pd.qcut(rfm['Recency'], 5, labels=[5, 4, 3, 2, 1])

    rfm["Frequency_score"] = pd.qcut(rfm['Frequency'].rank(method="first"), 5, labels=[1, 2, 3, 4, 5])

    rfm["Monetary_score"] = pd.qcut(rfm['Monetary'], 5, labels=[1, 2, 3, 4, 5])

    rfm["RF_SCORE"] = (rfm['Recency_score'].astype(str) +
                       rfm['Frequency_score'].astype(str))


    # SEGMENTLERIN ISIMLENDIRILMESI
    seg_map = {
        r'[1-2][1-2]': 'hibernating',
        r'[1-2][3-4]': 'at_risk',
        r'[1-2]5': 'cant_loose',
        r'3[1-2]': 'about_to_sleep',
        r'33': 'need_attention',
        r'[3-4][4-5]': 'loyal_customers',
        r'41': 'promising',
        r'51': 'new_customers',
        r'[4-5][2-3]': 'potential_loyalists',
        r'5[4-5]': 'champions'
    }

    rfm['segment'] = rfm['RF_SCORE'].replace(seg_map, regex=True)
    return rfm

df = df_.copy()
df.head()
rfm2 = create_rfm(df)
rfm2.head()

df2 = df[df["interested_in_categories_12"].str.contains("KADIN")]
rfm2 = rfm2[(rfm2["segment"] == "champions") | (rfm2["segment"] == "loyal_customers")]
df2.head()
rfm2.head()

rfm2.rename(columns={"CustomerId": "master_id"}, inplace=True)
target_list = df2.merge(rfm2, how="inner", on="master_id")

target_list.to_csv("/Users/eminebozkurt/Desktop/vbo/week3/hw1/FLO_RFM_Analizi/target_list.csv")

# b. Erkek ve Çocuk ürünlerinde %40'a yakın indirim planlanmaktadır. Bu indirimle ilgili kategorilerle ilgilenen geçmişte iyi müşteri olan ama uzun
# süredir alışveriş yapmayan kaybedilmemesi gereken müşteriler, uykuda olanlar ve yeni gelen müşteriler özel olarak hedef alınmak isteniyor.
# Uygun profildeki müşterilerin id'lerini csv dosyasına kaydediniz.

df = df_.copy()
df.head()
rfm3 = create_rfm(df)
rfm3.head()

df3 = df[(df["interested_in_categories_12"].str.contains("ERKEK")) | (df["interested_in_categories_12"].str.contains("COCUK"))]
rfm3 = rfm3[(rfm3["segment"] == "champions") | (rfm3["segment"] == "loyal_customers")]

rfm3.rename(columns={"CustomerId": "master_id"}, inplace=True)
target_list = df3.merge(rfm3, how="inner", on="master_id")

target_list.to_csv("/Users/eminebozkurt/Desktop/vbo/week3/hw1/FLO_RFM_Analizi/target_list2.csv")
