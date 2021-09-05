from tensorflow.keras import Sequential
from tensorflow.keras.layers import Dense, LSTM, Dropout
from sklearn.preprocessing import MinMaxScaler
from sklearn.model_selection import train_test_split
from tensorflow.keras.callbacks import EarlyStopping

import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from tensorflow.python.ops.gen_dataset_ops import window_dataset

import Analyzer

mk = Analyzer.MarketDB()
raw_df = mk.get_daily_price('유한양행', '2018-05-04', '2021-07-22')

#def MinMaxScaler(data):
#    """최솟값과 최댓값을 이용하여 0~1 값으로 변환"""
#    numerator = data-np.min(data, 0)
#    denominator = np.max(data, 0) - np.min(data, 0)
#    return numerator / (denominator + 1e-7)

# 데이터 전처리
#dfx = raw_df[['open', 'high', 'low', 'volume', 'close']]
#dfx = MinMaxScaler(dfx)     # 계산 속도 향상을 위해 데이터의 스케일을 0~1로 변경
#dfy = dfx[['close']]

def make_dataset(data, label, window_size=20):
    feature_list = []
    label_list = []
    for i in range(len(data) - window_size):
        feature_list.append(np.array(data.iloc[i: i + window_size]))
        label_list.append(np.array(label.iloc[i + window_size]))
    return np.array(feature_list), np.array(label_list)

scaler = MinMaxScaler()
scale_cols = ['open', 'high', 'low', 'volume', 'close']

df_scaled = scaler.fit_transform(raw_df[scale_cols])
df_scaled = pd.DataFrame(df_scaled)
df_scaled.columns = scale_cols

feature_cols = scale_cols[0:4]
label_cols = scale_cols[4]

TEST_SIZE = 200
train = df_scaled[:-TEST_SIZE]
test = df_scaled[-TEST_SIZE:]

train_feature = train[feature_cols]
train_label = train[label_cols]

test_feature = test[feature_cols]
test_label = test[label_cols]

# train dataset
train_feature, train_label = make_dataset(train_feature, train_label, 20)
train_x, valid_x, train_y, valid_y = train_test_split(train_feature, train_label, test_size=0.2)

# test dataset
test_feature, test_label = make_dataset(test_feature, test_label, 20)

# 모델 생성
model = Sequential()
model.add(LSTM(16, activation='relu', return_sequences=False, input_shape=(train_feature.shape[1], train_feature.shape[2])))
model.add(Dropout(0.1))
#model.add(LSTM(16, activation='relu'))
#model.add(Dropout(0.1))
model.add(Dense(units=1))

# 학습 및 예측
model.compile(optimizer='adam', loss='mean_squared_error')
early_stop = EarlyStopping(monitor='val_loss', patience=5)
history = model.fit(train_x, train_y, epochs=200, batch_size=16, validation_data=(valid_x, valid_y), callbacks=[early_stop])   # 학습
pred = model.predict(test_feature)  # 예측

# 실제 종가와 예측치를 그래프로 비교
plt.figure()
plt.plot(test_label, color='red', label='real SEC stock price')
plt.plot(pred, color='blue', label='predicted SEC stock price')
plt.title('SEC stock price prediction')
plt.xlabel('time')
plt.ylabel('stock price')
plt.legend()
plt.show()

# 다음 날 예측 종가 출력
#print("SEC tomorrow's price : ", raw_df[-1:4] * pred[-1] / label_cols[-1])