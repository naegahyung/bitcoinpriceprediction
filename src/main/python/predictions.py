from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split, cross_val_predict
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.tree import DecisionTreeRegressor
from sklearn.neural_network import MLPRegressor


def convertToUnix(x):
    return pd.Timestamp.strptime(x, '%Y-%m-%d %H:%M').timestamp()


def convertToDateTime(x):
    pd.Timestamp.utcfromtimestamp(x)

#For final.csv
file = "final.csv"
df = pd.read_csv(file, names=["Time", "BitDom", "MarketCap", "Price"])
train_perc = 0.8
test_perc = 0.2
train_data, test_data = train_test_split(df, test_size=test_perc)
train_data['Time'] = train_data['Time'].apply(convertToUnix).astype(int)
test_data['Time'] = test_data['Time'].apply(convertToUnix).astype(int)
clf = LinearRegression()
x_train_data = train_data.drop('Price', axis=1)
y_train_data = train_data[['Price']]
x_test_data = test_data.drop('Price', axis=1)
y_test_data = test_data[['Price']]


sorted_test_data = test_data.sort_values(by='Time')
plt.xlabel("Time")
plt.ylabel("Price")
plt.title("Bitcoin(USD) Price")
plt.plot(sorted_test_data['Time'][400000:].apply(pd.Timestamp.utcfromtimestamp), sorted_test_data['Price'][400000:])
plt.gcf().autofmt_xdate()
plt.savefig("realprice_last8months")

#Linear Regression
clf.fit(x_train_data, y_train_data)
print("Linear Regression Accuracy: ", clf.score(x_test_data, y_test_data))
y_pred = clf.predict(x_test_data)
print("Linear Regression r2_score: ", r2_score(y_pred, y_test_data))
x_test_data_lr = x_test_data
x_test_data_lr['Price'] = pd.DataFrame(y_pred, index=x_test_data_lr.index)
sorted_x_test_data_lr = x_test_data_lr.sort_values(by="Time")
plt.xlabel("Time")
plt.ylabel("Price")
plt.title("Linear Regression - Bitcoin(USD) Price Predictions")
plt.plot(sorted_x_test_data_lr['Time'][400000:].apply(pd.Timestamp.utcfromtimestamp), sorted_x_test_data_lr['Price'][400000:])
plt.gcf().autofmt_xdate()
plt.savefig("lr_rice_predictions")


#Decision Tree
x_test_data = test_data.drop('Price', axis=1)
y_test_data = test_data[['Price']]
dtree = DecisionTreeRegressor(random_state=0)
dtree.fit(x_train_data, y_train_data)
print("Decision tree accuracy: ", dtree.score(x_test_data, y_test_data))
print("Decision tree r2_score: ", r2_score(y_pred, y_test_data))
y_pred = dtree.predict(x_test_data)
x_test_data_dtree = x_test_data
x_test_data_dtree['Price'] = pd.DataFrame(y_pred, index=x_test_data_dtree.index)
sorted_x_test_data_dtree = x_test_data_dtree.sort_values(by="Time")

plt.xlabel("Time")
plt.ylabel("Price")
plt.title("Decision Tree- Bitcoin(USD) Price Predictions")
plt.plot(sorted_x_test_data_dtree['Time'][400000:].apply(pd.Timestamp.utcfromtimestamp), 
         sorted_x_test_data_dtree['Price'][400000:])
plt.gcf().autofmt_xdate()
plt.savefig("decision_tree_price_pred")


#Neural Network 
x_test_data = test_data.drop('Price', axis=1)
y_test_data = test_data[['Price']]
nn = MLPRegressor()
nn.fit(x_train_data, y_train_data)
print("Neural network accuracy: ", nn.score(x_test_data, y_test_data))
print("Neural network r2_score: ", r2_score(y_pred, y_test_data))
y_pred = nn.predict(x_test_data)
combined_x_test_data = x_test_data
combined_x_test_data['Price'] = pd.DataFrame(y_pred, index=combined_x_test_data.index)
sorted_x_test_data_nn = combined_x_test_data.sort_values(by="Time")
plt.xlabel("Time")
plt.ylabel("Price")
plt.title("Neural Network - Bitcoin(USD) Price Predictions")
plt.plot(sorted_x_test_data_nn['Time'][400000:].apply(pd.Timestamp.utcfromtimestamp), 
          sorted_x_test_data_nn['Price'][400000:])
plt.gcf().autofmt_xdate()
plt.savefig("nn_price_pred")

