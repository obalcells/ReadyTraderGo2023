# -*- coding: utf-8 -*-

# parameters
model_name = ""
scaling_factor = 1000
lag_window = 20
target_ahead = 3
valid_size = 0.25
test_size  = 0.05
seed0 = 42
n_fold = 7 # we dont use this at the moment
plot_start = 0
plot_size = 50

params = {
    'early_stopping_rounds': 50,
    'objective': 'regression',
    'metric': 'rmse',
#     'metric': 'None',
    'boosting_type': 'gbdt',
    'max_depth': 5,
    'verbose': -1,
    'max_bin':600,
    'min_data_in_leaf':50,
    'learning_rate': 0.03,
    'subsample': 0.7,
    'subsample_freq': 1,
    'feature_fraction': 1,
    'lambda_l1': 0.5,
    'lambda_l2': 2,
    'seed':seed0,
    'feature_fraction_seed': seed0,
    'bagging_fraction_seed': seed0,
    'drop_seed': seed0,
    'data_random_seed': seed0,
    'extra_trees': True,
    'extra_seed': seed0,
    'zero_as_missing': True,
    "first_metric_only": True
}

def verify_features_3(data, proc_data) -> None:
  etf_midprice = np.array( data["MidPriceETF"] ) # .iloc[lag_window+1:-target_ahead] )
  fut_midprice = np.array( data["MidPriceFUT"] ) # .iloc[lag_window+1:-target_ahead] )
  for lag in range(1, lag_window + 1):
    # Column 1
    other_log_etf_over_mean_price = np.array( proc_data[f'log_etf/etf_mean_{lag}'] )
    # Column 3 
    other_log_diff_over_diff_mean = np.array( proc_data[f'log_diff/diff_mean_{lag}'] )
    # Target
    other_target = np.array( proc_data['target'] )

    for i in range(lag_window, len(etf_midprice) - target_ahead):
      other_i = i - lag_window # indices are shifted in the proc_data by lag_window

      # Check Column 1
      mean_etf_midprice = 0.0
      for j in range(1, lag + 1):
        assert i - j >= 0
        mean_etf_midprice += etf_midprice[i - j] / lag
      log_etf_over_mean_price = np.log(etf_midprice[i] / mean_etf_midprice)
      if abs(log_etf_over_mean_price - other_log_etf_over_mean_price[other_i]) > 1e-8:
        print("i={0}, lag={1}, other_calc={2}, now_calc={3}".format(i, lag, other_log_etf_over_mean_price[other_i], log_etf_over_mean_price))
        print(etf_midprice[i - lag : i + 1])
      assert abs(other_log_etf_over_mean_price[other_i] - log_etf_over_mean_price) < 1e-8

      # Check Column 3 
      diff_mean = 0.0
      for j in range(1, lag + 1):
        diff_mean += (etf_midprice[i - j] / fut_midprice[i - j]) / lag
      log_diff_over_diff_mean = np.log( (etf_midprice[i] / fut_midprice[i]) / diff_mean )
      if abs(log_diff_over_diff_mean - other_log_diff_over_diff_mean[other_i]) > 1e-8:
        print("i={0}, lag={1}, other_calc={2}, now_calc={3}".format(i, lag, other_log_etf_over_mean_price[other_i], log_etf_over_mean_price))
        print(etf_midprice[i - lag : i + 1])
        print(fut_midprice[i - lag : i + 1])
      assert abs(log_diff_over_diff_mean - other_log_diff_over_diff_mean[other_i]) < 1e-8
       
      # Check target
      target = np.log( etf_prices[i + target_ahead] / etf_prices[i] )
      if abs(target - other_target[other_i]) > 1e-6:
        print(i, lag)
        print(target)
        print(other_target[other_i])
        print(etf_prices[i + target_ahead], etf_prices[i])
      assert abs(target - other_target[other_i]) < 1e-6

def get_features_3(df) -> pd.DataFrame:
    new_df = df.copy()
    for lag in range(1, lag_window + 1):
      new_df[f'log_etf/etf_mean_{lag}'] = np.log( np.array(df['MidPriceETF']) / np.roll( np.append( np.convolve( np.array(df['MidPriceETF']), np.ones(lag)/lag , mode="valid"), np.ones(lag-1) ), lag) ) 
      new_df[f'log_fut/fut_mean_{lag}'] = np.log( np.array(df['MidPriceFUT']) / np.roll( np.append( np.convolve( np.array(df['MidPriceFUT']), np.ones(lag)/lag , mode="valid"), np.ones(lag-1) ), lag) ) 
      new_df[f'log_diff/diff_mean_{lag}'] = np.log( (np.array(df['MidPriceETF'])/np.array(df['MidPriceFUT'])) / np.roll( np.append( np.convolve( (np.array(df['MidPriceETF'])/np.array(df['MidPriceFUT'])), np.ones(lag)/lag, mode="valid"), np.ones(lag-1) ), lag) )
      new_df[f'log_etf_return_{lag}'] = np.log( np.array(df["MidPriceETF"]) / np.roll(np.array(df["MidPriceETF"]), lag) )
      new_df[f'log_fut_return_{lag}'] = np.log( np.array(df["MidPriceFUT"]) / np.roll(np.array(df["MidPriceFUT"]), lag) )
    new_df["target"] = np.log( np.array(df['MidPriceETF']) / np.roll( np.array(df["MidPriceETF"]), target_ahead) ) 
    new_df = new_df.drop(columns=df.columns)
    # we keep those for visualization purposes
    new_df["MidPriceETF"] = df["MidPriceETF"]
    new_df["MidPriceFUT"] = df["MidPriceFUT"]
    new_df = new_df.iloc[lag_window:-target_ahead]
    for column in new_df.columns:
      assert new_df[column].isnull().values.any() == False
    verify_features(df, new_df)
    return new_df


# we return the predictions on the test data and the test data itself
def get_Xy_and_model(proc_data) -> Tuple[np.array, pd.DataFrame]:
    test_split_idx  = int(proc_data.shape[0] * (1-test_size))
    valid_split_idx = int(proc_data.shape[0] * (1-(valid_size+test_size)))

    train_data  = proc_data.loc[:valid_split_idx].copy()
    valid_data  = proc_data.loc[valid_split_idx+1:test_split_idx].copy()
    test_data   = proc_data.loc[test_split_idx+1:].copy()

    # we feed this to the model
    y_train = train_data['target'].copy() * scaling_factor
    X_train = train_data.drop(['target', "MidPriceETF", "MidPriceFUT"], 1) * scaling_factor 

    # we also feed this to the model, it's what it has to minimize (together with training data loss)
    y_valid = valid_data['target'].copy() * scaling_factor 
    X_valid = valid_data.drop(['target', "MidPriceETF", "MidPriceFUT"], 1) * scaling_factor 

    # this data isn't given to the model, we're gonna use it to plot
    y_test  = test_data['target'].copy() * scaling_factor 
    X_test  = test_data.drop(['target', "MidPriceETF", "MidPriceFUT"], 1) * scaling_factor 

    # remember to unscale to plot the predictions!

    features = list(X_train.columns)
    not_used_features_train = ['target', "MidPriceETF", "MidPriceFUT"]


    train_dataset = lgb.Dataset(X_train,
                                y_train,
                                feature_name = features)

    val_dataset = lgb.Dataset(X_val,
                             y_val,
                             feature_name = features)

    importances = []

    file = f'{model_name}.txt'

    if file in os.listdir():
        model = lgb.Booster(model_name=file)
        print(f'Trained model loaded from files')
    else:
        print("Training model...")
        model = lgb.train(params = params,
                        train_set = train_dataset, 
                        valid_sets=[train_dataset, val_dataset],
                        valid_names=['tr', 'vl'],
                        num_boost_round = 5000,
                        verbose_eval = 100,
                        feval = correlation)

        importances.append(model.feature_importance(importance_type='gain'))

        model.booster_.save_model(file)
        print(f"Trained model was saved to '{file}'")

    pred = list(model.predict(X_test))

    plot_importance(np.array(importances),features, PLOT_TOP_N = 20, figsize=(10, 5))

    return pred, test_data

def plot_predictions(predictions, test_data, plot_start_=plot_start, plot_size_=plot_size) -> None:
    # we're gonna print the first 50 ticks of the test data
    price_pred = np.zeros(plot_size_)

    for i in range(plot_start_, plot_start_ + plot_size_):
        other_i = i - plot_start_
        log_return_pred = pred[i] / scaling_factor
        etf_midprice = test_data.iloc[i]["MidPriceETF"]
        fut_midprice = test_data.iloc[i]["MidPriceFUT"]
        etf_price_prediction = np.exp(log_return_pred) * etf_midprice
        price_pred[i - plot_start_] = etf_price_prediction

    actual_prices = list(test_data["MidPriceETF"].iloc[plot_start_ + target_ahead : plot_start_ + plot_size_ + target_ahead])

    plt.plot(price_pred, label="prediction")
    plt.plot(actual_prices, label="actual")
    plt.legend()

# we get only the first dataset we have (only one market simulation)
df = load_market_data_files_to_df_list()[0]

# we internally verify that the features are consistent with what we want them to be
proc_data = get_features_3(df)

# let's print the processed data once to be able to manually check that everything's ok
print(proc_data.head())

val_predictions, val_data = get_Xy_and_model(proc_data)

plot_predictions(val_predictions, val_data)


# we don't use this function at the moment but we will update it
# from: https://www.kaggle.com/code/nrcjea001/lgbm-embargocv-weightedpearson-lagtarget/
# def get_time_series_cross_val_splits(data, cv = n_fold, embargo = 3750):
#     all_train_timestamps = data['timestamp'].unique()
#     len_split = len(all_train_timestamps) // cv
#     test_splits = [all_train_timestamps[i * len_split:(i + 1) * len_split] for i in range(cv)]
#     # fix the last test split to have all the last timestamps, in case the number of timestamps wasn't divisible by cv
#     rem = len(all_train_timestamps) - len_split*cv
#     if rem>0:
#         test_splits[-1] = np.append(test_splits[-1], all_train_timestamps[-rem:])

#     train_splits = []
#     for test_split in test_splits:
#         test_split_max = int(np.max(test_split))
#         test_split_min = int(np.min(test_split))
#         # get all of the timestamps that aren't in the test split
#         train_split_not_embargoed = [e for e in all_train_timestamps if not (test_split_min <= int(e) <= test_split_max)]
#         # embargo the train split so we have no leakage. Note timestamps are expressed in seconds, so multiply by 60
#         embargo_sec = 60*embargo
#         train_split = [e for e in train_split_not_embargoed if
#                        abs(int(e) - test_split_max) > embargo_sec and abs(int(e) - test_split_min) > embargo_sec]
#         train_splits.append(train_split)

#     # convenient way to iterate over train and test splits
#     train_test_zip = zip(train_splits, test_splits)
#     return train_test_zip

# def get_Xy_and_model(df_proc):
    
#     # EmbargoCV
#     train_test_zip = get_time_series_cross_val_splits(df_proc, cv = n_fold, embargo = 3750)
#     print("entering time series cross validation loop")
#     importances = []
#     oof_pred = []
#     oof_valid = []
    
#     for split, train_test_split in enumerate(train_test_zip):
#         gc.collect()
        
#         print(f"doing split {split+1} out of {n_fold}")
#         train_split, test_split = train_test_split
#         train_split_index = df_proc['timestamp'].isin(train_split)
#         test_split_index = df_proc['timestamp'].isin(test_split)
    
#         train_dataset = lgb.Dataset(df_proc.loc[train_split_index, features],
#                                     df_proc.loc[train_split_index, f'Target_{asset_id}'].values, 
#                                     feature_name = features, 
#                                    )
#         val_dataset = lgb.Dataset(df_proc.loc[test_split_index, features], 
#                                   df_proc.loc[test_split_index, f'Target_{asset_id}'].values, 
#                                   feature_name = features, 
#                                  )

#         print(f"number of train data: {len(df_proc.loc[train_split_index])}")
#         print(f"number of val data:   {len(df_proc.loc[test_split_index])}")

#         model = lgb.train(params = params,
#                           train_set = train_dataset, 
#                           valid_sets=[train_dataset, val_dataset],
#                           valid_names=['tr', 'vl'],
#                           num_boost_round = 5000,
#                           verbose_eval = 100,     
#                           feval = correlation,
#                          )
#         importances.append(model.feature_importance(importance_type='gain'))
        
#         file = f'trained_model_id{asset_id}_fold{split}.pkl'
#         pickle.dump(model, open(file, 'wb'))
#         print(f"Trained model was saved to 'trained_model_id{asset_id}_fold{split}.pkl'")
#         print("")
            
#         oof_pred += list(  model.predict(df_proc.loc[test_split_index, features])        )
#         oof_valid += list(   df_proc.loc[test_split_index, f'Target_{asset_id}'].values    )
    
    
#     plot_importance(np.array(importances),features, PLOT_TOP_N = 20, figsize=(10, 5))

#     return oof_pred, oof_valid
