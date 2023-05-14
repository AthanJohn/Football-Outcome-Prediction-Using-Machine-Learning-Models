import data_analysis
from sklearn.model_selection import GridSearchCV, StratifiedKFold
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
from sklearn.ensemble import RandomForestClassifier, AdaBoostClassifier
from sklearn.svm import SVC
from sklearn.neighbors import KNeighborsClassifier
import xgboost as xgb



def trainModelsAndVisualize(X_train, X_test, y_train, y_test):
    '''Trains different models with X_train and y_train variables, using different parameters. It finds the hyperparameters of each column. Tests the models' accuracy with X_test and y_test values, using different metrics.
     Creates confusion matrices for each model. Creates accuaracy-score and F1-score plots.'''

    rfParams = {'n_estimators': [10, 50, 100, 200, 500], 
                 'max_depth': [None, 5, 10, 20, 30, 40],
                 'min_samples_split': [2, 5, 10, 20, 30]}
    
    adaParams = {'n_estimators': [50, 100, 150, 200, 500],
                 'learning_rate': [0.1, 0.5, 1]}
    
    svmParams = {'C': [1, 10, 100, 200],
                 'gamma': [0.01, 0.1, 0.5]}
    
    knnParams = {'n_neighbors': [3, 5, 7, 10, 15, 20 , 50, 100, 200, 500, 1000],
                 'weights': ['uniform', 'distance']}
    
    xbgParams = {'max_depth': [2, 4, 6],
                 'min_child_weight': [1, 3, 5],
                 'gamma': [0, 0.1, 0.2],
                 'subsample': [0.6, 0.8, 1.0],
                 'colsample_bytree': [0.6, 0.8, 1.0],
                 'learning_rate': [0.1, 0.01, 0.001]}

    rf = RandomForestClassifier()
    ada = AdaBoostClassifier()
    svm = SVC()
    knn = KNeighborsClassifier()
    xgBoost = xgb.XGBClassifier(objective = 'multi:softmax', num_class = 3)

    skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)

    rfGS = GridSearchCV(rf, rfParams, scoring='accuracy', cv=skf)
    adaGS = GridSearchCV(ada, adaParams, scoring='accuracy', cv=skf)
    svmGS = GridSearchCV(svm, svmParams, scoring='accuracy', cv=skf)
    knnGS = GridSearchCV(knn, knnParams, scoring='accuracy', cv=skf)
    xgbGS = GridSearchCV(xgBoost, xbgParams, scoring='accuracy', cv=skf)

    print('AdaBoost - fit')
    adaGS.fit(X_train, y_train)

    print('SVM - fit')
    svmGS.fit(X_train, y_train)

    print('KNN - fit')
    knnGS.fit(X_train, y_train)

    print('Random Forest - fit')
    rfGS.fit(X_train, y_train)

    print('XGBoost - fit')
    xgbGS.fit(X_train, y_train)

    print('Random Forest - Best Params:', rfGS.best_params_)
    print('Random Forest - Best score:', rfGS.best_score_)
    print('AdaBoost - Best Params:', adaGS.best_params_)
    print('AdaBoost - Best score:', adaGS.best_score_)
    print('SVM - Best Params:', svmGS.best_params_)
    print('SVM - Best score:', svmGS.best_score_)
    print('KNN - Best Params:', knnGS.best_params_)
    print('KNN - Best score:', knnGS.best_score_)
    print('XGBoost - Best Params:', xgbGS.best_params_)
    print('XGBoost - Best score:', xgbGS.best_score_)

    rfBest = rfGS.best_estimator_
    adaBest = adaGS.best_estimator_
    svmBest = svmGS.best_estimator_
    knnBest = knnGS.best_estimator_
    xgbBest = xgbGS.best_estimator_

    rfBest.fit(X_train, y_train)
    adaBest.fit(X_train, y_train)
    svmBest.fit(X_train, y_train)
    knnBest.fit(X_train, y_train)
    xgbBest.fit(X_train, y_train)

    rfPred = rfBest.predict(X_test)
    adaPred = adaBest.predict(X_test)
    svmPred = svmBest.predict(X_test)
    knnPred = knnBest.predict(X_test)
    xgbPred = xgbBest.predict(X_test)

    #accuracy score
    rfAc = round(accuracy_score(y_test, rfPred), 4)
    adaAc = round(accuracy_score(y_test, adaPred), 4)
    svmAc = round(accuracy_score(y_test, svmPred), 4)
    knnAc = round(accuracy_score(y_test, knnPred), 4)
    xgbAc = round(accuracy_score(y_test, xgbPred), 4)

    #precision score
    rfPrec = round(precision_score(y_test, rfPred, average='weighted'), 4)
    adaPrec = round(precision_score(y_test, adaPred, average='weighted'), 4)
    svmPrec = round(precision_score(y_test, svmPred, average='weighted'), 4)
    knnPrec = round(precision_score(y_test, knnPred, average='weighted'), 4)
    xgbPrec = round(precision_score(y_test, xgbPred, average='weighted'), 4)

    #recall score
    rfRecall = round(recall_score(y_test, rfPred, average='weighted'), 4)
    adaRecall = round(recall_score(y_test, adaPred, average='weighted'), 4)
    svmRecall = round(recall_score(y_test, svmPred, average='weighted'), 4)
    knnRecall = round(recall_score(y_test, knnPred, average='weighted'), 4)
    xgbRecall = round(recall_score(y_test, xgbPred, average='weighted'), 4)

    #F1 Score
    rfF1 = round(f1_score(y_test, rfPred, average='weighted'), 4)
    adaF1 = round(f1_score(y_test, adaPred, average='weighted'), 4)
    svmF1 = round(f1_score(y_test, svmPred, average='weighted'), 4) 
    knnF1 = round(f1_score(y_test, knnPred, average='weighted'), 4)
    xgbF1 = round(f1_score(y_test, xgbPred, average='weighted'), 4)

    print('Random Forest Accuracy Score:',rfAc)
    print('Random Forest Precision Score:',rfPrec)
    print('Random Forest Recall Score:',rfRecall)
    print('Random Forest F1 Score:',rfF1)
    print('')

    print('AdaBoost Accuracy Score:',adaAc)
    print('AdaBoost Precision Score:',adaPrec)
    print('AdaBoost Recall Score:',adaRecall)
    print('AdaBoost F1 Score:',adaF1)
    print('')

    print('SVM Accuracy Score:',svmAc)
    print('SVM Precision Score:',svmPrec)
    print('SVM Recall Score:',svmRecall)
    print('SVM F1 Score:',svmF1)
    print('')

    print('K-Nearest Neighbors Accuracy Score:',knnAc)
    print('K-Nearest Neighbors Precision Score:',knnPrec)
    print('K-Nearest Neighbors Recall Score:',knnRecall)
    print('K-Nearest Neighbors F1 Score:',knnF1)
    print('')

    print('XGBoost Accuracy Score:',xgbAc)
    print('XGBoost Precision Score:',xgbPrec)
    print('XGBoost Recall Score:',xgbRecall)
    print('XGBoost F1 Score:',xgbF1)

    data_analysis.ConfusionMatrix(y_test, rfPred, 'RandomForest')
    data_analysis.ConfusionMatrix(y_test, adaPred, 'AdaBoost')
    data_analysis.ConfusionMatrix(y_test, svmPred, 'SVM')
    data_analysis.ConfusionMatrix(y_test, knnPred, 'KNN')
    data_analysis.ConfusionMatrix(y_test, xgbPred, 'XGBoost')

    #Accuracy Bar Chart
    modelAccuracy = [rfAc , adaAc , svmAc , knnAc, xgbAc]
    data_analysis.ModelsAccuracyHist(modelAccuracy)

    #F1-Score Bar Chart
    modelF1Score = [rfF1, adaF1, svmF1, knnF1, xgbF1]
    data_analysis.ModelsF1ScoreHist(modelF1Score)