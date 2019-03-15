#!/usr/bin/env python

"""modules.py: modules is dedicated for main body of model building."""

__author__ = "Chakraborty, S."
__copyright__ = "Copyright 2019, Space@VT"
__credits__ = []
__license__ = "MIT"
__version__ = "1.0."
__maintainer__ = "Chakraborty, S."
__email__ = "shibaji7@vt.edu"
__status__ = "Research"

import os
import numpy as np
import pandas as pd

from keras.layers.core import Dense, Activation, Dropout
from keras.preprocessing import sequence
from keras.models import Sequential
from keras.layers import Dense, Embedding
from keras.layers import LSTM
from keras.utils import to_categorical
from sklearn.metrics import roc_curve
from sklearn.metrics import auc

import database as db
import utils

from startup import *


class kp_lstm_classifier(object):
    """
    This is a classifier model, LSTM classifier is trained here.
    """

    def __init__(self):
        self.ini()
        self.setup()
        return

    def test_unit(self,X_test,y_test):
        y_pred_keras = self.model.predict_proba(X_test)[:,1]
        y_test_x = np.array([np.argmax(ym, axis=None, out=None) for ym in y_test])
        fpr_keras, tpr_keras, thresholds_keras = roc_curve(y_test_x, y_pred_keras)
        auc_keras = auc(fpr_keras, tpr_keras)
        print "=========================================> AUC:",np.round(auc_keras,3)
        return

    def ini(self):
        self.look_back = 3
        self.hidden_node = 100
        self.batch_size = 100
        self.epochs = 30
        return

    def compile_model(self):
        self.model = Sequential()
        self.model.add(LSTM(self.hidden_node, return_sequences=False, input_shape=self.input_shape))
        self.model.add(Dropout(0.2))
        self.model.add(Dense(units=2))
        self.model.add(Activation("softmax"))
        self.model.compile(loss="categorical_crossentropy", optimizer="adam", metrics=["acc"])
        return

    def setup(self):
        self.source = db.kp_classifier_dataset()
        X_train, X_test, y_train, y_test = self.source.create_master_model_data()
        self.input_shape = (len(self.source.xparams), self.look_back)
        self.compile_model()
        self.model.fit(X_train, y_train, batch_size=self.batch_size, epochs=self.epochs, validation_data=(X_test,y_test))#, verbose=0)
        self.test_unit(X_test,y_test)
        return

    def predict_proba(self,X):
        """
        X should be reformed and transformed
        """
        y_pred = self.model.predict_proba(X)[:,1]
        return y_pred



if __name__ == "__main__":
    clf = kp_lstm_classifier()
    pass
