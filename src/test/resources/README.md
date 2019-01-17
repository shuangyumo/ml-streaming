The model file is trans-language and can be generated with Scala Worksheet train.sc or with this Python code:

```python
import numpy as np
import xgboost as xgb
dtrain = xgb.DMatrix('agaricus.txt.train')
bst = xgb.train({'max_depth':2, 'eta':1, 'objective':'binary:logistic'}, dtrain, 3)
bst.save_model('agaricus.model')
``` 
