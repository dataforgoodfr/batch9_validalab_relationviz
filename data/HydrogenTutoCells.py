# Click alt + shift + enter to see what happens


# %% md
# Hydrogen Tuto
step 1 : work with cells

# %% yo
print('Hello, world!')
print('yesh')
print('yo')

# alt + shift + enter to run all code in cell
# %% test


print('Hello, world!')
print('yesh')
print('yo')
## %% md
# This is a test :
- test
- test
 _italic text_
 **bold text**
 - Indented
     - Lists
# %% Work with interactive JSON

print('yesh')
from IPython.display import JSON

data = {"foo": {"bar": "baz"}, "a": 1}
JSON(data)
# %% Interactive matplotlib
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Qt5Agg')
import matplotlib.pyplot as plt
import numpy as np

t = np.linspace(0, 20, 500)
plt.plot(t, np.sin(t))
plt.show()

# %% static plots
import matplotlib.pyplot as plt
import numpy as np

%matplotlib inline
%config InlineBackend.figure_format = 'svg'
t = np.linspace(0, 20, 500)

plt.plot(t, np.sin(t))
plt.show()

# %% Dataframes
import numpy as np
import pandas as pd

df = pd.DataFrame({'A': 1.,
                   'B': pd.Timestamp('20130102'),
                   'C': pd.Series(1, index=list(range(4)), dtype='float32'),
                   'D': np.array([3] * 4, dtype='int32'),
                   'E': pd.Categorical(["test", "train", "test", "train"]),
                   'F': 'foo'})

df


# %% Images
from IPython.display import Image
Image('http://jakevdp.github.com/figures/xkcd_version.png')
# %% html
from IPython.display import HTML
HTML("<iframe src='https://nteract.io/' width='900' height='490'></iframe>")
# %% test
import pandas as pd

pd.options.display.html.table_schema = True
pd.options.display.max_rows = None


iris_url = "https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data"

df1 = pd.read_csv(iris_url)

df1
