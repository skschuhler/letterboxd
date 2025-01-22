import pandas as pd
import scipy.sparse
from sklearn.model_selection import train_test_split
from scipy.sparse import csr_matrix

df = pd.read_csv('data/combined_data.csv')

# Create outcome variable 
df['enjoyed'] = df.apply(lambda row: 1 if row['Rating'] >= 4 or row['Liked'] == 1 else 0, axis=1)
df.to_csv('data/combined_data_v2.csv')

# Create user-item matrix
user_item_matrix = df.pivot_table(index='user', columns='Title', values='enjoyed', fill_value=0)
#shuffled_columns = np.random.permutation(user_item_matrix.columns)
user_item_sparse_matrix = csr_matrix(user_item_matrix, dtype='float32')
scipy.sparse.save_npz('data/sparse_matrix.npz', user_item_sparse_matrix)

# Split data into train (80%) and test set (20%)
train_data, test_data = train_test_split(df, test_size=0.2, random_state=4)
train_data.to_csv('data/train_data.csv')
test_data.to_csv('data/test_data.csv')