import pandas as pd
import numpy as np
from sklearn.metrics import precision_score, recall_score, f1_score, confusion_matrix
from sklearn.metrics import accuracy_score
import os

df = pd.read_csv('data/combined_data_v2.csv')
train_data = pd.read_csv('data/train_data.csv')
test_data = pd.read_csv('data/test_data.csv')

# Random Guess Baseline
random_predictions = np.random.choice([0, 1], size=len(test_data))

# Accuracy for random baseline
accuracy_random = accuracy_score(test_data['enjoyed'], random_predictions)

# Precision, Recall, and F1 for Random Guess Baseline
precision_random = precision_score(test_data['enjoyed'], random_predictions)
recall_random = recall_score(test_data['enjoyed'], random_predictions)
f1_random = f1_score(test_data['enjoyed'], random_predictions)

# Confusion Matrix for Random Guess Baseline
conf_matrix_random = confusion_matrix(test_data['enjoyed'], random_predictions)
# Convert the confusion matrix to a pandas DataFrame for better readability
conf_matrix_df = pd.DataFrame(conf_matrix_random, 
                               columns=['Predicted: 0', 'Predicted: 1'], 
                               index=['Actual: 0', 'Actual: 1'])
print(f"Confusion Matrix (Random Guess): \n{conf_matrix_df}")

# Saving results as text in an output file
with open(os.path.join("output", 'random_baseline_results.txt'), 'w') as f:
    f.write(f"Random Guess Accuracy: {round(accuracy_random, 3)}\n")
    f.write(f"Random Guess Precision: {round(precision_random, 3)}\n")
    f.write(f"Random Guess Recall: {round(recall_random, 3)}\n")
    f.write(f"Random Guess F1-Score: {round(f1_random, 3)}\n")
    f.write("\nConfusion Matrix (Random Guess):\n")
    f.write(f"{conf_matrix_df}\n")

# Saving confusion matrix as a CSV file
conf_matrix_df.to_csv(os.path.join('output', 'confusion_matrix_random_guess.csv'))